use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::oneshot;
use tracing::{debug, trace};

#[derive(Debug, Clone)]
pub struct AppServerCommand {
    codex_bin: PathBuf,
}

impl AppServerCommand {
    pub fn new(codex_bin: PathBuf) -> Self {
        Self { codex_bin }
    }
}

pub struct AppServerClient {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl AppServerClient {
    pub async fn spawn(command: AppServerCommand) -> anyhow::Result<Self> {
        debug!(codex_bin = %command.codex_bin.display(), "spawning codex app-server");

        let mut child = Command::new(&command.codex_bin)
            .arg("app-server")
            .arg("--stdio")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| {
                format!(
                    "failed to spawn `{}` app-server --stdio",
                    command.codex_bin.display()
                )
            })?;

        let stdin = child
            .stdin
            .take()
            .context("codex app-server child stdin was not captured")?;
        let stdout = child
            .stdout
            .take()
            .context("codex app-server child stdout was not captured")?;

        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
        })
    }

    pub async fn initialize(
        &mut self,
        name: &str,
        title: &str,
        version: &str,
    ) -> anyhow::Result<InitializeResponse> {
        let params = InitializeParams {
            client_info: ClientInfo {
                name: name.to_owned(),
                title: title.to_owned(),
                version: version.to_owned(),
            },
            capabilities: ClientCapabilities {
                experimental_api: true,
            },
        };

        let response = self.request("initialize", params).await?;
        self.notify("initialized", json!({})).await?;
        Ok(response)
    }

    pub async fn thread_start(&mut self, cwd: String) -> anyhow::Result<ThreadStartResponse> {
        self.request("thread/start", ThreadStartParams { cwd: Some(cwd) })
            .await
    }

    pub async fn thread_fork(
        &mut self,
        thread_id: String,
        cwd: String,
    ) -> anyhow::Result<ThreadForkResponse> {
        self.request(
            "thread/fork",
            ThreadForkParams {
                thread_id,
                cwd: Some(cwd),
                ..ThreadForkParams::default()
            },
        )
        .await
    }

    pub async fn thread_resume(
        &mut self,
        thread_id: String,
        cwd: String,
    ) -> anyhow::Result<ThreadResumeResponse> {
        self.request(
            "thread/resume",
            ThreadResumeParams {
                thread_id,
                cwd: Some(cwd),
                exclude_turns: true,
            },
        )
        .await
    }

    pub async fn thread_list(
        &mut self,
        cwd: Option<String>,
        cursor: Option<String>,
    ) -> anyhow::Result<ThreadListResponse> {
        self.request(
            "thread/list",
            ThreadListParams {
                cursor,
                limit: Some(25),
                archived: Some(false),
                cwd,
            },
        )
        .await
    }

    pub async fn thread_unsubscribe(
        &mut self,
        thread_id: String,
    ) -> anyhow::Result<ThreadUnsubscribeResponse> {
        self.request("thread/unsubscribe", ThreadUnsubscribeParams { thread_id })
            .await
    }

    pub async fn turn_start_text_until_complete(
        &mut self,
        thread_id: String,
        text: String,
        cancel_rx: Option<oneshot::Receiver<()>>,
        mut on_event: impl FnMut(AppServerPromptEvent) -> anyhow::Result<()>,
    ) -> anyhow::Result<AppServerPromptCompletion> {
        let id = self.next_id;
        self.next_id += 1;

        let request = json!({
            "id": id,
            "method": "turn/start",
            "params": TurnStartParams {
                thread_id: thread_id.clone(),
                input: vec![TurnInput::Text { text }],
            },
        });

        self.write_message(&request).await?;

        let mut turn_started = false;
        let mut active_turn_id: Option<String> = None;
        let mut cancel_rx = cancel_rx;
        let mut interrupt_request_id = None;
        let mut interrupt_requested = false;
        loop {
            if interrupt_requested
                && interrupt_request_id.is_none()
                && let Some(turn_id) = active_turn_id.as_deref()
            {
                let interrupt_id = self.next_id;
                self.next_id += 1;
                let request = json!({
                    "id": interrupt_id,
                    "method": "turn/interrupt",
                    "params": TurnInterruptParams {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.to_owned(),
                    },
                });
                self.write_message(&request).await?;
                interrupt_request_id = Some(interrupt_id);
            }

            let message = if let Some(rx) = cancel_rx.as_mut() {
                tokio::select! {
                    message = self.read_message() => message?,
                    _ = rx => {
                        cancel_rx = None;
                        interrupt_requested = true;
                        continue;
                    }
                }
            } else {
                self.read_message().await?
            }
            .with_context(|| "codex app-server exited during turn")?;

            if message.get("id").and_then(Value::as_u64) == Some(id) {
                if let Some(error) = message.get("error") {
                    bail!("codex app-server turn/start failed: {error}");
                }

                let result = message
                    .get("result")
                    .cloned()
                    .context("turn/start response did not include `result`")?;
                let response: TurnStartResponse =
                    serde_json::from_value(result).context("failed to decode turn/start result")?;
                active_turn_id = Some(response.turn.id);
                turn_started = true;
                trace!(
                    turn_id = active_turn_id.as_deref(),
                    "codex app-server turn started"
                );
                continue;
            }

            if let Some(interrupt_id) = interrupt_request_id
                && message.get("id").and_then(Value::as_u64) == Some(interrupt_id)
            {
                if let Some(error) = message.get("error") {
                    bail!("codex app-server turn/interrupt failed: {error}");
                }

                let result = message
                    .get("result")
                    .cloned()
                    .context("turn/interrupt response did not include `result`")?;
                let _response: TurnInterruptResponse = serde_json::from_value(result)
                    .context("failed to decode turn/interrupt result")?;
                continue;
            }

            let Some(method) = message.get("method").and_then(Value::as_str) else {
                trace!(
                    ?message,
                    "ignoring non-notification app-server message during turn"
                );
                continue;
            };
            let params = message.get("params").cloned().unwrap_or(Value::Null);

            match method {
                "item/agentMessage/delta" => {
                    let notification: AgentMessageDeltaNotification =
                        serde_json::from_value(params)
                            .context("failed to decode agent message delta")?;
                    if notification.thread_id == thread_id
                        && Some(notification.turn_id.as_str()) == active_turn_id.as_deref()
                    {
                        on_event(AppServerPromptEvent::AgentMessageDelta(notification.delta))?;
                    }
                }
                "turn/completed" => {
                    let notification: TurnCompletedNotification = serde_json::from_value(params)
                        .context("failed to decode turn completed")?;
                    if notification.thread_id == thread_id
                        && Some(notification.turn.id.as_str()) == active_turn_id.as_deref()
                    {
                        return Ok(if interrupt_requested {
                            AppServerPromptCompletion::Cancelled
                        } else {
                            AppServerPromptCompletion::EndTurn
                        });
                    }
                }
                _ => {
                    if !turn_started {
                        trace!(
                            method,
                            ?message,
                            "ignoring app-server notification before turn response"
                        );
                    }
                }
            }
        }
    }

    pub async fn request<P, R>(&mut self, method: &str, params: P) -> anyhow::Result<R>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let id = self.next_id;
        self.next_id += 1;

        let request = json!({
            "id": id,
            "method": method,
            "params": params,
        });

        self.write_message(&request).await?;

        loop {
            let message = self.read_message().await?.with_context(|| {
                format!("codex app-server exited before responding to `{method}`")
            })?;

            if message.get("id").and_then(Value::as_u64) != Some(id) {
                trace!(
                    ?message,
                    "ignoring app-server notification while waiting for response"
                );
                continue;
            }

            if let Some(error) = message.get("error") {
                bail!("codex app-server request `{method}` failed: {error}");
            }

            let result = message
                .get("result")
                .cloned()
                .context("app-server response did not include `result`")?;
            return serde_json::from_value(result)
                .with_context(|| format!("failed to decode app-server `{method}` response"));
        }
    }

    pub async fn notify<P>(&mut self, method: &str, params: P) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        let notification = json!({
            "method": method,
            "params": params,
        });

        self.write_message(&notification).await
    }

    async fn write_message(&mut self, message: &Value) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(message).context("failed to encode JSON-RPC message")?;
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .await
            .context("failed to write to codex app-server stdin")?;
        self.stdin
            .flush()
            .await
            .context("failed to flush codex app-server stdin")?;
        Ok(())
    }

    async fn read_message(&mut self) -> anyhow::Result<Option<Value>> {
        let mut line = String::new();
        let read = self
            .stdout
            .read_line(&mut line)
            .await
            .context("failed to read from codex app-server stdout")?;

        if read == 0 {
            return Ok(None);
        }

        let message = serde_json::from_str(&line).context("failed to decode JSON-RPC message")?;
        Ok(Some(message))
    }
}

impl Drop for AppServerClient {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InitializeParams {
    client_info: ClientInfo,
    capabilities: ClientCapabilities,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientInfo {
    name: String,
    title: String,
    version: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientCapabilities {
    experimental_api: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResponse {
    pub user_agent: String,
    pub codex_home: String,
    pub platform_family: String,
    pub platform_os: String,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadStartParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartResponse {
    pub thread: AppServerThread,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadForkParams {
    thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    ephemeral: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    exclude_turns: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadForkResponse {
    pub thread: AppServerThread,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadResumeParams {
    thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    exclude_turns: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadResumeResponse {
    pub thread: AppServerThread,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadListParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    archived: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadListResponse {
    pub data: Vec<AppServerThread>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerThread {
    pub id: String,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadUnsubscribeParams {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadUnsubscribeResponse {
    pub status: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnStartParams {
    thread_id: String,
    input: Vec<TurnInput>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnInterruptParams {
    thread_id: String,
    turn_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnInterruptResponse {}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum TurnInput {
    Text { text: String },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnStartResponse {
    turn: AppServerTurn,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AppServerTurn {
    id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentMessageDeltaNotification {
    thread_id: String,
    turn_id: String,
    delta: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnCompletedNotification {
    thread_id: String,
    turn: AppServerTurn,
}

pub enum AppServerPromptEvent {
    AgentMessageDelta(String),
}

pub enum AppServerPromptCompletion {
    EndTurn,
    Cancelled,
}
