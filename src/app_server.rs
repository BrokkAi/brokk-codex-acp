use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

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
    stdin: Arc<Mutex<ChildStdin>>,
    pending_responses: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>,
    messages_tx: broadcast::Sender<AppServerMessage>,
    reader_task: JoinHandle<()>,
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

        let stdin = Arc::new(Mutex::new(stdin));
        let pending_responses = Arc::new(Mutex::new(HashMap::new()));
        let (messages_tx, _) = broadcast::channel(1024);
        let reader_task = spawn_reader(stdout, pending_responses.clone(), messages_tx.clone());

        Ok(Self {
            child,
            stdin,
            pending_responses,
            messages_tx,
            reader_task,
            next_id: 1,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AppServerMessage> {
        self.messages_tx.subscribe()
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

    pub async fn thread_read(&mut self, thread_id: String) -> anyhow::Result<ThreadReadResponse> {
        self.request(
            "thread/read",
            ThreadReadParams {
                thread_id,
                include_turns: true,
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

    pub async fn thread_delete(
        &mut self,
        thread_id: String,
    ) -> anyhow::Result<ThreadDeleteResponse> {
        self.request("thread/delete", ThreadDeleteParams { thread_id })
            .await
    }

    pub async fn thread_archive(
        &mut self,
        thread_id: String,
    ) -> anyhow::Result<ThreadArchiveResponse> {
        self.request("thread/archive", ThreadArchiveParams { thread_id })
            .await
    }

    pub async fn thread_name_set(
        &mut self,
        thread_id: String,
        name: String,
    ) -> anyhow::Result<ThreadSetNameResponse> {
        self.request("thread/name/set", ThreadSetNameParams { thread_id, name })
            .await
    }

    pub async fn thread_goal_set(
        &mut self,
        thread_id: String,
        objective: Option<String>,
        status: Option<String>,
        token_budget: Option<Option<i64>>,
    ) -> anyhow::Result<ThreadGoalSetResponse> {
        self.request(
            "thread/goal/set",
            ThreadGoalSetParams {
                thread_id,
                objective,
                status,
                token_budget,
            },
        )
        .await
    }

    pub async fn thread_goal_get(
        &mut self,
        thread_id: String,
    ) -> anyhow::Result<ThreadGoalGetResponse> {
        self.request("thread/goal/get", ThreadGoalGetParams { thread_id })
            .await
    }

    pub async fn thread_goal_clear(
        &mut self,
        thread_id: String,
    ) -> anyhow::Result<ThreadGoalClearResponse> {
        self.request("thread/goal/clear", ThreadGoalClearParams { thread_id })
            .await
    }

    pub async fn skills_list(
        &mut self,
        cwd: String,
        force_reload: bool,
    ) -> anyhow::Result<SkillsListResponse> {
        self.request(
            "skills/list",
            SkillsListParams {
                cwds: vec![cwd],
                force_reload: Some(force_reload),
            },
        )
        .await
    }

    pub async fn skills_config_write(
        &mut self,
        name: Option<String>,
        path: Option<String>,
        enabled: bool,
    ) -> anyhow::Result<SkillsConfigWriteResponse> {
        if name.is_none() && path.is_none() {
            bail!("skills/config/write requires either a skill name or path");
        }

        self.request(
            "skills/config/write",
            SkillsConfigWriteParams {
                name,
                path,
                enabled,
            },
        )
        .await
    }

    pub async fn skills_extra_roots_set(
        &mut self,
        roots: Vec<String>,
    ) -> anyhow::Result<SkillsExtraRootsSetResponse> {
        self.request("skills/extraRoots/set", SkillsExtraRootsSetParams { roots })
            .await
    }

    pub async fn app_list(&mut self) -> anyhow::Result<Value> {
        self.request("app/list", EmptyParams {}).await
    }

    pub async fn plugin_list(&mut self) -> anyhow::Result<Value> {
        self.request("plugin/list", EmptyParams {}).await
    }

    pub async fn plugin_installed(&mut self) -> anyhow::Result<Value> {
        self.request("plugin/installed", EmptyParams {}).await
    }

    pub async fn hooks_list(&mut self, cwd: String) -> anyhow::Result<Value> {
        self.request("hooks/list", HooksListParams { cwds: vec![cwd] })
            .await
    }

    pub async fn mcp_server_status_list(&mut self, thread_id: String) -> anyhow::Result<Value> {
        self.request(
            "mcpServerStatus/list",
            McpServerStatusListParams {
                thread_id: Some(thread_id),
                detail: Some("full"),
            },
        )
        .await
    }

    pub async fn thread_loaded_list(&mut self) -> anyhow::Result<Value> {
        self.request("thread/loaded/list", EmptyParams {}).await
    }

    pub async fn thread_background_terminals_list(
        &mut self,
        thread_id: String,
    ) -> anyhow::Result<Value> {
        self.request(
            "thread/backgroundTerminals/list",
            ThreadScopedParams { thread_id },
        )
        .await
    }

    pub async fn thread_background_terminals_clean(
        &mut self,
        thread_id: String,
    ) -> anyhow::Result<Value> {
        self.request(
            "thread/backgroundTerminals/clean",
            ThreadScopedParams { thread_id },
        )
        .await
    }

    pub async fn model_list(&mut self) -> anyhow::Result<ModelListResponse> {
        self.request(
            "model/list",
            ModelListParams {
                cursor: None,
                limit: None,
                include_hidden: Some(false),
            },
        )
        .await
    }

    pub async fn permission_profile_list(
        &mut self,
        cwd: Option<String>,
    ) -> anyhow::Result<PermissionProfileListResponse> {
        self.request(
            "permissionProfile/list",
            PermissionProfileListParams {
                cursor: None,
                limit: None,
                cwd,
            },
        )
        .await
    }

    pub async fn collaboration_mode_list(
        &mut self,
    ) -> anyhow::Result<CollaborationModeListResponse> {
        self.request("collaborationMode/list", CollaborationModeListParams {})
            .await
    }

    pub async fn thread_settings_update(
        &mut self,
        thread_id: String,
        model: Option<String>,
        permissions: Option<String>,
        effort: Option<String>,
        service_tier: Option<Option<String>>,
        approval_policy: Option<String>,
        collaboration_mode: Option<AppServerCollaborationMode>,
    ) -> anyhow::Result<ThreadSettingsUpdateResponse> {
        if model.is_none()
            && permissions.is_none()
            && effort.is_none()
            && service_tier.is_none()
            && approval_policy.is_none()
            && collaboration_mode.is_none()
        {
            bail!("thread/settings/update requires at least one setting");
        }

        self.request(
            "thread/settings/update",
            ThreadSettingsUpdateParams {
                thread_id,
                model,
                permissions,
                effort,
                service_tier,
                approval_policy,
                collaboration_mode,
            },
        )
        .await
    }

    pub async fn turn_start_text_until_complete(
        &mut self,
        thread_id: String,
        text: String,
        cancel_rx: Option<oneshot::Receiver<()>>,
        on_event: impl FnMut(AppServerPromptEvent) -> anyhow::Result<()>,
    ) -> anyhow::Result<AppServerPromptCompletion> {
        self.turn_start_until_complete(
            thread_id,
            vec![AppServerTurnInput::Text { text }],
            cancel_rx,
            on_event,
            |_| async { Ok(AppServerApprovalDecision::Cancel) },
        )
        .await
    }

    pub async fn turn_start_until_complete<OnEvent, OnApproval, ApprovalFuture>(
        &mut self,
        thread_id: String,
        input: Vec<AppServerTurnInput>,
        cancel_rx: Option<oneshot::Receiver<()>>,
        on_event: OnEvent,
        on_approval: OnApproval,
    ) -> anyhow::Result<AppServerPromptCompletion>
    where
        OnEvent: FnMut(AppServerPromptEvent) -> anyhow::Result<()>,
        OnApproval: FnMut(AppServerApprovalRequest) -> ApprovalFuture,
        ApprovalFuture: Future<Output = anyhow::Result<AppServerApprovalDecision>>,
    {
        let messages_rx = self.subscribe();
        let response: TurnStartResponse = self
            .request(
                "turn/start",
                TurnStartParams {
                    thread_id: thread_id.clone(),
                    input,
                },
            )
            .await?;

        self.wait_for_turn_until_complete(
            messages_rx,
            thread_id,
            Some(response.turn.id),
            cancel_rx,
            on_event,
            on_approval,
        )
        .await
    }

    pub async fn review_start_until_complete<OnEvent, OnApproval, ApprovalFuture>(
        &mut self,
        thread_id: String,
        cancel_rx: Option<oneshot::Receiver<()>>,
        on_event: OnEvent,
        on_approval: OnApproval,
    ) -> anyhow::Result<AppServerPromptCompletion>
    where
        OnEvent: FnMut(AppServerPromptEvent) -> anyhow::Result<()>,
        OnApproval: FnMut(AppServerApprovalRequest) -> ApprovalFuture,
        ApprovalFuture: Future<Output = anyhow::Result<AppServerApprovalDecision>>,
    {
        let messages_rx = self.subscribe();
        let response: TurnStartResponse = self
            .request(
                "review/start",
                ReviewStartParams {
                    thread_id: thread_id.clone(),
                },
            )
            .await?;

        self.wait_for_turn_until_complete(
            messages_rx,
            thread_id,
            Some(response.turn.id),
            cancel_rx,
            on_event,
            on_approval,
        )
        .await
    }

    pub async fn thread_compact_start_until_complete<OnEvent, OnApproval, ApprovalFuture>(
        &mut self,
        thread_id: String,
        cancel_rx: Option<oneshot::Receiver<()>>,
        on_event: OnEvent,
        on_approval: OnApproval,
    ) -> anyhow::Result<AppServerPromptCompletion>
    where
        OnEvent: FnMut(AppServerPromptEvent) -> anyhow::Result<()>,
        OnApproval: FnMut(AppServerApprovalRequest) -> ApprovalFuture,
        ApprovalFuture: Future<Output = anyhow::Result<AppServerApprovalDecision>>,
    {
        let messages_rx = self.subscribe();
        let _response: ThreadCompactStartResponse = self
            .request(
                "thread/compact/start",
                ThreadCompactStartParams {
                    thread_id: thread_id.clone(),
                },
            )
            .await?;

        self.wait_for_turn_until_complete(
            messages_rx,
            thread_id,
            None,
            cancel_rx,
            on_event,
            on_approval,
        )
        .await
    }

    async fn wait_for_turn_until_complete<OnEvent, OnApproval, ApprovalFuture>(
        &mut self,
        mut messages_rx: broadcast::Receiver<AppServerMessage>,
        thread_id: String,
        active_turn_id: Option<String>,
        cancel_rx: Option<oneshot::Receiver<()>>,
        mut on_event: OnEvent,
        mut on_approval: OnApproval,
    ) -> anyhow::Result<AppServerPromptCompletion>
    where
        OnEvent: FnMut(AppServerPromptEvent) -> anyhow::Result<()>,
        OnApproval: FnMut(AppServerApprovalRequest) -> ApprovalFuture,
        ApprovalFuture: Future<Output = anyhow::Result<AppServerApprovalDecision>>,
    {
        let mut active_turn_id = active_turn_id;
        let mut cancel_rx = cancel_rx;
        let mut interrupt_requested = false;
        if let Some(active_turn_id) = active_turn_id.as_ref() {
            trace!(turn_id = %active_turn_id, "codex app-server turn started");
        }
        loop {
            let message = if let Some(cancel) = cancel_rx.as_mut() {
                tokio::select! {
                    message = messages_rx.recv() => receive_app_server_message(message)?,
                    _ = cancel => {
                        cancel_rx = None;
                        interrupt_requested = true;
                        if let Some(active_turn_id) = active_turn_id.as_ref() {
                            let _response: TurnInterruptResponse = self
                                .request(
                                    "turn/interrupt",
                                    TurnInterruptParams {
                                        thread_id: thread_id.clone(),
                                        turn_id: active_turn_id.clone(),
                                    },
                                )
                                .await?;
                        }
                        continue;
                    }
                }
            } else {
                receive_app_server_message(messages_rx.recv().await)?
            };

            let (method, params) = match message {
                AppServerMessage::Notification { method, params } => (method, params),
                AppServerMessage::Request { id, method, params } => {
                    if let Some(approval) = decode_approval_request(
                        &method,
                        &params,
                        &thread_id,
                        active_turn_id.as_deref(),
                    )
                    .with_context(|| {
                        format!("failed to decode app-server approval request `{method}`")
                    })? {
                        let decision = match on_approval(approval.clone()).await {
                            Ok(decision) => decision,
                            Err(error) => {
                                self.write_approval_response(
                                    id,
                                    approval,
                                    AppServerApprovalDecision::Cancel,
                                )
                                .await?;
                                return Err(error);
                            }
                        };
                        self.write_approval_response(id, approval, decision).await?;
                    } else {
                        if let Some(response) =
                            fallback_interactive_request_response(&method, &params)
                        {
                            self.write_request_response(id, response).await?;
                        } else {
                            trace!(method, ?params, "ignoring app-server request during turn");
                        }
                    }
                    continue;
                }
            };

            match method.as_str() {
                "turn/started" => {
                    let notification: TurnStartedNotification =
                        serde_json::from_value(params).context("failed to decode turn started")?;
                    if notification.thread_id == thread_id && active_turn_id.is_none() {
                        active_turn_id = Some(notification.turn.id);
                        if let Some(active_turn_id) = active_turn_id.as_ref() {
                            trace!(turn_id = %active_turn_id, "codex app-server turn started");
                            if interrupt_requested {
                                let _response: TurnInterruptResponse = self
                                    .request(
                                        "turn/interrupt",
                                        TurnInterruptParams {
                                            thread_id: thread_id.clone(),
                                            turn_id: active_turn_id.clone(),
                                        },
                                    )
                                    .await?;
                            }
                        }
                    }
                }
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
                "item/started"
                | "item/completed"
                | "item/commandExecution/outputDelta"
                | "item/reasoning/summaryTextDelta"
                | "item/reasoning/textDelta"
                | "turn/diff/updated"
                | "turn/plan/updated"
                | "thread/tokenUsage/updated" => {
                    if let Some(event) = decode_prompt_event(
                        method.as_str(),
                        &params,
                        &thread_id,
                        active_turn_id.as_deref(),
                    )
                    .with_context(|| {
                        format!("failed to decode app-server notification `{method}`")
                    })? {
                        on_event(event)?;
                    }
                }
                "skills/changed" => on_event(AppServerPromptEvent::SkillsChanged)?,
                "thread/settings/updated" => {
                    if let Some(settings) = decode_thread_settings_updated_for_thread(
                        &params, &thread_id,
                    )
                    .with_context(
                        || "failed to decode app-server thread/settings/updated notification",
                    )? {
                        on_event(AppServerPromptEvent::ThreadSettingsUpdated(settings))?;
                    }
                }
                _ => {
                    trace!(
                        method,
                        ?params,
                        "ignoring app-server notification during turn"
                    );
                }
            }
        }
    }

    pub async fn request<P, R>(&mut self, method: &str, params: P) -> anyhow::Result<R>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let response_rx = self.send_request(method, params).await?;
        let message = response_rx
            .await
            .with_context(|| format!("codex app-server exited before responding to `{method}`"))?;

        if let Some(error) = message.get("error") {
            warn!(method, %error, "codex app-server request failed");
            bail!("codex app-server request `{method}` failed: {error}");
        }

        let result = message
            .get("result")
            .cloned()
            .context("app-server response did not include `result`")?;
        trace!(method, "received codex app-server response");
        serde_json::from_value(result)
            .with_context(|| format!("failed to decode app-server `{method}` response"))
    }

    pub async fn notify<P>(&mut self, method: &str, params: P) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        let notification = json!({
            "method": method,
            "params": params,
        });

        debug!(method, "sending codex app-server notification");
        self.write_message(&notification).await
    }

    async fn send_request<P>(
        &mut self,
        method: &str,
        params: P,
    ) -> anyhow::Result<oneshot::Receiver<Value>>
    where
        P: Serialize,
    {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({
            "id": id,
            "method": method,
            "params": params,
        });
        let (response_tx, response_rx) = oneshot::channel();
        self.pending_responses.lock().await.insert(id, response_tx);
        debug!(request_id = id, method, "sending codex app-server request");
        if let Err(error) = self.write_message(&request).await {
            self.pending_responses.lock().await.remove(&id);
            return Err(error);
        }
        Ok(response_rx)
    }

    async fn write_message(&self, message: &Value) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(message).context("failed to encode JSON-RPC message")?;
        line.push(b'\n');
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(&line)
            .await
            .context("failed to write to codex app-server stdin")?;
        stdin
            .flush()
            .await
            .context("failed to flush codex app-server stdin")?;
        Ok(())
    }

    async fn write_approval_response(
        &self,
        request_id: Value,
        approval: AppServerApprovalRequest,
        decision: AppServerApprovalDecision,
    ) -> anyhow::Result<()> {
        let result = approval_response_result(approval, decision);
        let response = json!({
            "id": request_id,
            "result": result,
        });
        self.write_message(&response).await
    }

    async fn write_request_response(&self, request_id: Value, result: Value) -> anyhow::Result<()> {
        let response = json!({
            "id": request_id,
            "result": result,
        });
        self.write_message(&response).await
    }
}

impl Drop for AppServerClient {
    fn drop(&mut self) {
        self.reader_task.abort();
        let _ = self.child.start_kill();
    }
}

#[derive(Debug, Clone)]
pub enum AppServerMessage {
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
}

fn spawn_reader(
    stdout: ChildStdout,
    pending_responses: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>,
    messages_tx: broadcast::Sender<AppServerMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut stdout = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            let read = match stdout.read_line(&mut line).await {
                Ok(read) => read,
                Err(error) => {
                    warn!(%error, "failed to read from codex app-server stdout");
                    break;
                }
            };
            if read == 0 {
                break;
            }

            let message: Value = match serde_json::from_str(&line) {
                Ok(message) => message,
                Err(error) => {
                    warn!(%error, "failed to decode JSON-RPC message from codex app-server");
                    continue;
                }
            };

            if let Some(method) = message.get("method").and_then(Value::as_str) {
                let params = message.get("params").cloned().unwrap_or(Value::Null);
                trace!(
                    method,
                    has_request_id = message.get("id").is_some(),
                    "received codex app-server message"
                );
                let app_server_message = if let Some(id) = message.get("id").cloned() {
                    AppServerMessage::Request {
                        id,
                        method: method.to_owned(),
                        params,
                    }
                } else {
                    AppServerMessage::Notification {
                        method: method.to_owned(),
                        params,
                    }
                };
                if messages_tx.send(app_server_message).is_err() {
                    trace!("dropping app-server notification with no active subscribers");
                }
                continue;
            }

            let Some(id) = message.get("id").and_then(Value::as_u64) else {
                trace!(?message, "ignoring app-server message without id or method");
                continue;
            };
            let Some(response_tx) = pending_responses.lock().await.remove(&id) else {
                trace!(
                    id,
                    ?message,
                    "ignoring app-server response for unknown request"
                );
                continue;
            };
            trace!(request_id = id, "received codex app-server response");
            let _ = response_tx.send(message);
        }

        pending_responses.lock().await.clear();
    })
}

fn receive_app_server_message(
    message: Result<AppServerMessage, broadcast::error::RecvError>,
) -> anyhow::Result<AppServerMessage> {
    match message {
        Ok(message) => Ok(message),
        Err(broadcast::error::RecvError::Closed) => {
            bail!("codex app-server notification stream closed")
        }
        Err(broadcast::error::RecvError::Lagged(count)) => {
            bail!("missed {count} codex app-server notifications")
        }
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
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub approval_policy: Option<String>,
    #[serde(default)]
    pub collaboration_mode: Option<AppServerCollaborationMode>,
    #[serde(default)]
    pub active_permission_profile: Option<AppServerActivePermissionProfile>,
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
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub approval_policy: Option<String>,
    #[serde(default)]
    pub collaboration_mode: Option<AppServerCollaborationMode>,
    #[serde(default)]
    pub active_permission_profile: Option<AppServerActivePermissionProfile>,
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
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub approval_policy: Option<String>,
    #[serde(default)]
    pub collaboration_mode: Option<AppServerCollaborationMode>,
    #[serde(default)]
    pub active_permission_profile: Option<AppServerActivePermissionProfile>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerActivePermissionProfile {
    pub id: String,
    #[serde(default)]
    pub extends: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadReadParams {
    thread_id: String,
    include_turns: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadReadResponse {
    pub thread: AppServerThreadHistory,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerThreadHistory {
    pub id: String,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub turns: Vec<AppServerTurnHistory>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerTurnHistory {
    pub id: String,
    #[serde(default)]
    pub items: Vec<Value>,
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
    #[serde(default)]
    pub preview: Option<String>,
    #[serde(default)]
    pub status: Option<Value>,
    #[serde(default)]
    pub model_provider: Option<String>,
    #[serde(default)]
    pub created_at: Option<i64>,
    #[serde(default)]
    pub updated_at: Option<i64>,
    #[serde(default)]
    pub recency_at: Option<i64>,
    #[serde(default)]
    pub agent_nickname: Option<String>,
    #[serde(default)]
    pub agent_role: Option<String>,
    #[serde(default)]
    pub parent_thread_id: Option<String>,
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
struct ThreadDeleteParams {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadDeleteResponse {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadArchiveParams {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadArchiveResponse {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadSetNameParams {
    thread_id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSetNameResponse {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadGoalSetParams {
    thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    objective: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_budget: Option<Option<i64>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadGoalSetResponse {
    pub goal: Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadGoalGetParams {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadGoalGetResponse {
    pub goal: Option<Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadGoalClearParams {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadGoalClearResponse {
    pub cleared: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadScopedParams {
    thread_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SkillsListParams {
    cwds: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    force_reload: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillsListResponse {
    pub data: Vec<AppServerSkillsForCwd>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SkillsConfigWriteParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    enabled: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillsConfigWriteResponse {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SkillsExtraRootsSetParams {
    roots: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillsExtraRootsSetResponse {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EmptyParams {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HooksListParams {
    cwds: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct McpServerStatusListParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelListParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    include_hidden: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelListResponse {
    pub data: Vec<AppServerModel>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerModel {
    pub id: String,
    #[serde(default)]
    pub model: Option<String>,
    pub display_name: String,
    pub description: String,
    #[serde(default)]
    pub supported_reasoning_efforts: Vec<AppServerReasoningEffortOption>,
    #[serde(default)]
    pub default_reasoning_effort: Option<String>,
    #[serde(default)]
    pub service_tiers: Vec<AppServerServiceTier>,
    #[serde(default)]
    pub default_service_tier: Option<String>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub is_default: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerReasoningEffortOption {
    pub reasoning_effort: String,
    pub description: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerServiceTier {
    pub id: String,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PermissionProfileListParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionProfileListResponse {
    pub data: Vec<AppServerPermissionProfile>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CollaborationModeListParams {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationModeListResponse {
    pub data: Vec<AppServerCollaborationModeMask>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerCollaborationModeMask {
    pub name: String,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, rename = "reasoning_effort")]
    pub reasoning_effort: Option<Option<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub struct AppServerCollaborationMode {
    pub mode: String,
    pub settings: AppServerCollaborationModeSettings,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppServerCollaborationModeSettings {
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub developer_instructions: Option<String>,
}

impl AppServerCollaborationMode {
    pub fn new(
        mode: String,
        model: String,
        reasoning_effort: Option<String>,
        developer_instructions: Option<String>,
    ) -> Self {
        Self {
            mode,
            settings: AppServerCollaborationModeSettings {
                model,
                reasoning_effort,
                developer_instructions,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerPermissionProfile {
    pub id: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadSettingsUpdateParams {
    thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    permissions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    service_tier: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collaboration_mode: Option<AppServerCollaborationMode>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSettingsUpdateResponse {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerSkillsForCwd {
    pub cwd: String,
    #[serde(default)]
    pub skills: Vec<AppServerSkill>,
    #[serde(default)]
    pub errors: Vec<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerSkill {
    pub name: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub interface: Option<AppServerSkillInterface>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerSkillInterface {
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub short_description: Option<String>,
    #[serde(default)]
    pub default_prompt: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnStartParams {
    thread_id: String,
    input: Vec<AppServerTurnInput>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReviewStartParams {
    thread_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadCompactStartParams {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadCompactStartResponse {}

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
pub enum AppServerTurnInput {
    Text { text: String },
    Skill { name: String, path: String },
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
struct TurnStartedNotification {
    thread_id: String,
    turn: AppServerTurn,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnCompletedNotification {
    thread_id: String,
    turn: AppServerTurn,
}

pub enum AppServerPromptEvent {
    AgentMessageDelta(String),
    AgentThoughtDelta(String),
    ToolCallStarted(AppServerToolCall),
    ToolCallUpdated(AppServerToolCallUpdate),
    PlanUpdated(Vec<AppServerPlanEntry>),
    TurnDiffUpdated { turn_id: String, diff: String },
    UsageUpdated(AppServerUsage),
    SkillsChanged,
    ThreadSettingsUpdated(AppServerThreadSettingsUpdate),
}

pub enum AppServerHistoryEvent {
    UserMessage(String),
    PromptEvent(AppServerPromptEvent),
}

pub enum AppServerPromptCompletion {
    EndTurn,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct AppServerApprovalRequest {
    pub item_id: String,
    pub title: String,
    pub kind: AppServerToolKind,
    pub raw: Value,
    pub options: Vec<AppServerApprovalOption>,
    pub response_kind: AppServerApprovalResponseKind,
}

#[derive(Debug, Clone)]
pub enum AppServerApprovalResponseKind {
    Decision,
    Permissions { requested_permissions: Value },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppServerApprovalOption {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
}

impl AppServerApprovalOption {
    pub fn id(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::AcceptForSession => "acceptForSession",
            Self::Decline => "decline",
            Self::Cancel => "cancel",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Accept => "Allow once",
            Self::AcceptForSession => "Allow for session",
            Self::Decline => "Reject",
            Self::Cancel => "Cancel",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppServerApprovalDecision {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
}

impl AppServerApprovalDecision {
    fn as_app_server_value(self) -> Value {
        match self {
            Self::Accept => Value::String("accept".to_owned()),
            Self::AcceptForSession => Value::String("acceptForSession".to_owned()),
            Self::Decline => Value::String("decline".to_owned()),
            Self::Cancel => Value::String("cancel".to_owned()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppServerToolCall {
    pub id: String,
    pub title: String,
    pub kind: AppServerToolKind,
    pub status: AppServerToolStatus,
    pub raw: Value,
}

#[derive(Debug, Clone)]
pub struct AppServerToolCallUpdate {
    pub id: String,
    pub title: Option<String>,
    pub kind: Option<AppServerToolKind>,
    pub status: Option<AppServerToolStatus>,
    pub output_delta: Option<String>,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone, Copy)]
pub enum AppServerToolKind {
    Read,
    Edit,
    Search,
    Execute,
    Think,
    Fetch,
    Other,
}

#[derive(Debug, Clone, Copy)]
pub enum AppServerToolStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct AppServerPlanEntry {
    pub content: String,
    pub status: AppServerPlanStatus,
}

#[derive(Debug, Clone, Copy)]
pub enum AppServerPlanStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Clone)]
pub struct AppServerUsage {
    pub used: u64,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct AppServerThreadSettingsUpdate {
    pub thread_id: String,
    pub cwd: Option<String>,
    pub approval_policy: Option<String>,
    pub model: Option<String>,
    pub service_tier: Option<String>,
    pub reasoning_effort: Option<String>,
    pub collaboration_mode: Option<AppServerCollaborationMode>,
    pub active_permission_profile: Option<AppServerActivePermissionProfile>,
}

#[derive(Debug, Clone)]
pub struct AppServerThreadNameUpdate {
    pub thread_id: String,
    pub thread_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppServerThreadArchivedUpdate {
    pub thread_id: String,
}

#[derive(Debug, Clone)]
pub struct AppServerThreadStatusUpdate {
    pub thread_id: String,
    pub status: Value,
}

#[derive(Debug, Clone)]
pub struct AppServerThreadGoalUpdate {
    pub thread_id: String,
    pub goal: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadArchivedNotification {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadStatusChangedNotification {
    thread_id: String,
    status: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadGoalUpdatedNotification {
    thread_id: String,
    goal: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadGoalClearedNotification {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadNameUpdatedNotification {
    thread_id: String,
    #[serde(default)]
    thread_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadSettingsUpdatedNotification {
    thread_id: String,
    thread_settings: ThreadSettingsNotificationState,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadSettingsNotificationState {
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default)]
    approval_policy: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    service_tier: Option<String>,
    #[serde(default)]
    effort: Option<String>,
    #[serde(default)]
    collaboration_mode: Option<AppServerCollaborationMode>,
    #[serde(default)]
    active_permission_profile: Option<AppServerActivePermissionProfile>,
}

fn decode_prompt_event(
    method: &str,
    params: &Value,
    active_thread_id: &str,
    active_turn_id: Option<&str>,
) -> anyhow::Result<Option<AppServerPromptEvent>> {
    if !matches_thread(params, active_thread_id) || !matches_turn(params, active_turn_id) {
        return Ok(None);
    }

    match method {
        "item/started" => Ok(params
            .get("item")
            .and_then(decode_started_item)
            .map(AppServerPromptEvent::ToolCallStarted)),
        "item/completed" => Ok(params
            .get("item")
            .and_then(decode_completed_item)
            .map(AppServerPromptEvent::ToolCallUpdated)),
        "item/commandExecution/outputDelta" => {
            let Some(item_id) = string_field(params, "itemId") else {
                return Ok(None);
            };
            let Some(delta) =
                string_field(params, "delta").or_else(|| string_field(params, "output"))
            else {
                return Ok(None);
            };
            Ok(Some(AppServerPromptEvent::ToolCallUpdated(
                AppServerToolCallUpdate {
                    id: item_id,
                    title: None,
                    kind: None,
                    status: None,
                    output_delta: Some(delta),
                    raw: None,
                },
            )))
        }
        "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => Ok(params
            .get("delta")
            .and_then(Value::as_str)
            .map(|delta| AppServerPromptEvent::AgentThoughtDelta(delta.to_owned()))),
        "turn/diff/updated" => {
            let Some(turn_id) =
                string_field(params, "turnId").or_else(|| active_turn_id.map(str::to_owned))
            else {
                return Ok(None);
            };
            let Some(diff) = params.get("diff").and_then(Value::as_str) else {
                return Ok(None);
            };
            Ok(Some(AppServerPromptEvent::TurnDiffUpdated {
                turn_id,
                diff: diff.to_owned(),
            }))
        }
        "turn/plan/updated" => {
            let entries = params
                .get("plan")
                .and_then(Value::as_array)
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(decode_plan_entry)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            Ok(Some(AppServerPromptEvent::PlanUpdated(entries)))
        }
        "thread/tokenUsage/updated" => {
            Ok(decode_usage(params).map(AppServerPromptEvent::UsageUpdated))
        }
        _ => Ok(None),
    }
}

fn decode_approval_request(
    method: &str,
    params: &Value,
    active_thread_id: &str,
    active_turn_id: Option<&str>,
) -> anyhow::Result<Option<AppServerApprovalRequest>> {
    if !matches_thread(params, active_thread_id) || !matches_turn(params, active_turn_id) {
        return Ok(None);
    }

    let Some(item_id) = string_field(params, "itemId") else {
        return Ok(None);
    };

    let request = match method {
        "item/commandExecution/requestApproval" => {
            let command = string_field(params, "command").unwrap_or_else(|| "command".to_owned());
            AppServerApprovalRequest {
                item_id,
                title: format!("Run `{command}`"),
                kind: AppServerToolKind::Execute,
                raw: params.clone(),
                response_kind: AppServerApprovalResponseKind::Decision,
                options: approval_options_from_params(
                    params,
                    &[
                        AppServerApprovalOption::Accept,
                        AppServerApprovalOption::AcceptForSession,
                        AppServerApprovalOption::Decline,
                        AppServerApprovalOption::Cancel,
                    ],
                ),
            }
        }
        "item/fileChange/requestApproval" => AppServerApprovalRequest {
            item_id,
            title: "Apply file changes".to_owned(),
            kind: AppServerToolKind::Edit,
            raw: params.clone(),
            response_kind: AppServerApprovalResponseKind::Decision,
            options: approval_options_from_params(
                params,
                &[
                    AppServerApprovalOption::Accept,
                    AppServerApprovalOption::AcceptForSession,
                    AppServerApprovalOption::Decline,
                    AppServerApprovalOption::Cancel,
                ],
            ),
        },
        "item/permissions/requestApproval" => {
            let requested_permissions = params
                .get("permissions")
                .cloned()
                .unwrap_or_else(|| json!({}));
            AppServerApprovalRequest {
                item_id,
                title: string_field(params, "reason")
                    .unwrap_or_else(|| "Grant additional permissions".to_owned()),
                kind: AppServerToolKind::Other,
                raw: params.clone(),
                response_kind: AppServerApprovalResponseKind::Permissions {
                    requested_permissions,
                },
                options: vec![
                    AppServerApprovalOption::Accept,
                    AppServerApprovalOption::AcceptForSession,
                    AppServerApprovalOption::Decline,
                    AppServerApprovalOption::Cancel,
                ],
            }
        }
        _ => return Ok(None),
    };

    Ok(Some(request))
}

pub fn decode_thread_settings_updated(
    params: &Value,
) -> anyhow::Result<AppServerThreadSettingsUpdate> {
    let notification: ThreadSettingsUpdatedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerThreadSettingsUpdate {
        thread_id: notification.thread_id,
        cwd: notification
            .thread_settings
            .cwd
            .map(|cwd| cwd.to_string_lossy().into_owned()),
        approval_policy: notification.thread_settings.approval_policy,
        model: notification.thread_settings.model,
        service_tier: notification.thread_settings.service_tier,
        reasoning_effort: notification.thread_settings.effort,
        collaboration_mode: notification.thread_settings.collaboration_mode,
        active_permission_profile: notification.thread_settings.active_permission_profile,
    })
}

pub fn decode_thread_name_updated(params: &Value) -> anyhow::Result<AppServerThreadNameUpdate> {
    let notification: ThreadNameUpdatedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerThreadNameUpdate {
        thread_id: notification.thread_id,
        thread_name: notification.thread_name,
    })
}

pub fn decode_thread_archived(params: &Value) -> anyhow::Result<AppServerThreadArchivedUpdate> {
    let notification: ThreadArchivedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerThreadArchivedUpdate {
        thread_id: notification.thread_id,
    })
}

pub fn decode_thread_status_changed(params: &Value) -> anyhow::Result<AppServerThreadStatusUpdate> {
    let notification: ThreadStatusChangedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerThreadStatusUpdate {
        thread_id: notification.thread_id,
        status: notification.status,
    })
}

pub fn decode_thread_goal_updated(params: &Value) -> anyhow::Result<AppServerThreadGoalUpdate> {
    let notification: ThreadGoalUpdatedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerThreadGoalUpdate {
        thread_id: notification.thread_id,
        goal: Some(notification.goal),
    })
}

pub fn decode_thread_goal_cleared(params: &Value) -> anyhow::Result<AppServerThreadGoalUpdate> {
    let notification: ThreadGoalClearedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerThreadGoalUpdate {
        thread_id: notification.thread_id,
        goal: None,
    })
}

fn decode_thread_settings_updated_for_thread(
    params: &Value,
    active_thread_id: &str,
) -> anyhow::Result<Option<AppServerThreadSettingsUpdate>> {
    let notification = decode_thread_settings_updated(params)?;
    if notification.thread_id != active_thread_id {
        return Ok(None);
    }
    Ok(Some(notification))
}

fn approval_options_from_params(
    params: &Value,
    defaults: &[AppServerApprovalOption],
) -> Vec<AppServerApprovalOption> {
    let parsed = params
        .get("availableDecisions")
        .and_then(Value::as_array)
        .map(|decisions| {
            decisions
                .iter()
                .filter_map(|decision| decision.as_str().and_then(approval_option_from_id))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if parsed.is_empty() {
        defaults.to_vec()
    } else {
        parsed
    }
}

fn approval_option_from_id(id: &str) -> Option<AppServerApprovalOption> {
    match id {
        "accept" => Some(AppServerApprovalOption::Accept),
        "acceptForSession" => Some(AppServerApprovalOption::AcceptForSession),
        "decline" => Some(AppServerApprovalOption::Decline),
        "cancel" => Some(AppServerApprovalOption::Cancel),
        _ => None,
    }
}

fn approval_response_result(
    approval: AppServerApprovalRequest,
    decision: AppServerApprovalDecision,
) -> Value {
    match approval.response_kind {
        AppServerApprovalResponseKind::Decision => json!({
            "decision": decision.as_app_server_value(),
        }),
        AppServerApprovalResponseKind::Permissions {
            requested_permissions,
        } => {
            let (permissions, scope) = match decision {
                AppServerApprovalDecision::Accept => (requested_permissions, "turn"),
                AppServerApprovalDecision::AcceptForSession => (requested_permissions, "session"),
                AppServerApprovalDecision::Decline | AppServerApprovalDecision::Cancel => {
                    (json!({}), "turn")
                }
            };
            json!({
                "permissions": permissions,
                "scope": scope,
            })
        }
    }
}

fn fallback_interactive_request_response(method: &str, _params: &Value) -> Option<Value> {
    match method {
        "mcpServer/elicitation/request" => Some(json!({
            "action": "cancel",
            "content": null,
            "_meta": null,
        })),
        "item/tool/requestUserInput" => Some(json!({
            "answers": {},
        })),
        "item/tool/call" => Some(json!({
            "contentItems": [
                {
                    "type": "inputText",
                    "text": "Dynamic tool calls are not supported by this ACP adapter yet.",
                },
            ],
            "success": false,
        })),
        _ => None,
    }
}

pub fn history_events(thread: &AppServerThreadHistory) -> Vec<AppServerHistoryEvent> {
    thread
        .turns
        .iter()
        .flat_map(|turn| {
            turn.items
                .iter()
                .flat_map(|item| history_events_for_item(&turn.id, item))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn history_events_for_item(turn_id: &str, item: &Value) -> Vec<AppServerHistoryEvent> {
    let Some(item_type) = item.get("type").and_then(Value::as_str) else {
        return Vec::new();
    };

    match item_type {
        "userMessage" => user_message_text(item)
            .map(AppServerHistoryEvent::UserMessage)
            .into_iter()
            .collect(),
        "agentMessage" => string_field(item, "text")
            .map(AppServerPromptEvent::AgentMessageDelta)
            .map(AppServerHistoryEvent::PromptEvent)
            .into_iter()
            .collect(),
        "reasoning" => reasoning_text(item)
            .map(AppServerPromptEvent::AgentThoughtDelta)
            .map(AppServerHistoryEvent::PromptEvent)
            .into_iter()
            .collect(),
        "plan" => string_field(item, "text")
            .map(|text| {
                AppServerPromptEvent::PlanUpdated(vec![AppServerPlanEntry {
                    content: text,
                    status: AppServerPlanStatus::Completed,
                }])
            })
            .map(AppServerHistoryEvent::PromptEvent)
            .into_iter()
            .collect(),
        "fileChange" => {
            let mut events = tool_history_events(item);
            if let Some(diff) = file_change_diff(item) {
                events.push(AppServerPromptEvent::TurnDiffUpdated {
                    turn_id: turn_id.to_owned(),
                    diff,
                });
            }
            events
                .into_iter()
                .map(AppServerHistoryEvent::PromptEvent)
                .collect()
        }
        "commandExecution" | "mcpToolCall" | "collabToolCall" | "dynamicToolCall" | "webSearch"
        | "imageView" | "sleep" | "contextCompaction" | "enteredReviewMode"
        | "exitedReviewMode" => tool_history_events(item)
            .into_iter()
            .map(AppServerHistoryEvent::PromptEvent)
            .collect(),
        _ => Vec::new(),
    }
}

fn tool_history_events(item: &Value) -> Vec<AppServerPromptEvent> {
    let mut events = Vec::new();
    if let Some(started) = decode_started_item(item) {
        events.push(AppServerPromptEvent::ToolCallStarted(started));
    }
    if let Some(completed) = decode_completed_item(item) {
        events.push(AppServerPromptEvent::ToolCallUpdated(completed));
    }
    events
}

fn user_message_text(item: &Value) -> Option<String> {
    let content = item.get("content")?.as_array()?;
    let text = content
        .iter()
        .filter_map(user_content_text)
        .collect::<Vec<_>>()
        .join("\n");
    (!text.trim().is_empty()).then_some(text)
}

fn user_content_text(content: &Value) -> Option<String> {
    match content.get("type").and_then(Value::as_str) {
        Some("text") => string_field(content, "text"),
        Some("image") => string_field(content, "url").map(|url| format!("[image: {url}]")),
        Some("localImage") => string_field(content, "path").map(|path| format!("[image: {path}]")),
        _ => None,
    }
}

fn reasoning_text(item: &Value) -> Option<String> {
    let mut parts = Vec::new();
    collect_text_fragments(item.get("summary"), &mut parts);
    collect_text_fragments(item.get("content"), &mut parts);
    let text = parts.join("\n");
    (!text.trim().is_empty()).then_some(text)
}

fn collect_text_fragments(value: Option<&Value>, parts: &mut Vec<String>) {
    match value {
        Some(Value::String(text)) if !text.trim().is_empty() => parts.push(text.clone()),
        Some(Value::Array(values)) => {
            for value in values {
                collect_text_fragments(Some(value), parts);
            }
        }
        Some(Value::Object(object)) => {
            for key in ["text", "summary", "content"] {
                collect_text_fragments(object.get(key), parts);
            }
        }
        _ => {}
    }
}

fn file_change_diff(item: &Value) -> Option<String> {
    let changes = item.get("changes")?.as_array()?;
    let diff = changes
        .iter()
        .filter_map(|change| {
            let path = change
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            change
                .get("diff")
                .and_then(Value::as_str)
                .map(|diff| format!("### {path}\n{diff}"))
        })
        .collect::<Vec<_>>()
        .join("\n");
    (!diff.trim().is_empty()).then_some(diff)
}

fn matches_thread(params: &Value, active_thread_id: &str) -> bool {
    params
        .get("threadId")
        .and_then(Value::as_str)
        .is_none_or(|thread_id| thread_id == active_thread_id)
}

fn matches_turn(params: &Value, active_turn_id: Option<&str>) -> bool {
    params
        .get("turnId")
        .and_then(Value::as_str)
        .is_none_or(|turn_id| Some(turn_id) == active_turn_id)
}

fn decode_started_item(item: &Value) -> Option<AppServerToolCall> {
    let item_type = item.get("type")?.as_str()?;
    let id = item_id(item)?;

    let (title, kind, status) = match item_type {
        "commandExecution" => (
            command_title(item),
            AppServerToolKind::Execute,
            decode_tool_status(item).unwrap_or(AppServerToolStatus::InProgress),
        ),
        "fileChange" => (
            file_change_title(item),
            AppServerToolKind::Edit,
            decode_tool_status(item).unwrap_or(AppServerToolStatus::InProgress),
        ),
        "mcpToolCall" => (
            mcp_tool_title(item),
            AppServerToolKind::Other,
            decode_tool_status(item).unwrap_or(AppServerToolStatus::InProgress),
        ),
        "collabToolCall" | "dynamicToolCall" => (
            generic_tool_title(item, item_type),
            AppServerToolKind::Other,
            decode_tool_status(item).unwrap_or(AppServerToolStatus::InProgress),
        ),
        "webSearch" => (
            web_search_title(item),
            AppServerToolKind::Search,
            AppServerToolStatus::InProgress,
        ),
        "imageView" => (
            generic_tool_title(item, "imageView"),
            AppServerToolKind::Read,
            AppServerToolStatus::Completed,
        ),
        "sleep" => (
            generic_tool_title(item, "sleep"),
            AppServerToolKind::Other,
            AppServerToolStatus::InProgress,
        ),
        "contextCompaction" => (
            "Compacting conversation context".to_owned(),
            AppServerToolKind::Think,
            AppServerToolStatus::InProgress,
        ),
        "enteredReviewMode" => (
            "Starting review".to_owned(),
            AppServerToolKind::Think,
            AppServerToolStatus::InProgress,
        ),
        "exitedReviewMode" => (
            "Review result".to_owned(),
            AppServerToolKind::Think,
            AppServerToolStatus::Completed,
        ),
        _ => return None,
    };

    Some(AppServerToolCall {
        id,
        title,
        kind,
        status,
        raw: item.clone(),
    })
}

fn decode_completed_item(item: &Value) -> Option<AppServerToolCallUpdate> {
    let item_type = item.get("type")?.as_str()?;
    let id = item_id(item)?;
    let status = match item_type {
        "commandExecution" | "fileChange" | "mcpToolCall" | "collabToolCall"
        | "dynamicToolCall" | "webSearch" | "sleep" | "contextCompaction" | "enteredReviewMode"
        | "exitedReviewMode" => {
            Some(decode_tool_status(item).unwrap_or(AppServerToolStatus::Completed))
        }
        _ => None,
    }?;

    Some(AppServerToolCallUpdate {
        id,
        title: None,
        kind: None,
        status: Some(status),
        output_delta: final_item_output(item),
        raw: Some(item.clone()),
    })
}

fn decode_plan_entry(entry: &Value) -> Option<AppServerPlanEntry> {
    let content = string_field(entry, "step").or_else(|| string_field(entry, "content"))?;
    let status = match entry
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("pending")
    {
        "inProgress" | "in_progress" => AppServerPlanStatus::InProgress,
        "completed" => AppServerPlanStatus::Completed,
        _ => AppServerPlanStatus::Pending,
    };
    Some(AppServerPlanEntry { content, status })
}

fn decode_usage(params: &Value) -> Option<AppServerUsage> {
    let used = params
        .get("used")
        .or_else(|| params.get("inputTokens"))
        .or_else(|| params.get("totalTokens"))
        .and_then(Value::as_u64)?;
    let size = params
        .get("size")
        .or_else(|| params.get("contextWindow"))
        .or_else(|| params.get("tokenLimit"))
        .and_then(Value::as_u64)?;
    Some(AppServerUsage { used, size })
}

fn decode_tool_status(item: &Value) -> Option<AppServerToolStatus> {
    match item.get("status").and_then(Value::as_str)? {
        "pending" => Some(AppServerToolStatus::Pending),
        "inProgress" | "in_progress" => Some(AppServerToolStatus::InProgress),
        "completed" => Some(AppServerToolStatus::Completed),
        "failed" | "declined" => Some(AppServerToolStatus::Failed),
        _ => None,
    }
}

fn item_id(item: &Value) -> Option<String> {
    string_field(item, "id")
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field)?.as_str().map(ToOwned::to_owned)
}

fn command_title(item: &Value) -> String {
    match item.get("command") {
        Some(Value::String(command)) => command.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" "),
        _ => "Shell command".to_owned(),
    }
}

fn file_change_title(item: &Value) -> String {
    let count = item
        .get("changes")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    match count {
        0 => "File changes".to_owned(),
        1 => "Edit 1 file".to_owned(),
        count => format!("Edit {count} files"),
    }
}

fn mcp_tool_title(item: &Value) -> String {
    let server = item.get("server").and_then(Value::as_str);
    let tool = item.get("tool").and_then(Value::as_str);
    match (server, tool) {
        (Some(server), Some(tool)) => format!("{server}.{tool}"),
        (_, Some(tool)) => tool.to_owned(),
        _ => "MCP tool call".to_owned(),
    }
}

fn web_search_title(item: &Value) -> String {
    item.get("query")
        .and_then(Value::as_str)
        .map(|query| format!("Search web for {query}"))
        .unwrap_or_else(|| "Web search".to_owned())
}

fn generic_tool_title(item: &Value, item_type: &str) -> String {
    item.get("tool")
        .or_else(|| item.get("path"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| item_type.to_owned())
}

fn final_item_output(item: &Value) -> Option<String> {
    string_field(item, "aggregatedOutput")
        .or_else(|| string_field(item, "error"))
        .or_else(|| string_field(item, "review"))
        .or_else(|| {
            let output = compact_json_without_internal_fields(item);
            (!output.is_empty()).then_some(output)
        })
}

fn compact_json_without_internal_fields(item: &Value) -> String {
    let Some(object) = item.as_object() else {
        return String::new();
    };
    let mut fields: HashMap<String, Value> = object
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "id" | "type" | "status"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if fields.is_empty() {
        return String::new();
    }
    serde_json::to_string(&fields).unwrap_or_else(|_| {
        fields.clear();
        String::new()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn permissions_approval() -> AppServerApprovalRequest {
        let requested_permissions = json!({
            "network": {"enabled": true},
            "fileSystem": {
                "read": ["/repo"],
                "write": ["/repo/src"],
            },
        });
        AppServerApprovalRequest {
            item_id: "permissions-approval".to_owned(),
            title: "Grant permissions".to_owned(),
            kind: AppServerToolKind::Other,
            raw: json!({}),
            options: vec![],
            response_kind: AppServerApprovalResponseKind::Permissions {
                requested_permissions,
            },
        }
    }

    #[test]
    fn permission_approval_accept_grants_requested_permissions_for_turn() {
        let response =
            approval_response_result(permissions_approval(), AppServerApprovalDecision::Accept);

        assert_eq!(response["scope"], "turn");
        assert_eq!(response["permissions"]["network"]["enabled"], true);
        assert_eq!(
            response["permissions"]["fileSystem"]["write"][0],
            "/repo/src"
        );
    }

    #[test]
    fn permission_approval_accept_for_session_grants_requested_permissions_for_session() {
        let response = approval_response_result(
            permissions_approval(),
            AppServerApprovalDecision::AcceptForSession,
        );

        assert_eq!(response["scope"], "session");
        assert_eq!(response["permissions"]["network"]["enabled"], true);
    }

    #[test]
    fn permission_approval_rejects_with_empty_turn_scoped_permissions() {
        for decision in [
            AppServerApprovalDecision::Decline,
            AppServerApprovalDecision::Cancel,
        ] {
            let response = approval_response_result(permissions_approval(), decision);

            assert_eq!(response["scope"], "turn");
            assert_eq!(response["permissions"], json!({}));
        }
    }

    #[test]
    fn fallback_interactive_requests_cancel_or_fail_without_blocking_app_server() {
        assert_eq!(
            fallback_interactive_request_response("mcpServer/elicitation/request", &json!({}))
                .unwrap(),
            json!({
                "action": "cancel",
                "content": null,
                "_meta": null,
            })
        );
        assert_eq!(
            fallback_interactive_request_response("item/tool/requestUserInput", &json!({}))
                .unwrap(),
            json!({
                "answers": {},
            })
        );
        assert_eq!(
            fallback_interactive_request_response("item/tool/call", &json!({})).unwrap(),
            json!({
                "contentItems": [
                    {
                        "type": "inputText",
                        "text": "Dynamic tool calls are not supported by this ACP adapter yet.",
                    },
                ],
                "success": false,
            })
        );
    }

    #[test]
    fn thread_status_changed_preserves_status_payload() {
        let update = decode_thread_status_changed(&json!({
            "threadId": "thread-1",
            "status": {
                "type": "active",
                "activeFlags": ["waitingOnApproval"],
            },
        }))
        .unwrap();

        assert_eq!(update.thread_id, "thread-1");
        assert_eq!(update.status["type"], "active");
        assert_eq!(update.status["activeFlags"][0], "waitingOnApproval");
    }
}
