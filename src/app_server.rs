use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
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
