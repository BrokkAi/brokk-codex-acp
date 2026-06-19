use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_client_protocol::ByteStreams;
use brokk_codex_acp::agent::CodexAcpAgent;
use brokk_codex_acp::app_server::{AppServerClient, AppServerCommand};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf, split};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

#[tokio::test]
async fn serialized_rename_emits_session_info_update_without_starting_turn() -> anyhow::Result<()> {
    let fake_codex = fake_codex_app_server(SERIALIZED_RENAME_CODEX_APP_SERVER)?;
    let mut app_server =
        AppServerClient::spawn(AppServerCommand::new(fake_codex.path().to_owned())).await?;
    app_server
        .initialize(
            "brokk_codex_acp_test",
            "Brokk Codex ACP Test",
            env!("CARGO_PKG_VERSION"),
        )
        .await?;

    let (agent_side, client_side) = tokio::io::duplex(64 * 1024);
    let (agent_read, agent_write) = split(agent_side);
    let agent_transport = ByteStreams::new(agent_write.compat_write(), agent_read.compat());
    let agent_task = tokio::spawn(CodexAcpAgent::new(app_server).serve(agent_transport));

    let (client_read, mut client_write) = split(client_side);
    let mut client_read = BufReader::new(client_read);
    let mut notifications = Vec::new();

    write_json(
        &mut client_write,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": 1,
                "clientCapabilities": {},
            },
        }),
    )
    .await?;
    let initialize = read_response(&mut client_read, 1, &mut notifications).await?;
    assert_eq!(initialize["result"]["protocolVersion"], 1);

    let cwd = tempfile::tempdir()?;
    write_json(
        &mut client_write,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {
                "cwd": cwd.path(),
                "mcpServers": [],
            },
        }),
    )
    .await?;
    let session = read_response(&mut client_read, 2, &mut notifications).await?;
    let session_id = session["result"]["sessionId"]
        .as_str()
        .expect("session/new should return a session id")
        .to_owned();

    write_json(
        &mut client_write,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [
                    {
                        "type": "text",
                        "text": "/rename Serialized Rename",
                    },
                ],
            },
        }),
    )
    .await?;
    let prompt = read_response(&mut client_read, 3, &mut notifications).await?;
    assert_eq!(prompt["result"]["stopReason"], "end_turn");

    assert!(
        notifications.iter().any(|notification| {
            notification["method"] == "session/update"
                && notification["params"]["update"]["sessionUpdate"] == "session_info_update"
                && notification["params"]["update"]["title"] == "Serialized Rename"
        }),
        "notifications: {notifications:#?}"
    );

    drop(client_write);
    agent_task.abort();
    Ok(())
}

async fn write_json(writer: &mut (impl AsyncWrite + Unpin), message: Value) -> anyhow::Result<()> {
    let mut line = serde_json::to_vec(&message)?;
    line.push(b'\n');
    writer.write_all(&line).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_response(
    reader: &mut BufReader<ReadHalf<tokio::io::DuplexStream>>,
    id: u64,
    notifications: &mut Vec<Value>,
) -> anyhow::Result<Value> {
    loop {
        let message = read_json(reader).await?;
        if message.get("id").and_then(Value::as_u64) == Some(id) {
            return Ok(message);
        }
        notifications.push(message);
    }
}

async fn read_json(
    reader: &mut BufReader<ReadHalf<tokio::io::DuplexStream>>,
) -> anyhow::Result<Value> {
    let mut line = String::new();
    let read = tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line)).await??;
    anyhow::ensure!(
        read > 0,
        "ACP connection closed while waiting for a message"
    );
    Ok(serde_json::from_str(&line)?)
}

struct FakeCodex {
    _temp_dir: TempDir,
    path: PathBuf,
}

impl FakeCodex {
    fn path(&self) -> &Path {
        &self.path
    }
}

fn fake_codex_app_server(script: &str) -> anyhow::Result<FakeCodex> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("codex");
    fs::write(&path, script)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions)?;
    }
    Ok(FakeCodex {
        _temp_dir: temp_dir,
        path,
    })
}

const SERIALIZED_RENAME_CODEX_APP_SERVER: &str = r#"#!/usr/bin/env python3
import json
import sys

thread_cwd = None


def response(message_id, payload):
    print(json.dumps({"id": message_id, **payload}), flush=True)


for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialized":
        continue
    message_id = message["id"]
    params = message.get("params", {})
    if method == "initialize":
        response(message_id, {
            "result": {
                "userAgent": "serialized-rename-test",
                "codexHome": "/tmp/fake-codex-home",
                "platformFamily": "test",
                "platformOs": "test",
            },
        })
    elif method == "thread/start":
        thread_cwd = params.get("cwd")
        response(message_id, {
            "result": {
                "thread": {
                    "id": "thread-serialized",
                    "cwd": thread_cwd,
                    "turns": [],
                },
            },
        })
    elif method == "model/list":
        response(message_id, {"result": {"data": []}})
    elif method == "collaborationMode/list":
        response(message_id, {"result": {"data": []}})
    elif method == "permissionProfile/list":
        response(message_id, {"result": {"data": []}})
    elif method == "skills/list":
        response(message_id, {"result": {"data": [{"cwd": thread_cwd, "skills": []}]}})
    elif method == "thread/name/set":
        assert params["threadId"] == "thread-serialized"
        assert params["name"] == "Serialized Rename"
        response(message_id, {"result": {}})
    elif method == "turn/start":
        response(message_id, {
            "error": {
                "code": -32000,
                "message": "turn/start should not be called for /rename",
            },
        })
    else:
        response(message_id, {
            "error": {
                "code": -32601,
                "message": f"unknown method: {method}",
            },
        })
"#;
