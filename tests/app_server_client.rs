use std::fs;

use brokk_codex_acp::app_server::{
    AppServerClient, AppServerCommand, AppServerPromptCompletion, AppServerPromptEvent,
};
use tempfile::TempDir;
use tokio::{sync::oneshot, time};

#[tokio::test]
async fn app_server_client_maps_thread_and_prompt_methods() -> anyhow::Result<()> {
    let fake_codex = fake_codex_app_server()?;
    let mut client =
        AppServerClient::spawn(AppServerCommand::new(fake_codex.path().to_owned())).await?;

    let initialize = client
        .initialize("test_client", "Test Client", "0.0.0")
        .await?;
    assert_eq!(initialize.codex_home, "/tmp/fake-codex-home");

    let started = client.thread_start("/repo".to_string()).await?;
    assert_eq!(started.thread.id, "thread-1");
    assert_eq!(
        started.thread.cwd.as_deref(),
        Some(std::path::Path::new("/repo"))
    );

    let forked = client
        .thread_fork("thread-1".to_string(), "/repo-fork".to_string())
        .await?;
    assert_eq!(forked.thread.id, "thread-2");

    let listed = client.thread_list(Some("/repo".to_string()), None).await?;
    assert_eq!(listed.data.len(), 1);
    assert_eq!(listed.data[0].id, "thread-1");

    let mut deltas = Vec::new();
    client
        .turn_start_text_until_complete(
            "thread-1".to_string(),
            "hello".to_string(),
            None,
            |event| {
                match event {
                    AppServerPromptEvent::AgentMessageDelta(delta) => deltas.push(delta),
                }
                Ok(())
            },
        )
        .await?;
    assert_eq!(deltas, vec!["fake response"]);

    let (cancel_tx, cancel_rx) = oneshot::channel();
    tokio::spawn(async move {
        time::sleep(time::Duration::from_millis(50)).await;
        let _ = cancel_tx.send(());
    });
    let cancelled = client
        .turn_start_text_until_complete(
            "thread-1".to_string(),
            "cancel me".to_string(),
            Some(cancel_rx),
            |_event| Ok(()),
        )
        .await?;
    assert!(matches!(cancelled, AppServerPromptCompletion::Cancelled));

    let closed = client.thread_unsubscribe("thread-1".to_string()).await?;
    assert_eq!(closed.status, "ok");

    Ok(())
}

struct FakeCodex {
    _temp_dir: TempDir,
    path: std::path::PathBuf,
}

impl FakeCodex {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

fn fake_codex_app_server() -> anyhow::Result<FakeCodex> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("codex");
    fs::write(&path, FAKE_CODEX_APP_SERVER)?;

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

const FAKE_CODEX_APP_SERVER: &str = r#"#!/usr/bin/env python3
import json
import sys


def send(message):
    print(json.dumps(message), flush=True)


def response(message_id, result):
    send({"id": message_id, "result": result})


for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    params = message.get("params") or {}
    message_id = message.get("id")

    if method == "initialize":
        response(message_id, {
            "userAgent": "fake-codex/0.0.0",
            "codexHome": "/tmp/fake-codex-home",
            "platformFamily": "unix",
            "platformOs": "macos",
        })
    elif method == "initialized":
        continue
    elif method == "thread/start":
        assert params["cwd"] == "/repo"
        response(message_id, {
            "thread": {
                "id": "thread-1",
                "cwd": params["cwd"],
                "name": "Started Thread",
            }
        })
    elif method == "thread/fork":
        assert params["threadId"] == "thread-1"
        assert params["cwd"] == "/repo-fork"
        response(message_id, {
            "thread": {
                "id": "thread-2",
                "cwd": params["cwd"],
                "name": "Forked Thread",
            }
        })
    elif method == "thread/list":
        assert params["cwd"] == "/repo"
        response(message_id, {
            "data": [
                {
                    "id": "thread-1",
                    "cwd": "/repo",
                    "name": "Started Thread",
                }
            ],
            "nextCursor": None,
        })
    elif method == "turn/start":
        assert params["threadId"] == "thread-1"
        if params["input"] == [{"type": "text", "text": "hello"}]:
            response(message_id, {"turn": {"id": "turn-1", "status": "running"}})
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "itemId": "item-1",
                    "delta": "fake response",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-1", "status": "completed"},
                },
            })
        elif params["input"] == [{"type": "text", "text": "cancel me"}]:
            response(message_id, {"turn": {"id": "turn-2", "status": "running"}})
        else:
            raise AssertionError(f"unexpected input: {params['input']}")
    elif method == "turn/interrupt":
        assert params["threadId"] == "thread-1"
        assert params["turnId"] == "turn-2"
        response(message_id, {})
        send({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-1",
                "turn": {"id": "turn-2", "status": "completed"},
            },
        })
    elif method == "thread/unsubscribe":
        assert params["threadId"] == "thread-1"
        response(message_id, {"status": "ok"})
    else:
        send({
            "id": message_id,
            "error": {
                "code": -32601,
                "message": f"unknown method: {method}",
            },
        })
"#;
