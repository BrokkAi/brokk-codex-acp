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
    let (prompt, notifications) = run_serialized_prompt("/rename Serialized Rename").await?;

    assert_eq!(prompt["result"]["stopReason"], "end_turn");
    assert!(
        notifications.iter().any(|notification| {
            notification["method"] == "session/update"
                && notification["params"]["update"]["sessionUpdate"] == "session_info_update"
                && notification["params"]["update"]["title"] == "Serialized Rename"
        }),
        "notifications: {notifications:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn serialized_archive_emits_session_info_meta_without_starting_turn() -> anyhow::Result<()> {
    let (prompt, notifications) = run_serialized_prompt("/archive").await?;

    assert_eq!(prompt["result"]["stopReason"], "end_turn");
    assert!(
        notifications.iter().any(|notification| {
            notification["method"] == "session/update"
                && notification["params"]["update"]["sessionUpdate"] == "session_info_update"
                && notification["params"]["update"]["_meta"]["brokk_codex_acp"]["archived"] == true
        }),
        "notifications: {notifications:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn serialized_unarchive_emits_session_info_meta_without_starting_turn() -> anyhow::Result<()>
{
    let (prompt, notifications) = run_serialized_prompt("/unarchive").await?;

    assert_eq!(prompt["result"]["stopReason"], "end_turn");
    assert!(
        notifications.iter().any(|notification| {
            notification["method"] == "session/update"
                && notification["params"]["update"]["sessionUpdate"] == "session_info_update"
                && notification["params"]["update"]["_meta"]["brokk_codex_acp"]["archived"] == false
        }),
        "notifications: {notifications:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn serialized_goal_emits_session_info_meta_without_starting_turn() -> anyhow::Result<()> {
    let (prompt, notifications) =
        run_serialized_prompt("/goal Improve serialized coverage").await?;

    assert_eq!(prompt["result"]["stopReason"], "end_turn");
    assert!(
        notifications.iter().any(|notification| {
            notification["method"] == "session/update"
                && notification["params"]["update"]["sessionUpdate"] == "session_info_update"
                && notification["params"]["update"]["_meta"]["brokk_codex_acp"]["goal"]["objective"]
                    == "Improve serialized coverage"
        }),
        "notifications: {notifications:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn serialized_load_replays_history_before_response() -> anyhow::Result<()> {
    let (load, notifications) = run_serialized_load("thread-serialized").await?;

    assert!(load.get("error").is_none(), "load response: {load:#?}");

    let replay_updates = notifications
        .iter()
        .filter(|notification| notification["method"] == "session/update")
        .filter_map(|notification| {
            let update = &notification["params"]["update"];
            let kind = update["sessionUpdate"].as_str()?;
            let text = update["content"]["text"].as_str()?;
            Some(format!("{kind}:{text}"))
        })
        .collect::<Vec<_>>();

    assert_eq!(
        replay_updates,
        [
            "user_message_chunk:loaded hello",
            "agent_message_chunk:loaded response"
        ],
        "notifications before session/load response: {notifications:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn serialized_prompt_emits_session_update_notification_families() -> anyhow::Result<()> {
    let (prompt, notifications) = run_serialized_prompt("serialized notifications").await?;

    assert_eq!(prompt["result"]["stopReason"], "end_turn");

    let session_updates = notifications
        .iter()
        .filter(|notification| notification["method"] == "session/update")
        .map(|notification| &notification["params"]["update"])
        .collect::<Vec<_>>();

    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "agent_thought_chunk"
                && update["content"]["text"] == "thinking"
        }),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates
            .iter()
            .any(|update| update["sessionUpdate"] == "plan"),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "tool_call" && update["toolCallId"] == "cmd-1"
        }),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "tool_call_update" && update["toolCallId"] == "cmd-1"
        }),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "tool_call"
                && update["toolCallId"] == "mcp-1"
                && update["title"] == "filesystem.read_file"
        }),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "tool_call_update"
                && update["toolCallId"] == "mcp-1"
                && update["status"] == "completed"
        }),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "tool_call"
                && update["toolCallId"] == "file-1"
                && update["kind"] == "edit"
        }),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "tool_call_update"
                && update["toolCallId"] == "file-1"
                && update["status"] == "completed"
        }),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "tool_call"
                && update["toolCallId"] == "turn-diff:turn-serialized-notifications"
        }),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates
            .iter()
            .any(|update| { update["sessionUpdate"] == "usage_update" && update["used"] == 42 }),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "agent_message_chunk"
                && update["content"]["text"] == "serialized response"
        }),
        "session updates: {session_updates:#?}"
    );
    let agent_messages = session_updates
        .iter()
        .filter(|update| update["sessionUpdate"] == "agent_message_chunk")
        .filter_map(|update| update["content"]["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    for expected in [
        "Codex warning: limited skills loaded",
        "Codex rerouted the model from `gpt-5-codex` to `gpt-5` (High Risk Cyber Activity) for this turn.",
        "Codex error (retrying): transient failure",
        "retry details",
        "Code: Rate Limit",
    ] {
        assert!(
            agent_messages.contains(expected),
            "agent messages did not include {expected:?}: {agent_messages:#?}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn serialized_bang_prompt_runs_shell_command_without_starting_turn() -> anyhow::Result<()> {
    let (prompt, notifications) = run_serialized_prompt("!echo hi").await?;

    assert_eq!(prompt["result"]["stopReason"], "end_turn");

    let session_updates = notifications
        .iter()
        .filter(|notification| notification["method"] == "session/update")
        .map(|notification| &notification["params"]["update"])
        .collect::<Vec<_>>();

    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "tool_call"
                && update["toolCallId"] == "shell-serialized"
                && update["title"] == "echo hi"
                && update["kind"] == "execute"
        }),
        "session updates: {session_updates:#?}"
    );
    assert!(
        session_updates.iter().any(|update| {
            update["sessionUpdate"] == "tool_call_update"
                && update["toolCallId"] == "shell-serialized"
                && update["status"] == "completed"
        }),
        "session updates: {session_updates:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn serialized_close_interrupts_active_prompt_before_unsubscribe() -> anyhow::Result<()> {
    let (close, messages) = run_serialized_close_during_prompt().await?;

    assert!(close.get("error").is_none(), "close response: {close:#?}");
    assert!(
        messages.iter().any(|message| {
            message.get("id").and_then(Value::as_u64) == Some(3)
                && message["result"]["stopReason"] == "cancelled"
        }),
        "session/prompt response should be cancelled before close completes; messages: {messages:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn serialized_backend_commands_publish_catalog_messages() -> anyhow::Result<()> {
    for (command, expected_fragments) in [
        ("/apps", &["Apps: 1 entries", "- GitHub"][..]),
        (
            "/plugins",
            &["Plugins: 1 entries", "Installed plugins: 1 entries"],
        ),
        ("/mcp", &["MCP: 1 entries", "- filesystem"]),
        ("/hooks", &["Hooks: 1 entries", "- /repo"]),
        ("/ps", &["Background terminals: 1 entries", "terminal-1"]),
        (
            "/stop",
            &["Background terminals cleaned: 1 entries", "terminal-1"],
        ),
        (
            "/kill 42",
            &["Terminated background terminal process `42`."],
        ),
        ("/plan", &["Plan mode enabled for subsequent Codex turns."]),
        (
            "/rollback 2",
            &[
                "Rolled back the last 2 turns.",
                "The thread now contains 1 turn(s).",
                "Local file changes made by rolled-back turns were not reverted.",
            ],
        ),
    ] {
        let (prompt, notifications) = run_serialized_prompt(command).await?;

        assert_eq!(prompt["result"]["stopReason"], "end_turn", "{command}");
        let message = agent_message_texts(&notifications).join("\n");
        for expected in expected_fragments {
            assert!(
                message.contains(expected),
                "command {command} did not publish {expected:?}; message: {message:?}"
            );
        }
        if command == "/plan" {
            assert!(
                notifications.iter().any(plan_mode_config_update),
                "command {command} did not publish plan config update; notifications: {notifications:#?}"
            );
        }
        if command == "/apps" {
            assert!(
                notifications
                    .iter()
                    .any(|notification| adapter_meta_bool_update(notification, "deleted", true)),
                "command {command} did not publish deleted metadata update; notifications: {notifications:#?}"
            );
            assert!(
                notifications
                    .iter()
                    .any(|notification| adapter_meta_bool_update(notification, "closed", true)),
                "command {command} did not publish closed metadata update; notifications: {notifications:#?}"
            );
        }
    }

    Ok(())
}

fn adapter_meta_bool_update(notification: &Value, key: &str, value: bool) -> bool {
    notification["method"] == "session/update"
        && notification["params"]["update"]["sessionUpdate"] == "session_info_update"
        && notification["params"]["update"]["_meta"]["brokk_codex_acp"][key] == value
}

fn plan_mode_config_update(notification: &Value) -> bool {
    notification["method"] == "session/update"
        && notification["params"]["update"]["sessionUpdate"] == "config_option_update"
        && notification["params"]["update"]
            .pointer("/configOptions")
            .and_then(Value::as_array)
            .is_some_and(|options| {
                options.iter().any(|option| {
                    option["id"] == "collaboration_mode" && option["currentValue"] == "plan"
                })
            })
}

fn agent_message_texts(notifications: &[Value]) -> Vec<String> {
    notifications
        .iter()
        .filter(|notification| notification["method"] == "session/update")
        .filter_map(|notification| {
            let update = &notification["params"]["update"];
            (update["sessionUpdate"] == "agent_message_chunk")
                .then(|| update["content"]["text"].as_str())
                .flatten()
                .map(ToOwned::to_owned)
        })
        .collect()
}

async fn run_serialized_prompt(prompt_text: &str) -> anyhow::Result<(Value, Vec<Value>)> {
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
                        "text": prompt_text,
                    },
                ],
            },
        }),
    )
    .await?;
    let prompt = read_response(&mut client_read, 3, &mut notifications).await?;

    drop(client_write);
    agent_task.abort();
    Ok((prompt, notifications))
}

async fn run_serialized_load(session_id: &str) -> anyhow::Result<(Value, Vec<Value>)> {
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
            "method": "session/load",
            "params": {
                "sessionId": session_id,
                "cwd": cwd.path(),
                "mcpServers": [],
            },
        }),
    )
    .await?;
    let load = read_response(&mut client_read, 2, &mut notifications).await?;

    drop(client_write);
    agent_task.abort();
    Ok((load, notifications))
}

async fn run_serialized_close_during_prompt() -> anyhow::Result<(Value, Vec<Value>)> {
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
    let mut messages = Vec::new();

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
    let initialize = read_response(&mut client_read, 1, &mut messages).await?;
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
    let session = read_response(&mut client_read, 2, &mut messages).await?;
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
                        "text": "close me",
                    },
                ],
            },
        }),
    )
    .await?;

    let turn_started = read_json(&mut client_read).await?;
    assert!(
        turn_started["method"] == "session/update"
            && turn_started["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            && turn_started["params"]["update"]["content"]["text"] == "prompt started",
        "unexpected turn-start marker: {turn_started:#?}"
    );
    messages.push(turn_started);

    write_json(
        &mut client_write,
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "session/close",
            "params": {
                "sessionId": session_id,
            },
        }),
    )
    .await?;
    let close = read_response(&mut client_read, 4, &mut messages).await?;

    drop(client_write);
    agent_task.abort();
    Ok((close, messages))
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
interrupted_close_turn = False


def response(message_id, payload):
    print(json.dumps({"id": message_id, **payload}), flush=True)


def send(payload):
    print(json.dumps(payload), flush=True)


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
    elif method == "thread/resume":
        assert params["threadId"] == "thread-serialized"
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
    elif method == "thread/turns/list":
        assert params["threadId"] == "thread-serialized"
        assert params.get("limit") == 50
        assert params.get("sortDirection") == "asc"
        assert params.get("itemsView") == "full"
        cursor = params.get("cursor")
        if cursor is None:
            response(message_id, {
                "result": {
                    "data": [
                        {
                            "id": "load-turn-1",
                            "items": [
                                {
                                    "type": "userMessage",
                                    "content": [
                                        {"type": "text", "text": "loaded hello"},
                                    ],
                                },
                            ],
                        },
                    ],
                    "nextCursor": "load-cursor-2",
                    "backwardsCursor": None,
                },
            })
        else:
            assert cursor == "load-cursor-2"
            response(message_id, {
                "result": {
                    "data": [
                        {
                            "id": "load-turn-2",
                            "items": [
                                {
                                    "type": "agentMessage",
                                    "text": "loaded response",
                                },
                            ],
                        },
                    ],
                    "nextCursor": None,
                    "backwardsCursor": None,
                },
            })
    elif method == "thread/read":
        response(message_id, {
            "error": {
                "code": -32000,
                "message": "thread/read should not be called when paginated turns are available",
            },
        })
    elif method == "app/list":
        assert params == {}
        send({
            "method": "thread/deleted",
            "params": {"threadId": "thread-serialized"},
        })
        send({
            "method": "thread/closed",
            "params": {"threadId": "thread-serialized"},
        })
        response(message_id, {
            "result": {
                "data": [
                    {
                        "displayName": "GitHub",
                        "connectorId": "github",
                        "isAccessible": True,
                    },
                ],
            },
        })
    elif method == "plugin/list":
        assert params == {}
        response(message_id, {
            "result": {
                "data": [
                    {
                        "name": "github",
                        "marketplaceName": "openai",
                        "availability": "AVAILABLE",
                    },
                ],
            },
        })
    elif method == "plugin/installed":
        assert params == {}
        response(message_id, {
            "result": {
                "data": [
                    {
                        "pluginId": "github@openai",
                        "name": "github",
                    },
                ],
            },
        })
    elif method == "mcpServerStatus/list":
        assert params == {
            "threadId": "thread-serialized",
            "detail": "full",
        }
        response(message_id, {
            "result": {
                "data": [
                    {
                        "serverName": "filesystem",
                        "status": "running",
                        "tools": [
                            {"name": "read_file"},
                        ],
                    },
                ],
            },
        })
    elif method == "hooks/list":
        assert params == {"cwds": [thread_cwd]}
        response(message_id, {
            "result": {
                "data": [
                    {
                        "cwd": "/repo",
                        "hooks": [
                            {"name": "SessionStart"},
                        ],
                    },
                ],
            },
        })
    elif method == "thread/backgroundTerminals/list":
        assert params == {"threadId": "thread-serialized"}
        response(message_id, {
            "result": {
                "data": [
                    {
                        "terminalId": "terminal-1",
                        "command": "cargo test",
                        "status": "running",
                    },
                ],
            },
        })
    elif method == "thread/backgroundTerminals/clean":
        assert params == {"threadId": "thread-serialized"}
        response(message_id, {
            "result": {
                "data": [
                    {
                        "terminalId": "terminal-1",
                        "status": "cleaned",
                    },
                ],
            },
        })
    elif method == "thread/backgroundTerminals/terminate":
        assert params == {
            "threadId": "thread-serialized",
            "processId": "42",
        }
        response(message_id, {
            "result": {
                "terminated": True,
            },
        })
    elif method == "thread/rollback":
        assert params == {
            "threadId": "thread-serialized",
            "numTurns": 2,
        }
        response(message_id, {
            "result": {
                "thread": {
                    "id": "thread-serialized",
                    "cwd": thread_cwd,
                    "turns": [
                        {
                            "id": "rollback-turn-1",
                            "items": [],
                        },
                    ],
                },
            },
        })
    elif method == "model/list":
        response(message_id, {"result": {"data": []}})
    elif method == "collaborationMode/list":
        response(message_id, {
            "result": {
                "data": [
                    {
                        "name": "Plan",
                        "mode": "plan",
                        "model": "gpt-5-codex",
                        "reasoning_effort": "medium",
                    },
                ],
            },
        })
    elif method == "permissionProfile/list":
        response(message_id, {"result": {"data": []}})
    elif method == "thread/settings/update":
        assert params == {
            "threadId": "thread-serialized",
            "collaborationMode": {
                "mode": "plan",
                "settings": {
                    "model": "gpt-5-codex",
                    "reasoning_effort": "medium",
                    "developer_instructions": None,
                },
            },
        }
        response(message_id, {"result": {}})
    elif method == "skills/list":
        response(message_id, {"result": {"data": [{"cwd": thread_cwd, "skills": []}]}})
    elif method == "thread/name/set":
        assert params["threadId"] == "thread-serialized"
        assert params["name"] == "Serialized Rename"
        response(message_id, {"result": {}})
    elif method == "thread/archive":
        assert params["threadId"] == "thread-serialized"
        response(message_id, {"result": {}})
    elif method == "thread/unarchive":
        assert params["threadId"] == "thread-serialized"
        response(message_id, {
            "result": {
                "thread": {
                    "id": "thread-serialized",
                    "cwd": thread_cwd,
                    "turns": [],
                },
            },
        })
    elif method == "turn/interrupt":
        assert params == {
            "threadId": "thread-serialized",
            "turnId": "turn-close-serialized",
        }
        interrupted_close_turn = True
        response(message_id, {"result": {}})
        send({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-serialized",
                "turn": {"id": "turn-close-serialized", "status": "cancelled"},
            },
        })
    elif method == "thread/unsubscribe":
        assert params == {"threadId": "thread-serialized"}
        assert interrupted_close_turn
        response(message_id, {"result": {"status": "ok"}})
    elif method == "thread/goal/set":
        assert params["threadId"] == "thread-serialized"
        assert params["objective"] == "Improve serialized coverage"
        response(message_id, {
            "result": {
                "goal": {
                    "objective": "Improve serialized coverage",
                    "status": "active",
                },
            },
        })
    elif method == "thread/shellCommand":
        assert params["threadId"] == "thread-serialized"
        assert params["command"] == "echo hi"
        response(message_id, {"result": {}})
        send({
            "method": "turn/started",
            "params": {
                "threadId": "thread-serialized",
                "turn": {"id": "turn-shell-serialized", "status": "running"},
            },
        })
        send({
            "method": "item/started",
            "params": {
                "threadId": "thread-serialized",
                "turnId": "turn-shell-serialized",
                "item": {
                    "type": "commandExecution",
                    "id": "shell-serialized",
                    "command": "echo hi",
                    "cwd": "/repo",
                    "status": "inProgress",
                },
            },
        })
        send({
            "method": "item/commandExecution/outputDelta",
            "params": {
                "threadId": "thread-serialized",
                "turnId": "turn-shell-serialized",
                "itemId": "shell-serialized",
                "delta": "hi\n",
            },
        })
        send({
            "method": "item/completed",
            "params": {
                "threadId": "thread-serialized",
                "turnId": "turn-shell-serialized",
                "item": {
                    "type": "commandExecution",
                    "id": "shell-serialized",
                    "command": "echo hi",
                    "status": "completed",
                    "aggregatedOutput": "hi\n",
                },
            },
        })
        send({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-serialized",
                "turn": {"id": "turn-shell-serialized", "status": "completed"},
            },
        })
    elif method == "turn/start":
        assert params["threadId"] == "thread-serialized"
        if params["input"] == [{"type": "text", "text": "serialized notifications"}]:
            response(message_id, {
                "result": {
                    "turn": {"id": "turn-serialized-notifications", "status": "running"},
                },
            })
            send({
                "method": "item/reasoning/summaryTextDelta",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "itemId": "reasoning-1",
                    "delta": "thinking",
                },
            })
            send({
                "method": "turn/plan/updated",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "plan": [
                        {"step": "Run serialized test", "status": "inProgress"},
                    ],
                },
            })
            send({
                "method": "item/started",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "item": {
                        "type": "commandExecution",
                        "id": "cmd-1",
                        "command": "cargo test",
                        "cwd": "/repo",
                        "status": "inProgress",
                    },
                },
            })
            send({
                "method": "item/commandExecution/outputDelta",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "itemId": "cmd-1",
                    "delta": "ok",
                },
            })
            send({
                "method": "item/completed",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "item": {
                        "type": "commandExecution",
                        "id": "cmd-1",
                        "command": "cargo test",
                        "status": "completed",
                        "aggregatedOutput": "ok",
                    },
                },
            })
            send({
                "method": "item/started",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "item": {
                        "type": "mcpToolCall",
                        "id": "mcp-1",
                        "server": "filesystem",
                        "tool": "read_file",
                        "status": "inProgress",
                    },
                },
            })
            send({
                "method": "item/completed",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "item": {
                        "type": "mcpToolCall",
                        "id": "mcp-1",
                        "server": "filesystem",
                        "tool": "read_file",
                        "status": "completed",
                        "result": {"content": "read"},
                    },
                },
            })
            send({
                "method": "item/started",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "item": {
                        "type": "fileChange",
                        "id": "file-1",
                        "status": "inProgress",
                        "changes": [
                            {
                                "path": "src/lib.rs",
                                "kind": "update",
                                "diff": "@@ -1 +1 @@",
                            },
                        ],
                    },
                },
            })
            send({
                "method": "item/completed",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "item": {
                        "type": "fileChange",
                        "id": "file-1",
                        "status": "completed",
                        "changes": [
                            {
                                "path": "src/lib.rs",
                                "kind": "update",
                                "diff": "@@ -1 +1 @@",
                            },
                        ],
                    },
                },
            })
            send({
                "method": "turn/diff/updated",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "diff": "diff --git a/src/lib.rs b/src/lib.rs",
                },
            })
            send({
                "method": "thread/tokenUsage/updated",
                "params": {
                    "threadId": "thread-serialized",
                    "used": 42,
                    "size": 100,
                },
            })
            send({
                "method": "warning",
                "params": {
                    "threadId": "thread-serialized",
                    "message": "limited skills loaded",
                },
            })
            send({
                "method": "model/rerouted",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "fromModel": "gpt-5-codex",
                    "toModel": "gpt-5",
                    "reason": "highRiskCyberActivity",
                },
            })
            send({
                "method": "error",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "willRetry": True,
                    "error": {
                        "message": "transient failure",
                        "codexErrorInfo": "rateLimit",
                        "additionalDetails": "retry details",
                    },
                },
            })
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-serialized-notifications",
                    "itemId": "item-1",
                    "delta": "serialized response",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-serialized",
                    "turn": {"id": "turn-serialized-notifications", "status": "completed"},
                },
            })
        elif params["input"] == [{"type": "text", "text": "close me"}]:
            response(message_id, {
                "result": {
                    "turn": {"id": "turn-close-serialized", "status": "running"},
                },
            })
            send({
                "method": "turn/started",
                "params": {
                    "threadId": "thread-serialized",
                    "turn": {"id": "turn-close-serialized", "status": "running"},
                },
            })
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-serialized",
                    "turnId": "turn-close-serialized",
                    "itemId": "item-close",
                    "delta": "prompt started",
                },
            })
        else:
            response(message_id, {
                "error": {
                    "code": -32000,
                    "message": "turn/start should not be called for built-in slash commands",
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
