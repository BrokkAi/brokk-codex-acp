use std::fs;

use brokk_codex_acp::app_server::{
    AppServerApprovalDecision, AppServerClient, AppServerCollaborationMode, AppServerCommand,
    AppServerHistoryEvent, AppServerMessage, AppServerPromptCompletion, AppServerPromptEvent,
    AppServerRealtimeUpdate, AppServerTurnHistory, AppServerTurnInput, ThreadSettingsUpdateParams,
    history_events, history_events_for_turns, is_app_server_method_unavailable,
    is_app_server_overloaded,
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

    let mut app_server_messages = client.subscribe();
    let started = client
        .thread_start(
            "/repo".to_string(),
            Some(vec![
                std::path::PathBuf::from("/repo"),
                std::path::PathBuf::from("/shared"),
            ]),
        )
        .await?;
    assert_eq!(started.thread.id, "thread-1");
    assert_eq!(
        started.thread.cwd.as_deref(),
        Some(std::path::Path::new("/repo"))
    );
    assert_eq!(
        started.runtime_workspace_roots,
        vec![
            std::path::PathBuf::from("/repo"),
            std::path::PathBuf::from("/shared")
        ]
    );
    let message = time::timeout(time::Duration::from_secs(1), app_server_messages.recv()).await??;
    assert!(matches!(
        message,
        AppServerMessage::Notification { ref method, .. } if method == "skills/changed"
    ));

    let forked = client
        .thread_fork(
            "thread-1".to_string(),
            "/repo-fork".to_string(),
            Some(vec![
                std::path::PathBuf::from("/repo-fork"),
                std::path::PathBuf::from("/shared-fork"),
            ]),
        )
        .await?;
    assert_eq!(forked.thread.id, "thread-2");
    let fork_events = history_events_for_thread_turns(&forked.thread.turns);
    assert_eq!(fork_events, vec!["user:forked hello"]);

    let ephemeral_fork = client
        .thread_fork_with_options(
            "thread-1".to_string(),
            "/repo-side".to_string(),
            Some(vec![std::path::PathBuf::from("/repo-side")]),
            true,
            true,
        )
        .await?;
    assert_eq!(ephemeral_fork.thread.id, "thread-side");
    assert!(ephemeral_fork.thread.turns.is_empty());

    let resumed = client
        .thread_resume(
            "thread-1".to_string(),
            "/repo".to_string(),
            Some(vec![
                std::path::PathBuf::from("/repo"),
                std::path::PathBuf::from("/shared"),
            ]),
        )
        .await?;
    assert_eq!(resumed.thread.id, "thread-1");
    assert_eq!(resumed.model.as_deref(), Some("gpt-5-codex"));

    let listed = client.thread_list(Some("/repo".to_string()), None).await?;
    assert_eq!(listed.data.len(), 1);
    assert_eq!(listed.data[0].id, "thread-1");
    assert_eq!(listed.data[0].preview.as_deref(), Some("Started preview"));
    assert_eq!(
        listed.data[0]
            .status
            .as_ref()
            .and_then(|status| status["type"].as_str()),
        Some("notLoaded")
    );
    assert_eq!(listed.data[0].model_provider.as_deref(), Some("openai"));
    assert_eq!(listed.data[0].updated_at, Some(1700000100));
    assert_eq!(
        listed.data[0].parent_thread_id.as_deref(),
        Some("thread-parent")
    );

    client
        .thread_name_set("thread-1".to_string(), "Renamed Thread".to_string())
        .await?;
    let message = time::timeout(time::Duration::from_secs(1), app_server_messages.recv()).await??;
    assert!(matches!(
        message,
        AppServerMessage::Notification { ref method, ref params }
            if method == "thread/name/updated"
                && params["threadId"] == "thread-1"
                && params["threadName"] == "Renamed Thread"
    ));

    client.thread_archive("thread-1".to_string()).await?;
    let message = time::timeout(time::Duration::from_secs(1), app_server_messages.recv()).await??;
    assert!(matches!(
        message,
        AppServerMessage::Notification { ref method, ref params }
            if method == "thread/archived"
                && params["threadId"] == "thread-1"
    ));
    let message = time::timeout(time::Duration::from_secs(1), app_server_messages.recv()).await??;
    assert!(matches!(
        message,
        AppServerMessage::Notification { ref method, ref params }
            if method == "thread/status/changed"
                && params["threadId"] == "thread-1"
                && params["status"]["type"] == "notLoaded"
    ));

    let unarchived = client.thread_unarchive("thread-1".to_string()).await?;
    assert_eq!(unarchived.thread.id, "thread-1");
    let message = time::timeout(time::Duration::from_secs(1), app_server_messages.recv()).await??;
    assert!(matches!(
        message,
        AppServerMessage::Notification { ref method, ref params }
            if method == "thread/unarchived"
                && params["threadId"] == "thread-1"
    ));

    let goal = client
        .thread_goal_set(
            "thread-1".to_string(),
            Some("Improve ACP coverage".to_string()),
            None,
            Some(Some(200_000)),
        )
        .await?;
    assert_eq!(goal.goal["objective"], "Improve ACP coverage");
    let message = time::timeout(time::Duration::from_secs(1), app_server_messages.recv()).await??;
    assert!(matches!(
        message,
        AppServerMessage::Notification { ref method, ref params }
            if method == "thread/goal/updated"
                && params["threadId"] == "thread-1"
                && params["goal"]["tokenBudget"] == 200_000
    ));

    let goal = client.thread_goal_get("thread-1".to_string()).await?;
    assert_eq!(
        goal.goal.as_ref().and_then(|goal| goal["status"].as_str()),
        Some("active")
    );

    let cleared = client.thread_goal_clear("thread-1".to_string()).await?;
    assert!(cleared.cleared);
    let message = time::timeout(time::Duration::from_secs(1), app_server_messages.recv()).await??;
    assert!(matches!(
        message,
        AppServerMessage::Notification { ref method, ref params }
            if method == "thread/goal/cleared"
                && params["threadId"] == "thread-1"
    ));

    let skills = client.skills_list("/repo".to_string(), false).await?;
    assert_eq!(skills.data.len(), 1);
    assert_eq!(skills.data[0].cwd, "/repo");
    assert_eq!(skills.data[0].skills.len(), 2);
    assert_eq!(skills.data[0].skills[0].name, "skill-creator");
    assert_eq!(
        skills.data[0].skills[0].path.as_deref(),
        Some("/skills/skill-creator/SKILL.md")
    );
    assert_eq!(
        skills.data[0].skills[0].description.as_deref(),
        Some("Create skills")
    );
    assert!(skills.data[0].skills[0].enabled);
    assert_eq!(
        skills.data[0].skills[0]
            .interface
            .as_ref()
            .and_then(|interface| interface.short_description.as_deref()),
        Some("Create or update Codex skills")
    );
    assert_eq!(skills.data[0].skills[1].name, "disabled-skill");
    assert!(!skills.data[0].skills[1].enabled);

    client
        .skills_config_write(Some("skill-creator".to_string()), None, false)
        .await?;
    let skills = client.skills_list("/repo".to_string(), true).await?;
    assert!(!skills.data[0].skills[0].enabled);

    client
        .skills_extra_roots_set(vec![
            "/repo/.codex/skills".to_string(),
            "/shared/codex-skills".to_string(),
        ])
        .await?;

    let models = client.model_list().await?;
    assert_eq!(models.data.len(), 2);
    assert_eq!(models.data[0].id, "gpt-5");
    assert!(models.data[0].is_default);

    let collaboration_modes = client.collaboration_mode_list().await?;
    assert_eq!(collaboration_modes.data.len(), 2);
    assert_eq!(collaboration_modes.data[0].mode.as_deref(), Some("default"));
    assert_eq!(
        collaboration_modes.data[1].reasoning_effort.as_ref(),
        Some(&Some("medium".to_string()))
    );

    let permission_profiles = client
        .permission_profile_list(Some("/repo".to_string()))
        .await?;
    assert_eq!(permission_profiles.data.len(), 3);
    assert_eq!(permission_profiles.data[1].id, ":workspace");

    client
        .thread_settings_update(
            ThreadSettingsUpdateParams::new("thread-1".to_string())
                .with_model("gpt-5-codex".to_string()),
        )
        .await?;
    client
        .thread_settings_update(
            ThreadSettingsUpdateParams::new("thread-1".to_string())
                .with_permissions(":danger-full-access".to_string()),
        )
        .await?;
    client
        .thread_settings_update(
            ThreadSettingsUpdateParams::new("thread-1".to_string()).with_effort("high".to_string()),
        )
        .await?;
    client
        .thread_settings_update(
            ThreadSettingsUpdateParams::new("thread-1".to_string())
                .with_service_tier(Some("priority".to_string())),
        )
        .await?;
    client
        .thread_settings_update(
            ThreadSettingsUpdateParams::new("thread-1".to_string()).with_service_tier(None),
        )
        .await?;
    client
        .thread_settings_update(
            ThreadSettingsUpdateParams::new("thread-1".to_string())
                .with_approval_policy("never".to_string()),
        )
        .await?;
    client
        .thread_settings_update(
            ThreadSettingsUpdateParams::new("thread-1".to_string()).with_collaboration_mode(
                AppServerCollaborationMode::new(
                    "plan".to_string(),
                    "gpt-5-codex".to_string(),
                    Some("medium".to_string()),
                    None,
                ),
            ),
        )
        .await?;

    let mut skill_deltas = Vec::new();
    client
        .turn_start_until_complete(
            "thread-1".to_string(),
            vec![
                AppServerTurnInput::Text {
                    text: "$skill-creator Make one".to_string(),
                },
                AppServerTurnInput::Skill {
                    name: "skill-creator".to_string(),
                    path: "/skills/skill-creator/SKILL.md".to_string(),
                },
            ],
            None,
            |event| {
                if let AppServerPromptEvent::AgentMessageDelta(delta) = event {
                    skill_deltas.push(delta);
                }
                Ok(())
            },
            |_approval| async { Ok(AppServerApprovalDecision::Cancel) },
        )
        .await?;
    assert_eq!(skill_deltas, vec!["skill response"]);

    let read = client.thread_read("thread-1".to_string()).await?;
    assert_eq!(read.thread.id, "thread-1");
    let history_events = history_events(&read.thread)
        .into_iter()
        .map(summarize_history_event)
        .collect::<Vec<_>>();
    assert_eq!(
        history_events,
        vec![
            "user:hello",
            "thought:remembered reasoning",
            "tool-started:cmd-history:cargo check",
            "tool-updated:cmd-history",
            "tool-started:file-history:Edit 1 file",
            "tool-updated:file-history",
            "diff:### src/lib.rs\n@@ -1 +1 @@",
            "message:remembered response",
        ]
    );

    let rollback = client.thread_rollback("thread-1".to_string(), 2).await?;
    assert_eq!(rollback.thread.id, "thread-1");
    assert_eq!(rollback.thread.turns.len(), 1);

    let turns_page = client
        .thread_turns_list("thread-1".to_string(), None, 50)
        .await?;
    assert_eq!(turns_page.data.len(), 1);
    assert_eq!(turns_page.next_cursor.as_deref(), Some("older-turns"));
    let turns_page = client
        .thread_turns_list("thread-1".to_string(), turns_page.next_cursor, 50)
        .await?;
    assert_eq!(turns_page.data[0].id, "turn-history-older");
    assert!(turns_page.next_cursor.is_none());

    let mut events = Vec::new();
    client
        .turn_start_text_until_complete(
            "thread-1".to_string(),
            "hello".to_string(),
            None,
            |event| {
                match event {
                    AppServerPromptEvent::AgentMessageDelta(delta) => {
                        events.push(format!("message:{delta}"));
                    }
                    AppServerPromptEvent::AgentThoughtDelta(delta) => {
                        events.push(format!("thought:{delta}"));
                    }
                    AppServerPromptEvent::ToolCallStarted(call) => {
                        events.push(format!("tool-started:{}:{}", call.id, call.title));
                    }
                    AppServerPromptEvent::ToolCallUpdated(update) => {
                        events.push(format!("tool-updated:{}", update.id));
                    }
                    AppServerPromptEvent::PlanUpdated(entries) => {
                        events.push(format!("plan:{}", entries.len()));
                    }
                    AppServerPromptEvent::TurnDiffUpdated { diff, .. } => {
                        events.push(format!("diff:{diff}"));
                    }
                    AppServerPromptEvent::UsageUpdated(usage) => {
                        events.push(format!("usage:{}/{}", usage.used, usage.size));
                    }
                    AppServerPromptEvent::SkillsChanged => {
                        events.push("skills-changed".to_string());
                    }
                    AppServerPromptEvent::ThreadSettingsUpdated(settings) => {
                        events.push(format!(
                            "settings:{}:{}:{}:{}",
                            settings.thread_id,
                            settings.model.as_deref().unwrap_or_default(),
                            settings.reasoning_effort.as_deref().unwrap_or_default(),
                            settings.service_tier.as_deref().unwrap_or_default()
                        ));
                    }
                    AppServerPromptEvent::Warning(update) => {
                        events.push(format!("warning:{}", update.message));
                    }
                    AppServerPromptEvent::Error(update) => {
                        events.push(format!("error:{}", update.message));
                    }
                    AppServerPromptEvent::ModelRerouted(update) => {
                        events.push(format!(
                            "model-rerouted:{}:{}",
                            update.from_model, update.to_model
                        ));
                    }
                    AppServerPromptEvent::ModelVerification(update) => {
                        events.push(format!("model-verification:{}", update.verifications));
                    }
                    AppServerPromptEvent::TurnModerationMetadata(update) => {
                        events.push(format!("moderation:{}", update.metadata));
                    }
                    AppServerPromptEvent::McpServerStartupStatus(update) => {
                        events.push(format!("mcp-startup:{}:{}", update.name, update.status));
                    }
                    AppServerPromptEvent::Realtime(update) => {
                        events.push(realtime_summary(&update));
                    }
                    AppServerPromptEvent::ConfigWarning(update) => {
                        events.push(format!("config-warning:{}", update.summary));
                    }
                    AppServerPromptEvent::WindowsSandboxSetup(update) => {
                        events.push(format!(
                            "windows-sandbox:{}:{}",
                            update.mode, update.success
                        ));
                    }
                }
                Ok(())
            },
        )
        .await?;
    assert_eq!(
        events,
        vec![
            "thought:thinking",
            "plan:1",
            "tool-started:cmd-1:cargo test",
            "tool-updated:cmd-1",
            "tool-updated:cmd-1",
            "diff:diff --git a/src/lib.rs b/src/lib.rs",
            "usage:42/100",
            "skills-changed",
            "settings:thread-1:gpt-5-codex:high:priority",
            "realtime-started:session-1",
            "realtime-sdp:10",
            "realtime-item:{\"kind\":\"handoff_request\",\"target\":\"browser\"}",
            "realtime-delta:assistant:live",
            "realtime-done:assistant:live final",
            "realtime-audio:8:24000:1:320",
            "realtime-error:network interrupted",
            "realtime-closed:client stopped",
            "message:fake response",
        ]
    );

    let mut shell_events = Vec::new();
    client
        .thread_shell_command_until_complete(
            "thread-1".to_string(),
            "echo hi".to_string(),
            None,
            |event| {
                shell_events.push(summarize_prompt_event(event));
                Ok(())
            },
            |_approval| async { Ok(AppServerApprovalDecision::Cancel) },
        )
        .await?;
    assert_eq!(
        shell_events,
        vec![
            "tool-started:shell-1:echo hi",
            "tool-updated:shell-1",
            "tool-updated:shell-1"
        ]
    );

    let mut approval_summaries = Vec::new();
    client
        .turn_start_until_complete(
            "thread-1".to_string(),
            vec![AppServerTurnInput::Text {
                text: "approval callback".to_string(),
            }],
            None,
            |event| {
                if let AppServerPromptEvent::AgentMessageDelta(delta) = event {
                    approval_summaries.push(format!("message:{delta}"));
                }
                Ok(())
            },
            |approval| async move {
                assert_eq!(approval.item_id, "cmd-approval");
                assert_eq!(approval.title, "Run `cargo fmt`");
                assert!(matches!(
                    approval.kind,
                    brokk_codex_acp::app_server::AppServerToolKind::Execute
                ));
                assert_eq!(
                    approval
                        .options
                        .iter()
                        .map(|option| option.id())
                        .collect::<Vec<_>>(),
                    vec!["accept", "decline"]
                );
                assert!(approval.raw.get("command").is_some());
                Ok(AppServerApprovalDecision::Accept)
            },
        )
        .await?;
    assert_eq!(approval_summaries, vec!["message:approved callback"]);

    let mut file_approval_summaries = Vec::new();
    client
        .turn_start_until_complete(
            "thread-1".to_string(),
            vec![AppServerTurnInput::Text {
                text: "file approval callback".to_string(),
            }],
            None,
            |event| {
                if let AppServerPromptEvent::AgentMessageDelta(delta) = event {
                    file_approval_summaries.push(format!("message:{delta}"));
                }
                Ok(())
            },
            |approval| async move {
                assert_eq!(approval.item_id, "file-approval");
                assert_eq!(approval.title, "Apply file changes");
                assert!(matches!(
                    approval.kind,
                    brokk_codex_acp::app_server::AppServerToolKind::Edit
                ));
                assert_eq!(
                    approval
                        .options
                        .iter()
                        .map(|option| option.id())
                        .collect::<Vec<_>>(),
                    vec!["accept", "acceptForSession", "decline", "cancel"]
                );
                Ok(AppServerApprovalDecision::AcceptForSession)
            },
        )
        .await?;
    assert_eq!(file_approval_summaries, vec!["message:approved file"]);

    let mut permissions_approval_summaries = Vec::new();
    client
        .turn_start_until_complete(
            "thread-1".to_string(),
            vec![AppServerTurnInput::Text {
                text: "permissions approval callback".to_string(),
            }],
            None,
            |event| {
                if let AppServerPromptEvent::AgentMessageDelta(delta) = event {
                    permissions_approval_summaries.push(format!("message:{delta}"));
                }
                Ok(())
            },
            |approval| async move {
                assert_eq!(approval.item_id, "permissions-approval");
                assert_eq!(approval.title, "Need network and write access");
                assert!(matches!(
                    approval.kind,
                    brokk_codex_acp::app_server::AppServerToolKind::Other
                ));
                assert_eq!(
                    approval
                        .options
                        .iter()
                        .map(|option| option.id())
                        .collect::<Vec<_>>(),
                    vec!["accept", "acceptForSession", "decline", "cancel"]
                );
                assert_eq!(approval.raw["permissions"]["network"]["enabled"], true);
                Ok(AppServerApprovalDecision::AcceptForSession)
            },
        )
        .await?;
    assert_eq!(
        permissions_approval_summaries,
        vec!["message:approved permissions"]
    );

    let mcp_elicitation_summaries =
        run_text_turn_and_collect_messages(&mut client, "mcp elicitation callback").await?;
    assert_eq!(
        mcp_elicitation_summaries,
        vec!["message:cancelled elicitation"]
    );

    let mut review_events = Vec::new();
    client
        .review_start_until_complete(
            "thread-1".to_string(),
            None,
            |event| {
                review_events.push(summarize_prompt_event(event));
                Ok(())
            },
            |_approval| async { Ok(AppServerApprovalDecision::Cancel) },
        )
        .await?;
    assert_eq!(review_events, vec!["message:review complete"]);

    let mut compact_events = Vec::new();
    client
        .thread_compact_start_until_complete(
            "thread-1".to_string(),
            None,
            |event| {
                compact_events.push(summarize_prompt_event(event));
                Ok(())
            },
            |_approval| async { Ok(AppServerApprovalDecision::Cancel) },
        )
        .await?;
    assert_eq!(
        compact_events,
        vec![
            "tool-started:compact-1:Compacting conversation context",
            "tool-updated:compact-1",
            "message:compact complete"
        ]
    );

    let apps = client.app_list().await?;
    assert_eq!(apps["data"][0]["displayName"], "GitHub");

    let plugins = client.plugin_list().await?;
    assert_eq!(plugins["data"][0]["name"], "github");

    let installed_plugins = client.plugin_installed().await?;
    assert_eq!(installed_plugins["data"][0]["pluginId"], "github@openai");

    let mcp_servers = client
        .mcp_server_status_list("thread-1".to_string())
        .await?;
    assert_eq!(mcp_servers["data"][0]["serverName"], "filesystem");

    let hooks = client.hooks_list("/repo".to_string()).await?;
    assert_eq!(hooks["data"][0]["cwd"], "/repo");

    let loaded_threads = client.thread_loaded_list().await?;
    assert_eq!(loaded_threads["data"][0], "thread-1");

    let background_terminals = client
        .thread_background_terminals_list("thread-1".to_string())
        .await?;
    assert_eq!(background_terminals["data"][0]["command"], "cargo test");

    let cleaned_terminals = client
        .thread_background_terminals_clean("thread-1".to_string())
        .await?;
    assert_eq!(cleaned_terminals["data"][0]["terminalId"], "terminal-1");

    let terminated_terminal = client
        .thread_background_terminals_terminate("thread-1".to_string(), "42".to_string())
        .await?;
    assert_eq!(terminated_terminal["terminated"], true);

    let user_input_summaries =
        run_text_turn_and_collect_messages(&mut client, "user input callback").await?;
    assert_eq!(user_input_summaries, vec!["message:empty user input"]);

    let dynamic_tool_summaries =
        run_text_turn_and_collect_messages(&mut client, "dynamic tool callback").await?;
    assert_eq!(dynamic_tool_summaries, vec!["message:failed dynamic tool"]);

    let current_time_summaries =
        run_text_turn_and_collect_messages(&mut client, "current time callback").await?;
    assert_eq!(
        current_time_summaries,
        vec!["message:reported current time"]
    );

    let unsupported_request_summaries =
        run_text_turn_and_collect_messages(&mut client, "unsupported server request").await?;
    assert_eq!(
        unsupported_request_summaries,
        vec!["message:rejected unsupported server request"]
    );

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

    let mut delete_notifications = client.subscribe();
    client.thread_delete("thread-1".to_string()).await?;
    let mut saw_deleted = false;
    let mut saw_closed = false;
    for _ in 0..10 {
        let message =
            time::timeout(time::Duration::from_secs(1), delete_notifications.recv()).await??;
        match message {
            AppServerMessage::Notification { method, params }
                if method == "thread/deleted" && params["threadId"] == "thread-1" =>
            {
                saw_deleted = true;
            }
            AppServerMessage::Notification { method, params }
                if method == "thread/closed" && params["threadId"] == "thread-1" =>
            {
                saw_closed = true;
            }
            _ => {}
        }
        if saw_deleted && saw_closed {
            break;
        }
    }
    assert!(saw_deleted, "thread/delete should emit thread/deleted");
    assert!(saw_closed, "thread/delete should emit thread/closed");

    Ok(())
}

#[tokio::test]
async fn app_server_client_maps_error_responses() -> anyhow::Result<()> {
    let fake_codex = fake_codex_app_server_with_script(ERROR_CODEX_APP_SERVER)?;
    let mut client =
        AppServerClient::spawn(AppServerCommand::new(fake_codex.path().to_owned())).await?;

    let error = client.app_list().await.unwrap_err();
    let message = error.to_string();
    assert!(message.contains("codex app-server request `app/list` failed"));
    assert!(message.contains("boom"));

    let error = client
        .thread_start("/repo".to_string(), None)
        .await
        .unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("failed to decode app-server `thread/start` response"));

    let error = client.plugin_list().await.unwrap_err();
    assert_eq!(
        is_app_server_method_unavailable(&error),
        Some("plugin/list")
    );

    let models = client.model_list().await?;
    assert_eq!(models.data[0].id, "gpt-5");

    let error = client.thread_loaded_list().await.unwrap_err();
    assert_eq!(is_app_server_overloaded(&error), Some("thread/loaded/list"));

    Ok(())
}

async fn run_text_turn_and_collect_messages(
    client: &mut AppServerClient,
    text: &str,
) -> anyhow::Result<Vec<String>> {
    let mut summaries = Vec::new();
    client
        .turn_start_text_until_complete("thread-1".to_string(), text.to_owned(), None, |event| {
            if let AppServerPromptEvent::AgentMessageDelta(delta) = event {
                summaries.push(format!("message:{delta}"));
            }
            Ok(())
        })
        .await?;
    Ok(summaries)
}

fn summarize_history_event(event: AppServerHistoryEvent) -> String {
    match event {
        AppServerHistoryEvent::UserMessage(text) => format!("user:{text}"),
        AppServerHistoryEvent::PromptEvent(event) => summarize_prompt_event(*event),
    }
}

fn history_events_for_thread_turns(turns: &[AppServerTurnHistory]) -> Vec<String> {
    history_events_for_turns(turns)
        .into_iter()
        .map(summarize_history_event)
        .collect()
}

fn summarize_prompt_event(event: AppServerPromptEvent) -> String {
    match event {
        AppServerPromptEvent::AgentMessageDelta(delta) => format!("message:{delta}"),
        AppServerPromptEvent::AgentThoughtDelta(delta) => format!("thought:{delta}"),
        AppServerPromptEvent::ToolCallStarted(call) => {
            format!("tool-started:{}:{}", call.id, call.title)
        }
        AppServerPromptEvent::ToolCallUpdated(update) => format!("tool-updated:{}", update.id),
        AppServerPromptEvent::PlanUpdated(entries) => format!("plan:{}", entries.len()),
        AppServerPromptEvent::TurnDiffUpdated { diff, .. } => format!("diff:{diff}"),
        AppServerPromptEvent::UsageUpdated(usage) => format!("usage:{}/{}", usage.used, usage.size),
        AppServerPromptEvent::SkillsChanged => "skills-changed".to_owned(),
        AppServerPromptEvent::ThreadSettingsUpdated(settings) => {
            format!("settings:{}", settings.thread_id)
        }
        AppServerPromptEvent::Warning(update) => format!("warning:{}", update.message),
        AppServerPromptEvent::Error(update) => format!("error:{}", update.message),
        AppServerPromptEvent::ModelRerouted(update) => {
            format!("model-rerouted:{}:{}", update.from_model, update.to_model)
        }
        AppServerPromptEvent::ModelVerification(update) => {
            format!("model-verification:{}", update.verifications)
        }
        AppServerPromptEvent::TurnModerationMetadata(update) => {
            format!("moderation:{}", update.metadata)
        }
        AppServerPromptEvent::McpServerStartupStatus(update) => {
            format!("mcp-startup:{}:{}", update.name, update.status)
        }
        AppServerPromptEvent::Realtime(update) => realtime_summary(&update),
        AppServerPromptEvent::ConfigWarning(update) => {
            format!("config-warning:{}", update.summary)
        }
        AppServerPromptEvent::WindowsSandboxSetup(update) => {
            format!("windows-sandbox:{}:{}", update.mode, update.success)
        }
    }
}

fn realtime_summary(update: &AppServerRealtimeUpdate) -> String {
    match update {
        AppServerRealtimeUpdate::Started {
            realtime_session_id,
            ..
        } => format!(
            "realtime-started:{}",
            realtime_session_id.as_deref().unwrap_or_default()
        ),
        AppServerRealtimeUpdate::Sdp { sdp, .. } => {
            format!("realtime-sdp:{}", sdp.len())
        }
        AppServerRealtimeUpdate::ItemAdded { item, .. } => {
            format!("realtime-item:{item}")
        }
        AppServerRealtimeUpdate::TranscriptDelta { role, delta, .. } => {
            format!("realtime-delta:{role}:{delta}")
        }
        AppServerRealtimeUpdate::TranscriptDone { role, text, .. } => {
            format!("realtime-done:{role}:{text}")
        }
        AppServerRealtimeUpdate::OutputAudioDelta { audio, .. } => {
            format!(
                "realtime-audio:{}:{}:{}:{}",
                audio.data.as_deref().unwrap_or_default().len(),
                audio.sample_rate.unwrap_or_default(),
                audio.num_channels.unwrap_or_default(),
                audio.samples_per_channel.unwrap_or_default()
            )
        }
        AppServerRealtimeUpdate::Error { message, .. } => {
            format!("realtime-error:{message}")
        }
        AppServerRealtimeUpdate::Closed { reason, .. } => {
            format!("realtime-closed:{reason}")
        }
    }
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
    fake_codex_app_server_with_script(FAKE_CODEX_APP_SERVER)
}

fn fake_codex_app_server_with_script(script: &str) -> anyhow::Result<FakeCodex> {
    let temp_dir = tempfile::tempdir()?;

    #[cfg(windows)]
    let path = {
        let script_path = temp_dir.path().join("fake_codex_app_server.py");
        fs::write(&script_path, script)?;

        let wrapper_path = temp_dir.path().join("codex.cmd");
        fs::write(
            &wrapper_path,
            "@echo off\r\npython \"%~dp0fake_codex_app_server.py\" %*\r\n",
        )?;
        wrapper_path
    };

    #[cfg(not(windows))]
    let path = {
        let path = temp_dir.path().join("codex");
        fs::write(&path, script)?;
        path
    };

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

const ERROR_CODEX_APP_SERVER: &str = r#"#!/usr/bin/env python3
import json
import sys

model_list_attempts = 0

def response(message_id, payload):
    print(json.dumps({"id": message_id, **payload}), flush=True)

for line in sys.stdin:
    message = json.loads(line)
    message_id = message["id"]
    method = message["method"]
    if method == "app/list":
        response(message_id, {
            "error": {
                "code": -32000,
                "message": "boom",
            },
        })
    elif method == "plugin/list":
        response(message_id, {
            "error": {
                "code": -32601,
                "message": "Method not found",
            },
        })
    elif method == "thread/start":
        response(message_id, {"result": {"thread": {}}})
    elif method == "model/list":
        model_list_attempts += 1
        if model_list_attempts == 1:
            response(message_id, {
                "error": {
                    "code": -32001,
                    "message": "Server overloaded; retry later.",
                },
            })
        else:
            response(message_id, {
                "result": {
                    "data": [{
                        "id": "gpt-5",
                        "displayName": "GPT-5",
                        "description": "Test model",
                    }],
                },
            })
    elif method == "thread/loaded/list":
        response(message_id, {
            "error": {
                "code": -32001,
                "message": "Server overloaded; retry later.",
            },
        })
    else:
        response(message_id, {"result": {}})
"#;

const FAKE_CODEX_APP_SERVER: &str = r#"#!/usr/bin/env python3
import json
import sys

skill_creator_enabled = True


def send(message):
    print(json.dumps(message), flush=True)


def response(message_id, result):
    send({"id": message_id, "result": result})


def goal_payload():
    return {
        "threadId": "thread-1",
        "objective": "Improve ACP coverage",
        "status": "active",
        "tokenBudget": 200000,
        "tokensUsed": 0,
        "timeUsedSeconds": 0,
        "createdAt": 1776272400,
        "updatedAt": 1776272400,
    }


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
        assert params["runtimeWorkspaceRoots"] == ["/repo", "/shared"]
        response(message_id, {
            "thread": {
                "id": "thread-1",
                "cwd": params["cwd"],
                "name": "Started Thread",
            },
            "runtimeWorkspaceRoots": params["runtimeWorkspaceRoots"],
            "model": "gpt-5",
            "reasoningEffort": "medium",
            "serviceTier": "standard",
            "approvalPolicy": "on-request",
            "collaborationMode": {
                "mode": "default",
                "settings": {
                    "model": "gpt-5",
                    "reasoning_effort": "medium",
                    "developer_instructions": None,
                },
            },
            "activePermissionProfile": {"id": ":workspace"},
        })
        send({
            "method": "skills/changed",
            "params": {},
        })
    elif method == "thread/fork":
        assert params["threadId"] == "thread-1"
        if params["cwd"] == "/repo-side":
            assert params["runtimeWorkspaceRoots"] == ["/repo-side"]
            assert params["ephemeral"] is True
            assert params["excludeTurns"] is True
            response(message_id, {
                "thread": {
                    "id": "thread-side",
                    "cwd": params["cwd"],
                    "name": "Ephemeral Fork",
                },
                "runtimeWorkspaceRoots": params["runtimeWorkspaceRoots"],
            })
            continue
        assert params["cwd"] == "/repo-fork"
        assert params["runtimeWorkspaceRoots"] == ["/repo-fork", "/shared-fork"]
        assert "ephemeral" not in params
        assert "excludeTurns" not in params
        response(message_id, {
            "thread": {
                "id": "thread-2",
                "cwd": params["cwd"],
                "name": "Forked Thread",
                "turns": [
                    {
                        "id": "turn-fork-history",
                        "items": [
                            {
                                "type": "userMessage",
                                "content": [
                                    {"type": "text", "text": "forked hello"},
                                ],
                            },
                        ],
                    },
                ],
            },
            "runtimeWorkspaceRoots": params["runtimeWorkspaceRoots"],
            "model": "gpt-5-codex",
            "reasoningEffort": "high",
            "serviceTier": "priority",
            "approvalPolicy": "never",
            "collaborationMode": {
                "mode": "plan",
                "settings": {
                    "model": "gpt-5-codex",
                    "reasoning_effort": "medium",
                    "developer_instructions": None,
                },
            },
            "activePermissionProfile": {"id": ":workspace"},
        })
    elif method == "thread/resume":
        assert params["threadId"] == "thread-1"
        assert params["cwd"] == "/repo"
        assert params["runtimeWorkspaceRoots"] == ["/repo", "/shared"]
        assert params["excludeTurns"] is True
        response(message_id, {
            "thread": {
                "id": "thread-1",
                "cwd": params["cwd"],
                "name": "Started Thread",
            },
            "runtimeWorkspaceRoots": params["runtimeWorkspaceRoots"],
            "model": "gpt-5-codex",
            "reasoningEffort": "high",
            "serviceTier": "priority",
            "approvalPolicy": "never",
            "collaborationMode": {
                "mode": "plan",
                "settings": {
                    "model": "gpt-5-codex",
                    "reasoning_effort": "medium",
                    "developer_instructions": None,
                },
            },
            "activePermissionProfile": {"id": ":workspace"},
        })
    elif method == "thread/list":
        assert params["cwd"] == "/repo"
        response(message_id, {
            "data": [
                {
                    "id": "thread-1",
                    "cwd": "/repo",
                    "name": "Started Thread",
                    "preview": "Started preview",
                    "status": {"type": "notLoaded"},
                    "modelProvider": "openai",
                    "createdAt": 1700000000,
                    "updatedAt": 1700000100,
                    "recencyAt": 1700000200,
                    "agentNickname": "Codex",
                    "agentRole": "primary",
                    "parentThreadId": "thread-parent",
                }
            ],
            "nextCursor": None,
        })
    elif method == "thread/name/set":
        assert params == {
            "threadId": "thread-1",
            "name": "Renamed Thread",
        }
        response(message_id, {})
        send({
            "method": "thread/name/updated",
            "params": {
                "threadId": "thread-1",
                "threadName": params["name"],
            },
        })
    elif method == "thread/archive":
        assert params == {"threadId": "thread-1"}
        response(message_id, {})
        send({
            "method": "thread/archived",
            "params": {"threadId": "thread-1"},
        })
        send({
            "method": "thread/status/changed",
            "params": {
                "threadId": "thread-1",
                "status": {"type": "notLoaded"},
            },
        })
    elif method == "thread/unarchive":
        assert params == {"threadId": "thread-1"}
        response(message_id, {
            "thread": {
                "id": "thread-1",
                "cwd": "/repo",
                "turns": [],
            },
        })
        send({
            "method": "thread/unarchived",
            "params": {"threadId": "thread-1"},
        })
    elif method == "thread/goal/set":
        assert params == {
            "threadId": "thread-1",
            "objective": "Improve ACP coverage",
            "tokenBudget": 200000,
        }
        goal = goal_payload()
        response(message_id, {"goal": goal})
        send({
            "method": "thread/goal/updated",
            "params": {
                "threadId": "thread-1",
                "goal": goal,
            },
        })
    elif method == "thread/goal/get":
        assert params == {"threadId": "thread-1"}
        response(message_id, {"goal": goal_payload()})
    elif method == "thread/goal/clear":
        assert params == {"threadId": "thread-1"}
        response(message_id, {"cleared": True})
        send({
            "method": "thread/goal/cleared",
            "params": {"threadId": "thread-1"},
        })
    elif method == "review/start":
        assert params == {"threadId": "thread-1"}
        response(message_id, {"turn": {"id": "turn-review", "status": "running"}})
        send({
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-review",
                "itemId": "item-review",
                "delta": "review complete",
            },
        })
        send({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-1",
                "turn": {"id": "turn-review", "status": "completed"},
            },
        })
    elif method == "thread/compact/start":
        assert params == {"threadId": "thread-1"}
        response(message_id, {})
        send({
            "method": "turn/started",
            "params": {
                "threadId": "thread-1",
                "turn": {"id": "turn-compact", "status": "running"},
            },
        })
        send({
            "method": "item/started",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-compact",
                "item": {
                    "type": "contextCompaction",
                    "id": "compact-1",
                    "status": "inProgress",
                },
            },
        })
        send({
            "method": "item/completed",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-compact",
                "item": {
                    "type": "contextCompaction",
                    "id": "compact-1",
                    "status": "completed",
                },
            },
        })
        send({
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-compact",
                "itemId": "item-compact",
                "delta": "compact complete",
            },
        })
        send({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-1",
                "turn": {"id": "turn-compact", "status": "completed"},
            },
        })
    elif method == "app/list":
        assert params == {}
        response(message_id, {
            "data": [
                {
                    "displayName": "GitHub",
                    "connectorId": "github",
                    "isAccessible": True,
                },
            ],
        })
    elif method == "plugin/list":
        assert params == {}
        response(message_id, {
            "data": [
                {
                    "name": "github",
                    "marketplaceName": "openai",
                    "availability": "AVAILABLE",
                },
            ],
        })
    elif method == "plugin/installed":
        assert params == {}
        response(message_id, {
            "data": [
                {
                    "pluginId": "github@openai",
                    "name": "github",
                },
            ],
        })
    elif method == "mcpServerStatus/list":
        assert params == {
            "threadId": "thread-1",
            "detail": "full",
        }
        response(message_id, {
            "data": [
                {
                    "serverName": "filesystem",
                    "status": "running",
                    "tools": [
                        {"name": "read_file"},
                    ],
                },
            ],
        })
    elif method == "hooks/list":
        assert params == {"cwds": ["/repo"]}
        response(message_id, {
            "data": [
                {
                    "cwd": "/repo",
                    "hooks": [
                        {"name": "SessionStart"},
                    ],
                },
            ],
        })
    elif method == "thread/loaded/list":
        assert params == {}
        response(message_id, {"data": ["thread-1"]})
    elif method == "thread/backgroundTerminals/list":
        assert params == {"threadId": "thread-1"}
        response(message_id, {
            "data": [
                {
                    "terminalId": "terminal-1",
                    "command": "cargo test",
                    "status": "running",
                },
            ],
        })
    elif method == "thread/backgroundTerminals/clean":
        assert params == {"threadId": "thread-1"}
        response(message_id, {
            "data": [
                {
                    "terminalId": "terminal-1",
                    "status": "cleaned",
                },
            ],
        })
    elif method == "thread/backgroundTerminals/terminate":
        assert params == {
            "threadId": "thread-1",
            "processId": "42",
        }
        response(message_id, {"terminated": True})
    elif method == "thread/rollback":
        assert params == {
            "threadId": "thread-1",
            "numTurns": 2,
        }
        response(message_id, {
            "thread": {
                "id": "thread-1",
                "cwd": "/repo",
                "turns": [
                    {
                        "id": "rollback-turn-1",
                        "items": [],
                    },
                ],
            },
        })
    elif method == "skills/list":
        assert params["cwds"] == ["/repo"]
        assert params["forceReload"] in (False, True)
        response(message_id, {
            "data": [
                {
                    "cwd": "/repo",
                    "skills": [
                        {
                            "name": "skill-creator",
                            "path": "/skills/skill-creator/SKILL.md",
                            "description": "Create skills",
                            "enabled": skill_creator_enabled,
                            "interface": {
                                "displayName": "Skill Creator",
                                "shortDescription": "Create or update Codex skills",
                                "defaultPrompt": "Create a skill",
                            },
                        },
                        {
                            "name": "disabled-skill",
                            "description": "Not currently enabled",
                            "enabled": False,
                        },
                    ],
                    "errors": [],
                },
            ],
        })
    elif method == "skills/config/write":
        assert params["name"] == "skill-creator"
        assert "path" not in params
        skill_creator_enabled = params["enabled"]
        response(message_id, {})
    elif method == "skills/extraRoots/set":
        assert params == {
            "roots": [
                "/repo/.codex/skills",
                "/shared/codex-skills",
            ],
        }
        response(message_id, {})
    elif method == "model/list":
        assert params == {"includeHidden": False}
        response(message_id, {
            "data": [
                {
                    "id": "gpt-5",
                    "model": "gpt-5",
                    "displayName": "GPT-5",
                    "description": "Default model",
                    "supportedReasoningEfforts": [
                        {"reasoningEffort": "medium", "description": "Balanced"},
                        {"reasoningEffort": "high", "description": "More thorough"},
                    ],
                    "defaultReasoningEffort": "medium",
                    "serviceTiers": [
                        {"id": "standard", "name": "Standard", "description": "Default speed"},
                        {"id": "priority", "name": "Priority", "description": "Faster"},
                    ],
                    "defaultServiceTier": "standard",
                    "hidden": False,
                    "isDefault": True,
                },
                {
                    "id": "gpt-5-codex",
                    "model": "gpt-5-codex",
                    "displayName": "GPT-5 Codex",
                    "description": "Coding model",
                    "supportedReasoningEfforts": [
                        {"reasoningEffort": "low", "description": "Fast"},
                        {"reasoningEffort": "high", "description": "Deep"},
                    ],
                    "defaultReasoningEffort": "high",
                    "serviceTiers": [
                        {"id": "standard", "name": "Standard", "description": "Default speed"},
                        {"id": "priority", "name": "Priority", "description": "Faster"},
                    ],
                    "defaultServiceTier": "standard",
                    "hidden": False,
                    "isDefault": False,
                },
            ],
            "nextCursor": None,
        })
    elif method == "collaborationMode/list":
        assert params == {}
        response(message_id, {
            "data": [
                {
                    "name": "Default",
                    "mode": "default",
                    "model": None,
                    "reasoning_effort": None,
                },
                {
                    "name": "Plan",
                    "mode": "plan",
                    "model": None,
                    "reasoning_effort": "medium",
                },
            ],
        })
    elif method == "permissionProfile/list":
        assert params["cwd"] == "/repo"
        response(message_id, {
            "data": [
                {"id": ":read-only", "description": None},
                {"id": ":workspace", "description": None},
                {"id": ":danger-full-access", "description": None},
            ],
            "nextCursor": None,
        })
    elif method == "thread/settings/update":
        assert params["threadId"] == "thread-1"
        if "model" in params:
            assert params == {"threadId": "thread-1", "model": "gpt-5-codex"}
        elif "permissions" in params:
            assert params == {
                "threadId": "thread-1",
                "permissions": ":danger-full-access",
            }
        elif "effort" in params:
            assert params == {"threadId": "thread-1", "effort": "high"}
        elif "approvalPolicy" in params:
            assert params == {"threadId": "thread-1", "approvalPolicy": "never"}
        elif "collaborationMode" in params:
            assert params == {
                "threadId": "thread-1",
                "collaborationMode": {
                    "mode": "plan",
                    "settings": {
                        "model": "gpt-5-codex",
                        "reasoning_effort": "medium",
                        "developer_instructions": None,
                    },
                },
            }
        else:
            assert "serviceTier" in params
            assert params in (
                {"threadId": "thread-1", "serviceTier": "priority"},
                {"threadId": "thread-1", "serviceTier": None},
            )
        response(message_id, {})
    elif method == "thread/turns/list":
        assert params["threadId"] == "thread-1"
        assert params["limit"] == 50
        assert params["sortDirection"] == "asc"
        assert params["itemsView"] == "full"
        if params.get("cursor") is None:
            response(message_id, {
                "data": [
                    {
                        "id": "turn-history-page",
                        "items": [
                            {
                                "type": "userMessage",
                                "content": [
                                    {"type": "text", "text": "page hello"},
                                ],
                            },
                        ],
                    },
                ],
                "nextCursor": "older-turns",
                "backwardsCursor": None,
            })
        else:
            assert params["cursor"] == "older-turns"
            response(message_id, {
                "data": [
                    {
                        "id": "turn-history-older",
                        "items": [],
                    },
                ],
                "nextCursor": None,
                "backwardsCursor": "newer-turns",
            })
    elif method == "thread/read":
        assert params["threadId"] == "thread-1"
        assert params["includeTurns"] is True
        response(message_id, {
            "thread": {
                "id": "thread-1",
                "cwd": "/repo",
                "name": "Started Thread",
                "turns": [
                    {
                        "id": "turn-history",
                        "items": [
                            {
                                "type": "userMessage",
                                "id": "user-history",
                                "content": [
                                    {"type": "text", "text": "hello"},
                                ],
                            },
                            {
                                "type": "reasoning",
                                "id": "reasoning-history",
                                "summary": [
                                    {"text": "remembered reasoning"},
                                ],
                            },
                            {
                                "type": "commandExecution",
                                "id": "cmd-history",
                                "command": "cargo check",
                                "cwd": "/repo",
                                "status": "completed",
                                "aggregatedOutput": "checked",
                            },
                            {
                                "type": "fileChange",
                                "id": "file-history",
                                "status": "completed",
                                "changes": [
                                    {
                                        "path": "src/lib.rs",
                                        "kind": "update",
                                        "diff": "@@ -1 +1 @@",
                                    },
                                ],
                            },
                            {
                                "type": "agentMessage",
                                "id": "agent-history",
                                "text": "remembered response",
                            },
                        ],
                    },
                ],
            }
        })
    elif method == "thread/shellCommand":
        assert params == {
            "threadId": "thread-1",
            "command": "echo hi",
        }
        response(message_id, {})
        send({
            "method": "turn/started",
            "params": {
                "threadId": "thread-1",
                "turn": {"id": "turn-shell", "status": "running"},
            },
        })
        send({
            "method": "item/started",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-shell",
                "item": {
                    "type": "commandExecution",
                    "id": "shell-1",
                    "command": "echo hi",
                    "cwd": "/repo",
                    "status": "inProgress",
                },
            },
        })
        send({
            "method": "item/commandExecution/outputDelta",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-shell",
                "itemId": "shell-1",
                "delta": "hi\n",
            },
        })
        send({
            "method": "item/completed",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-shell",
                "item": {
                    "type": "commandExecution",
                    "id": "shell-1",
                    "command": "echo hi",
                    "status": "completed",
                    "aggregatedOutput": "hi\n",
                },
            },
        })
        send({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-1",
                "turn": {"id": "turn-shell", "status": "completed"},
            },
        })
    elif method == "turn/start":
        assert params["threadId"] == "thread-1"
        if params["input"] == [{"type": "text", "text": "hello"}]:
            response(message_id, {"turn": {"id": "turn-1", "status": "running"}})
            send({
                "method": "item/reasoning/summaryTextDelta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "itemId": "reasoning-1",
                    "delta": "thinking",
                },
            })
            send({
                "method": "turn/plan/updated",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "plan": [
                        {"step": "Run tests", "status": "inProgress"},
                    ],
                },
            })
            send({
                "method": "item/started",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
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
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "itemId": "cmd-1",
                    "delta": "ok",
                },
            })
            send({
                "method": "item/completed",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
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
                "method": "turn/diff/updated",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "diff": "diff --git a/src/lib.rs b/src/lib.rs",
                },
            })
            send({
                "method": "thread/tokenUsage/updated",
                "params": {
                    "threadId": "thread-1",
                    "used": 42,
                    "size": 100,
                },
            })
            send({
                "method": "skills/changed",
                "params": {},
            })
            send({
                "method": "thread/settings/updated",
                "params": {
                    "threadId": "thread-1",
                    "threadSettings": {
                        "cwd": "/repo",
                        "model": "gpt-5-codex",
                        "effort": "high",
                        "serviceTier": "priority",
                        "approvalPolicy": "never",
                        "collaborationMode": {
                            "mode": "plan",
                            "settings": {
                                "model": "gpt-5-codex",
                                "reasoning_effort": "medium",
                                "developer_instructions": None,
                            },
                        },
                        "activePermissionProfile": {"id": ":workspace"},
                    },
                },
            })
            send({
                "method": "thread/realtime/started",
                "params": {
                    "threadId": "thread-1",
                    "realtimeSessionId": "session-1",
                },
            })
            send({
                "method": "thread/realtime/sdp",
                "params": {
                    "threadId": "thread-1",
                    "sdp": "answer-sdp",
                },
            })
            send({
                "method": "thread/realtime/itemAdded",
                "params": {
                    "threadId": "thread-1",
                    "item": {
                        "kind": "handoff_request",
                        "target": "browser",
                    },
                },
            })
            send({
                "method": "thread/realtime/transcript/delta",
                "params": {
                    "threadId": "thread-1",
                    "role": "assistant",
                    "delta": "live",
                },
            })
            send({
                "method": "thread/realtime/transcript/done",
                "params": {
                    "threadId": "thread-1",
                    "role": "assistant",
                    "text": "live final",
                },
            })
            send({
                "method": "thread/realtime/outputAudio/delta",
                "params": {
                    "threadId": "thread-1",
                    "audio": {
                        "data": "YWJjZA==",
                        "sampleRate": 24000,
                        "numChannels": 1,
                        "samplesPerChannel": 320,
                    },
                },
            })
            send({
                "method": "thread/realtime/error",
                "params": {
                    "threadId": "thread-1",
                    "message": "network interrupted",
                },
            })
            send({
                "method": "thread/realtime/closed",
                "params": {
                    "threadId": "thread-1",
                    "reason": "client stopped",
                },
            })
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
        elif params["input"] == [
            {"type": "text", "text": "$skill-creator Make one"},
            {"type": "skill", "name": "skill-creator", "path": "/skills/skill-creator/SKILL.md"},
        ]:
            response(message_id, {"turn": {"id": "turn-skill", "status": "running"}})
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-skill",
                    "itemId": "item-skill",
                    "delta": "skill response",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-skill", "status": "completed"},
                },
            })
        elif params["input"] == [{"type": "text", "text": "approval callback"}]:
            response(message_id, {"turn": {"id": "turn-approval", "status": "running"}})
            send({
                "method": "item/started",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-approval",
                    "item": {
                        "type": "commandExecution",
                        "id": "cmd-approval",
                        "command": "cargo fmt",
                        "cwd": "/repo",
                        "status": "pending",
                    },
                },
            })
            send({
                "id": "approval-request-1",
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-approval",
                    "itemId": "cmd-approval",
                    "command": "cargo fmt",
                    "cwd": "/repo",
                    "reason": "test approval",
                    "availableDecisions": ["accept", "decline"],
                },
            })
            approval_response = json.loads(sys.stdin.readline())
            assert approval_response == {
                "id": "approval-request-1",
                "result": {"decision": "accept"},
            }
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-approval",
                    "itemId": "item-approval",
                    "delta": "approved callback",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-approval", "status": "completed"},
                },
            })
        elif params["input"] == [{"type": "text", "text": "file approval callback"}]:
            response(message_id, {"turn": {"id": "turn-file-approval", "status": "running"}})
            send({
                "method": "item/started",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-file-approval",
                    "item": {
                        "type": "fileChange",
                        "id": "file-approval",
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
                "id": "file-approval-request-1",
                "method": "item/fileChange/requestApproval",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-file-approval",
                    "itemId": "file-approval",
                    "reason": "test file approval",
                },
            })
            approval_response = json.loads(sys.stdin.readline())
            assert approval_response == {
                "id": "file-approval-request-1",
                "result": {"decision": "acceptForSession"},
            }
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-file-approval",
                    "itemId": "item-file-approval",
                    "delta": "approved file",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-file-approval", "status": "completed"},
                },
            })
        elif params["input"] == [{"type": "text", "text": "permissions approval callback"}]:
            response(message_id, {"turn": {"id": "turn-permissions-approval", "status": "running"}})
            send({
                "id": "permissions-approval-request-1",
                "method": "item/permissions/requestApproval",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-permissions-approval",
                    "itemId": "permissions-approval",
                    "environmentId": "local",
                    "startedAtMs": 123,
                    "cwd": "/repo",
                    "reason": "Need network and write access",
                    "permissions": {
                        "network": {"enabled": True},
                        "fileSystem": {
                            "read": ["/repo"],
                            "write": ["/repo/src"],
                        },
                    },
                },
            })
            approval_response = json.loads(sys.stdin.readline())
            assert approval_response == {
                "id": "permissions-approval-request-1",
                "result": {
                    "permissions": {
                        "network": {"enabled": True},
                        "fileSystem": {
                            "read": ["/repo"],
                            "write": ["/repo/src"],
                        },
                    },
                    "scope": "session",
                },
            }
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-permissions-approval",
                    "itemId": "item-permissions-approval",
                    "delta": "approved permissions",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-permissions-approval", "status": "completed"},
                },
            })
        elif params["input"] == [{"type": "text", "text": "mcp elicitation callback"}]:
            response(message_id, {"turn": {"id": "turn-mcp-elicitation", "status": "running"}})
            send({
                "id": "mcp-elicitation-request-1",
                "method": "mcpServer/elicitation/request",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-mcp-elicitation",
                    "serverName": "test-mcp",
                    "type": "url",
                    "message": "Authorize app",
                    "url": "https://example.test/auth",
                    "elicitationId": "elicitation-1",
                },
            })
            elicitation_response = json.loads(sys.stdin.readline())
            assert elicitation_response == {
                "id": "mcp-elicitation-request-1",
                "result": {
                    "action": "cancel",
                    "content": None,
                    "_meta": None,
                },
            }
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-mcp-elicitation",
                    "itemId": "item-mcp-elicitation",
                    "delta": "cancelled elicitation",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-mcp-elicitation", "status": "completed"},
                },
            })
        elif params["input"] == [{"type": "text", "text": "user input callback"}]:
            response(message_id, {"turn": {"id": "turn-user-input", "status": "running"}})
            send({
                "id": "user-input-request-1",
                "method": "item/tool/requestUserInput",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-user-input",
                    "itemId": "user-input",
                    "questions": [
                        {
                            "id": "confirm",
                            "header": "Confirm",
                            "question": "Proceed?",
                            "options": [
                                {"label": "Yes", "description": "Continue"},
                                {"label": "No", "description": "Stop"},
                            ],
                        },
                    ],
                },
            })
            user_input_response = json.loads(sys.stdin.readline())
            assert user_input_response == {
                "id": "user-input-request-1",
                "result": {"answers": {}},
            }
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-user-input",
                    "itemId": "item-user-input",
                    "delta": "empty user input",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-user-input", "status": "completed"},
                },
            })
        elif params["input"] == [{"type": "text", "text": "dynamic tool callback"}]:
            response(message_id, {"turn": {"id": "turn-dynamic-tool", "status": "running"}})
            send({
                "id": "dynamic-tool-request-1",
                "method": "item/tool/call",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-dynamic-tool",
                    "callId": "dynamic-tool",
                    "namespace": "test",
                    "tool": "unsupported",
                    "arguments": {},
                },
            })
            dynamic_tool_response = json.loads(sys.stdin.readline())
            assert dynamic_tool_response == {
                "id": "dynamic-tool-request-1",
                "result": {
                    "contentItems": [
                        {
                            "type": "inputText",
                            "text": "Dynamic tool calls are not supported by this ACP adapter yet.",
                        },
                    ],
                    "success": False,
                },
            }
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-dynamic-tool",
                    "itemId": "item-dynamic-tool",
                    "delta": "failed dynamic tool",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-dynamic-tool", "status": "completed"},
                },
            })
        elif params["input"] == [{"type": "text", "text": "current time callback"}]:
            response(message_id, {"turn": {"id": "turn-current-time", "status": "running"}})
            send({
                "id": "current-time-request-1",
                "method": "currentTime/read",
                "params": {
                    "threadId": "thread-1",
                },
            })
            current_time_response = json.loads(sys.stdin.readline())
            assert current_time_response["id"] == "current-time-request-1"
            timestamp = current_time_response["result"]["currentTimeAt"]
            assert isinstance(timestamp, int)
            assert timestamp > 1700000000
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-current-time",
                    "itemId": "item-current-time",
                    "delta": "reported current time",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-current-time", "status": "completed"},
                },
            })
        elif params["input"] == [{"type": "text", "text": "unsupported server request"}]:
            response(message_id, {"turn": {"id": "turn-unsupported-request", "status": "running"}})
            send({
                "id": "unsupported-request-1",
                "method": "future/request",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-unsupported-request",
                },
            })
            unsupported_response = json.loads(sys.stdin.readline())
            assert unsupported_response == {
                "id": "unsupported-request-1",
                "error": {
                    "code": -32601,
                    "message": "unsupported app-server request `future/request`",
                },
            }
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-unsupported-request",
                    "itemId": "item-unsupported-request",
                    "delta": "rejected unsupported server request",
                },
            })
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-unsupported-request", "status": "completed"},
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
    elif method == "thread/delete":
        assert params["threadId"] == "thread-1"
        response(message_id, {})
        send({
            "method": "thread/deleted",
            "params": {"threadId": "thread-1"},
        })
        send({
            "method": "thread/closed",
            "params": {"threadId": "thread-1"},
        })
    else:
        send({
            "id": message_id,
            "error": {
                "code": -32601,
                "message": f"unknown method: {method}",
            },
        })
"#;
