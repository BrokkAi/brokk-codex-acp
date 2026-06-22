use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use agent_client_protocol::schema::{
    AgentCapabilities, AgentRequest, AvailableCommand, AvailableCommandInput,
    AvailableCommandsUpdate, CancelNotification, CloseSessionRequest, CloseSessionResponse,
    ConfigOptionUpdate, ContentBlock, ContentChunk, CreateElicitationRequest, DeleteSessionRequest,
    DeleteSessionResponse, Diff, ElicitationAction, ElicitationContentValue, ElicitationFormMode,
    ElicitationMode, ElicitationSchema, ElicitationSessionScope, ElicitationUrlMode,
    EmbeddedResourceResource, EnumOption, ExtRequest, ForkSessionRequest, ForkSessionResponse,
    InitializeRequest, InitializeResponse, ListSessionsRequest, ListSessionsResponse,
    LoadSessionRequest, LoadSessionResponse, MessageId, NewSessionRequest, NewSessionResponse,
    PermissionOption, PermissionOptionId, PermissionOptionKind, Plan, PlanEntry, PlanEntryPriority,
    PlanEntryStatus, PromptCapabilities, PromptRequest, PromptResponse, ProtocolVersion,
    RequestPermissionOutcome, RequestPermissionRequest, ResumeSessionRequest,
    ResumeSessionResponse, SessionAdditionalDirectoriesCapabilities, SessionCapabilities,
    SessionCloseCapabilities, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectOption, SessionDeleteCapabilities, SessionForkCapabilities, SessionId,
    SessionInfo, SessionInfoUpdate, SessionListCapabilities, SessionNotification,
    SessionResumeCapabilities, SessionUpdate, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, StopReason, StringPropertySchema, TextContent, ToolCall,
    ToolCallContent, ToolCallId, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, ToolKind,
    UnstructuredCommandInput, UsageUpdate,
};
use agent_client_protocol::{
    Agent, ByteStreams, Client, ConnectTo, ConnectionTo, Error, on_receive_notification,
    on_receive_request,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, trace, warn};

use crate::app_server::{
    AppServerAccountLoginCompletedUpdate, AppServerAccountRateLimitsUpdatedUpdate,
    AppServerAccountUpdatedUpdate, AppServerActivePermissionProfile, AppServerAdditionalContext,
    AppServerAdditionalContextEntry, AppServerAdditionalContextKind, AppServerAgentMessageDelta,
    AppServerAppListUpdate, AppServerApprovalChoice, AppServerApprovalDecision,
    AppServerApprovalOption, AppServerApprovalRequest, AppServerApprovalResponseKind,
    AppServerClient, AppServerCollaborationMode, AppServerCollaborationModeMask,
    AppServerCollaborationModeSettings, AppServerConfigWarningUpdate,
    AppServerDynamicToolCallRequest, AppServerErrorUpdate, AppServerFuzzyFileSearchUpdate,
    AppServerGuardianApprovalReviewLifecycle, AppServerGuardianApprovalReviewUpdate,
    AppServerHistoryEvent, AppServerInteractiveRequest, AppServerMcpElicitationRequest,
    AppServerMcpServerOAuthLoginCompletedUpdate, AppServerMcpServerStartupStatusUpdate,
    AppServerMessage, AppServerModel, AppServerModelReroutedUpdate,
    AppServerModelSafetyBufferingUpdate, AppServerModelVerificationUpdate,
    AppServerPermissionProfile, AppServerPlanStatus, AppServerPromptCompletion,
    AppServerPromptEvent, AppServerRealtimeAudioDelta, AppServerRealtimeUpdate,
    AppServerRemoteControlStatusUpdate, AppServerServerRequestResolvedUpdate, AppServerSkill,
    AppServerThread, AppServerThreadSettingsUpdate, AppServerToolKind, AppServerToolStatus,
    AppServerTurnInput, AppServerTurnModerationMetadataUpdate, AppServerTurnStartInput,
    AppServerUserInputQuestion, AppServerUserInputRequest, AppServerWarningUpdate,
    AppServerWindowsSandboxSetupUpdate, ThreadMemoryMode, ThreadSettingsUpdateParams,
    decode_account_login_completed, decode_account_rate_limits_updated, decode_account_updated,
    decode_app_list_updated, decode_config_warning, decode_error, decode_fuzzy_file_search_update,
    decode_guardian_approval_review_update, decode_mcp_server_oauth_login_completed,
    decode_mcp_server_startup_status_updated, decode_model_rerouted,
    decode_model_safety_buffering_updated, decode_model_verification, decode_realtime_update,
    decode_remote_control_status_changed, decode_server_request_resolved, decode_thread_archived,
    decode_thread_closed, decode_thread_deleted, decode_thread_goal_cleared,
    decode_thread_goal_updated, decode_thread_name_updated, decode_thread_settings_updated,
    decode_thread_status_changed, decode_thread_unarchived, decode_turn_moderation_metadata,
    decode_warning, decode_windows_sandbox_setup_completed, history_events_for_turns,
    is_app_server_method_unavailable,
};

const MODEL_CONFIG_ID: &str = "model";
const REASONING_EFFORT_CONFIG_ID: &str = "reasoning_effort";
const SERVICE_TIER_CONFIG_ID: &str = "service_tier";
const APPROVAL_POLICY_CONFIG_ID: &str = "approval_policy";
const COLLABORATION_MODE_CONFIG_ID: &str = "collaboration_mode";
const PERMISSION_PROFILE_CONFIG_ID: &str = "permission_profile";
const SKILL_CONFIG_PREFIX: &str = "skill:";
const DEFAULT_SERVICE_TIER_VALUE: &str = "__codex_default_service_tier";
const DEFAULT_APPROVAL_POLICY: &str = "on-request";
const SKILL_ENABLED_VALUE: &str = "enabled";
const SKILL_DISABLED_VALUE: &str = "disabled";
const DYNAMIC_TOOL_CALL_METHOD: &str = "_brokk_codex_acp/dynamic_tool_call";
const ACCOUNT_COMMAND: &str = "account";
const ARCHIVE_COMMAND: &str = "archive";
const APPS_COMMAND: &str = "apps";
const COMPACT_COMMAND: &str = "compact";
const CONFIG_COMMAND: &str = "config";
const CONFIG_REQUIREMENTS_COMMAND: &str = "config-requirements";
const DELETE_COMMAND: &str = "delete";
const FEATURE_COMMAND: &str = "feature";
const FEATURES_COMMAND: &str = "features";
const FORK_COMMAND: &str = "fork";
const GOAL_COMMAND: &str = "goal";
const HOOKS_COMMAND: &str = "hooks";
const INIT_COMMAND: &str = "init";
const KILL_COMMAND: &str = "kill";
const MARKETPLACE_ADD_COMMAND: &str = "marketplace-add";
const MARKETPLACE_REMOVE_COMMAND: &str = "marketplace-remove";
const MEMORY_COMMAND: &str = "memory";
const MCP_COMMAND: &str = "mcp";
const MCP_RELOAD_COMMAND: &str = "mcp-reload";
const MCP_RESOURCE_COMMAND: &str = "mcp-resource";
const MCP_TOOL_COMMAND: &str = "mcp-tool";
const MODEL_COMMAND: &str = "model";
const NEW_COMMAND: &str = "new";
const PERMISSIONS_COMMAND: &str = "permissions";
const PLAN_COMMAND: &str = "plan";
const PLUGIN_COMMAND: &str = "plugin";
const PLUGIN_INSTALL_COMMAND: &str = "plugin-install";
const PLUGIN_UNINSTALL_COMMAND: &str = "plugin-uninstall";
const PLUGINS_COMMAND: &str = "plugins";
const PS_COMMAND: &str = "ps";
const RATE_LIMITS_COMMAND: &str = "rate-limits";
const RENAME_COMMAND: &str = "rename";
const RESUME_COMMAND: &str = "resume";
const REVIEW_COMMAND: &str = "review";
const ROLLBACK_COMMAND: &str = "rollback";
const SKILL_COMMAND: &str = "skill";
const SKILL_ROOTS_COMMAND: &str = "skill-roots";
const STATUS_COMMAND: &str = "status";
const STOP_COMMAND: &str = "stop";
const UNARCHIVE_COMMAND: &str = "unarchive";
const USAGE_COMMAND: &str = "usage";
const WORKSPACE_MESSAGES_COMMAND: &str = "workspace-messages";
type CancelSignals = Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>;
const HISTORY_REPLAY_PAGE_SIZE: u32 = 50;
const APPROVAL_POLICY_OPTIONS: [(&str, &str, &str); 4] = [
    (
        "untrusted",
        "Ask for untrusted commands",
        "Ask before running commands Codex does not already trust.",
    ),
    (
        "on-failure",
        "Ask on failure",
        "Ask before retrying commands that fail under the sandbox.",
    ),
    (
        "on-request",
        "Ask when requested",
        "Ask whenever Codex requests explicit approval.",
    ),
    (
        "never",
        "Never ask",
        "Do not ask for approval before running subsequent turns.",
    ),
];

#[derive(Clone)]
pub struct CodexAcpAgent {
    app_server: Arc<Mutex<AppServerClient>>,
    active_prompts: CancelSignals,
    outstanding_approvals: CancelSignals,
    skills_by_cwd: Arc<Mutex<HashMap<String, Vec<AppServerSkill>>>>,
    session_cwds: Arc<Mutex<HashMap<String, String>>>,
    session_additional_directories: Arc<Mutex<HashMap<String, Vec<PathBuf>>>>,
    session_additional_directories_store: Option<PathBuf>,
    config_by_session: Arc<Mutex<HashMap<String, AcpConfigState>>>,
    notification_listener_started: Arc<Mutex<bool>>,
}

#[derive(Debug, Clone, Default)]
struct AcpConfigState {
    current_model: Option<String>,
    current_reasoning_effort: Option<String>,
    current_service_tier: Option<String>,
    current_approval_policy: Option<String>,
    current_collaboration_mode: Option<String>,
    current_permission_profile: Option<String>,
    models: Vec<AppServerModel>,
    collaboration_modes: Vec<AppServerCollaborationModeMask>,
    permission_profiles: Vec<AppServerPermissionProfile>,
}

#[derive(Debug, Default)]
struct CurrentConfigSelections {
    cwd: Option<String>,
    model: Option<String>,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    approval_policy: Option<String>,
    collaboration_mode: Option<AppServerCollaborationMode>,
    active_permission_profile: Option<AppServerActivePermissionProfile>,
}

#[derive(Default)]
struct PendingAppServerUpdates {
    skills_changed: bool,
    thread_settings: Option<AppServerThreadSettingsUpdate>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedSessionAdditionalDirectories {
    #[serde(default)]
    sessions: HashMap<String, Vec<PathBuf>>,
}

impl CodexAcpAgent {
    pub fn new(app_server: AppServerClient) -> Self {
        let session_additional_directories_store = app_server
            .codex_home()
            .map(session_additional_directories_store_path);
        let session_additional_directories = session_additional_directories_store
            .as_deref()
            .map(load_session_additional_directories)
            .unwrap_or_default();

        Self {
            app_server: Arc::new(Mutex::new(app_server)),
            active_prompts: Arc::new(Mutex::new(HashMap::new())),
            outstanding_approvals: Arc::new(Mutex::new(HashMap::new())),
            skills_by_cwd: Arc::new(Mutex::new(HashMap::new())),
            session_cwds: Arc::new(Mutex::new(HashMap::new())),
            session_additional_directories: Arc::new(Mutex::new(session_additional_directories)),
            session_additional_directories_store,
            config_by_session: Arc::new(Mutex::new(HashMap::new())),
            notification_listener_started: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn serve_stdio(self) -> agent_client_protocol::Result<()> {
        let stdin = tokio::io::stdin().compat();
        let stdout = tokio::io::stdout().compat_write();
        self.serve(ByteStreams::new(stdout, stdin)).await
    }

    pub async fn serve(
        self,
        transport: impl ConnectTo<Agent> + 'static,
    ) -> agent_client_protocol::Result<()> {
        let agent = Arc::new(self);

        let result = Agent
            .builder()
            .name("brokk-codex-acp")
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: InitializeRequest, responder, cx: ConnectionTo<Client>| {
                        agent.ensure_notification_listener(cx.clone()).await?;
                        responder.respond_with_result(agent.initialize(request).await)
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: NewSessionRequest, responder, cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        let session_cx = cx.clone();
                        cx.spawn(async move {
                            responder
                                .respond_with_result(agent.new_session(request, session_cx).await)
                        })?;
                        Ok(())
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: ForkSessionRequest, responder, cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        let session_cx = cx.clone();
                        cx.spawn(async move {
                            responder
                                .respond_with_result(agent.fork_session(request, session_cx).await)
                        })?;
                        Ok(())
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: LoadSessionRequest, responder, cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        let session_cx = cx.clone();
                        cx.spawn(async move {
                            responder
                                .respond_with_result(agent.load_session(request, session_cx).await)
                        })?;
                        Ok(())
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: ResumeSessionRequest,
                                responder,
                                cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        let session_cx = cx.clone();
                        cx.spawn(async move {
                            responder.respond_with_result(
                                agent.resume_session(request, session_cx).await,
                            )
                        })?;
                        Ok(())
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: ListSessionsRequest,
                                responder,
                                cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        cx.spawn(async move {
                            responder.respond_with_result(agent.list_sessions(request).await)
                        })?;
                        Ok(())
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: CloseSessionRequest,
                                responder,
                                cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        cx.spawn(async move {
                            responder.respond_with_result(agent.close_session(request).await)
                        })?;
                        Ok(())
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: DeleteSessionRequest,
                                responder,
                                cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        cx.spawn(async move {
                            responder.respond_with_result(agent.delete_session(request).await)
                        })?;
                        Ok(())
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: PromptRequest, responder, cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        let session_cx = cx.clone();
                        cx.spawn(async move {
                            responder.respond_with_result(agent.prompt(request, session_cx).await)
                        })?;
                        Ok(())
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: SetSessionConfigOptionRequest,
                                responder,
                                cx: ConnectionTo<Client>| {
                        let agent = agent.clone();
                        let session_cx = cx.clone();
                        cx.spawn(async move {
                            responder.respond_with_result(
                                agent.set_config_option(request, session_cx).await,
                            )
                        })?;
                        Ok(())
                    }
                },
                on_receive_request!(),
            )
            .on_receive_notification(
                {
                    let agent = agent.clone();
                    async move |notification: CancelNotification, _cx: ConnectionTo<Client>| {
                        agent.cancel_session(notification).await
                    }
                },
                on_receive_notification!(),
            )
            .connect_to(transport)
            .await;

        let cancelled_approvals = cancel_outstanding_approvals(&agent.outstanding_approvals).await;
        if cancelled_approvals > 0 {
            debug!(
                cancelled_approvals,
                "cancelled outstanding approvals after ACP transport disconnect"
            );
        }

        let cancelled_prompts = cancel_active_prompts(&agent.active_prompts).await;
        if cancelled_prompts > 0 {
            debug!(
                cancelled_prompts,
                "cancelled active prompts after ACP transport disconnect"
            );
        }

        result
    }

    async fn initialize(&self, request: InitializeRequest) -> Result<InitializeResponse, Error> {
        let _requested_version = request.protocol_version;

        Ok(InitializeResponse::new(ProtocolVersion::V1)
            .agent_capabilities(Self::capabilities())
            .auth_methods(vec![]))
    }

    async fn ensure_notification_listener(
        self: &Arc<Self>,
        cx: ConnectionTo<Client>,
    ) -> Result<(), Error> {
        let mut started = self.notification_listener_started.lock().await;
        if *started {
            return Ok(());
        }

        let messages_rx = self.app_server.lock().await.subscribe();
        let agent = self.clone();
        let listener_cx = cx.clone();
        cx.spawn(async move {
            agent
                .run_app_server_notification_listener(listener_cx, messages_rx)
                .await;
            Ok(())
        })?;
        *started = true;
        Ok(())
    }

    async fn run_app_server_notification_listener(
        self: Arc<Self>,
        cx: ConnectionTo<Client>,
        mut messages_rx: broadcast::Receiver<AppServerMessage>,
    ) {
        loop {
            let message = match messages_rx.recv().await {
                Ok(message) => message,
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    debug!(
                        count,
                        "missed app-server notifications in background listener"
                    );
                    continue;
                }
            };

            let AppServerMessage::Notification { method, params } = message else {
                continue;
            };

            if let Err(error) = self
                .handle_background_app_server_notification(&cx, &method, params)
                .await
            {
                debug!(%method, %error, "failed to handle app-server background notification");
            }
        }
    }

    async fn handle_background_app_server_notification(
        &self,
        cx: &ConnectionTo<Client>,
        method: &str,
        params: serde_json::Value,
    ) -> Result<(), Error> {
        match method {
            "skills/changed" => {
                let sessions = self.session_cwds.lock().await.clone();
                let active_prompts = self.active_prompts.lock().await;
                let sessions = sessions
                    .into_iter()
                    .filter(|(thread_id, _)| !active_prompts.contains_key(thread_id))
                    .collect::<Vec<_>>();
                drop(active_prompts);

                for (thread_id, cwd) in sessions {
                    self.refresh_and_publish_skills(cwd, &SessionId::new(thread_id), cx, true)
                        .await?;
                }
            }
            "thread/settings/updated" => {
                let settings =
                    decode_thread_settings_updated(&params).map_err(acp_internal_error)?;
                if self
                    .active_prompts
                    .lock()
                    .await
                    .contains_key(&settings.thread_id)
                {
                    return Ok(());
                }
                let session_id = SessionId::new(settings.thread_id.clone());
                self.publish_thread_settings_update(&session_id, settings, cx)
                    .await?;
            }
            "thread/name/updated" => {
                let update = decode_thread_name_updated(&params).map_err(acp_internal_error)?;
                let session_id = SessionId::new(update.thread_id);
                publish_session_name_update(&session_id, update.thread_name, cx)
                    .map_err(acp_internal_error)?;
            }
            "thread/archived" => {
                let update = decode_thread_archived(&params).map_err(acp_internal_error)?;
                let session_id = SessionId::new(update.thread_id);
                publish_session_archived_update(&session_id, true, cx)
                    .map_err(acp_internal_error)?;
            }
            "thread/unarchived" => {
                let update = decode_thread_unarchived(&params).map_err(acp_internal_error)?;
                let session_id = SessionId::new(update.thread_id);
                publish_session_archived_update(&session_id, false, cx)
                    .map_err(acp_internal_error)?;
            }
            "thread/deleted" => {
                let update = decode_thread_deleted(&params).map_err(acp_internal_error)?;
                let session_id = SessionId::new(update.thread_id);
                publish_session_deleted_update(&session_id, true, cx)
                    .map_err(acp_internal_error)?;
            }
            "thread/closed" => {
                let update = decode_thread_closed(&params).map_err(acp_internal_error)?;
                let session_id = SessionId::new(update.thread_id);
                publish_session_closed_update(&session_id, true, cx).map_err(acp_internal_error)?;
            }
            "thread/status/changed" => {
                let update = decode_thread_status_changed(&params).map_err(acp_internal_error)?;
                let session_id = SessionId::new(update.thread_id);
                publish_session_status_update(&session_id, update.status, cx)
                    .map_err(acp_internal_error)?;
            }
            "thread/goal/updated" => {
                let update = decode_thread_goal_updated(&params).map_err(acp_internal_error)?;
                let session_id = SessionId::new(update.thread_id);
                publish_session_goal_update(&session_id, update.goal, cx)
                    .map_err(acp_internal_error)?;
            }
            "thread/goal/cleared" => {
                let update = decode_thread_goal_cleared(&params).map_err(acp_internal_error)?;
                let session_id = SessionId::new(update.thread_id);
                publish_session_goal_update(&session_id, update.goal, cx)
                    .map_err(acp_internal_error)?;
            }
            "configWarning" => {
                let update = decode_config_warning(&params).map_err(acp_internal_error)?;
                self.publish_global_agent_message_to_inactive(config_warning_message(&update), cx)
                    .await?;
            }
            "windowsSandbox/setupCompleted" => {
                let update =
                    decode_windows_sandbox_setup_completed(&params).map_err(acp_internal_error)?;
                self.publish_global_agent_message_to_inactive(
                    windows_sandbox_setup_message(&update),
                    cx,
                )
                .await?;
            }
            "account/login/completed" => {
                let update = decode_account_login_completed(&params).map_err(acp_internal_error)?;
                self.publish_global_agent_message_to_inactive(
                    account_login_completed_message(&update),
                    cx,
                )
                .await?;
            }
            "account/updated" => {
                let update = decode_account_updated(&params).map_err(acp_internal_error)?;
                self.publish_global_agent_message_to_inactive(account_updated_message(&update), cx)
                    .await?;
            }
            "account/rateLimits/updated" => {
                let update =
                    decode_account_rate_limits_updated(&params).map_err(acp_internal_error)?;
                self.publish_global_agent_message_to_inactive(
                    account_rate_limits_updated_message(&update),
                    cx,
                )
                .await?;
            }
            "mcpServer/oauthLogin/completed" => {
                let update =
                    decode_mcp_server_oauth_login_completed(&params).map_err(acp_internal_error)?;
                self.publish_global_agent_message_to_inactive(
                    mcp_oauth_login_completed_message(&update),
                    cx,
                )
                .await?;
            }
            "app/list/updated" => {
                let update = decode_app_list_updated(&params).map_err(acp_internal_error)?;
                self.publish_global_agent_message_to_inactive(
                    app_list_updated_message(&update),
                    cx,
                )
                .await?;
            }
            "remoteControl/status/changed" => {
                let update =
                    decode_remote_control_status_changed(&params).map_err(acp_internal_error)?;
                self.publish_global_agent_message_to_inactive(
                    remote_control_status_message(&update),
                    cx,
                )
                .await?;
            }
            "fuzzyFileSearch/sessionUpdated" | "fuzzyFileSearch/sessionCompleted" => {
                let update =
                    decode_fuzzy_file_search_update(method, &params).map_err(acp_internal_error)?;
                self.publish_global_agent_message_to_inactive(
                    fuzzy_file_search_message(&update),
                    cx,
                )
                .await?;
            }
            "warning" => {
                let update = decode_warning(&params).map_err(acp_internal_error)?;
                let Some(thread_id) = update.thread_id.as_ref() else {
                    return Ok(());
                };
                if self.active_prompts.lock().await.contains_key(thread_id) {
                    return Ok(());
                }
                let session_id = SessionId::new(thread_id.clone());
                publish_agent_message(&session_id, warning_message(&update), cx)
                    .map_err(acp_internal_error)?;
            }
            "error" => {
                let update = decode_error(&params).map_err(acp_internal_error)?;
                if self
                    .active_prompts
                    .lock()
                    .await
                    .contains_key(&update.thread_id)
                {
                    return Ok(());
                }
                let session_id = SessionId::new(update.thread_id.clone());
                publish_agent_message(&session_id, error_message(&update), cx)
                    .map_err(acp_internal_error)?;
            }
            "item/autoApprovalReview/started" | "item/autoApprovalReview/completed" => {
                let update = decode_guardian_approval_review_update(method, &params)
                    .map_err(acp_internal_error)?;
                if self
                    .active_prompts
                    .lock()
                    .await
                    .contains_key(&update.thread_id)
                {
                    return Ok(());
                }
                let session_id = SessionId::new(update.thread_id.clone());
                publish_agent_message(&session_id, guardian_approval_review_message(&update), cx)
                    .map_err(acp_internal_error)?;
            }
            "serverRequest/resolved" => {
                let update = decode_server_request_resolved(&params).map_err(acp_internal_error)?;
                if self
                    .active_prompts
                    .lock()
                    .await
                    .contains_key(&update.thread_id)
                {
                    return Ok(());
                }
                let session_id = SessionId::new(update.thread_id.clone());
                publish_agent_message(&session_id, server_request_resolved_message(&update), cx)
                    .map_err(acp_internal_error)?;
            }
            "model/rerouted" => {
                let update = decode_model_rerouted(&params).map_err(acp_internal_error)?;
                if self
                    .active_prompts
                    .lock()
                    .await
                    .contains_key(&update.thread_id)
                {
                    return Ok(());
                }
                let session_id = SessionId::new(update.thread_id.clone());
                publish_agent_message(&session_id, model_rerouted_message(&update), cx)
                    .map_err(acp_internal_error)?;
            }
            "model/safetyBuffering/updated" => {
                let update =
                    decode_model_safety_buffering_updated(&params).map_err(acp_internal_error)?;
                if self
                    .active_prompts
                    .lock()
                    .await
                    .contains_key(&update.thread_id)
                {
                    return Ok(());
                }
                let session_id = SessionId::new(update.thread_id.clone());
                publish_agent_message(&session_id, model_safety_buffering_message(&update), cx)
                    .map_err(acp_internal_error)?;
            }
            "model/verification" => {
                let update = decode_model_verification(&params).map_err(acp_internal_error)?;
                if self
                    .active_prompts
                    .lock()
                    .await
                    .contains_key(&update.thread_id)
                {
                    return Ok(());
                }
                let session_id = SessionId::new(update.thread_id.clone());
                publish_agent_message(&session_id, model_verification_message(&update), cx)
                    .map_err(acp_internal_error)?;
            }
            "turn/moderationMetadata" => {
                let update =
                    decode_turn_moderation_metadata(&params).map_err(acp_internal_error)?;
                if self
                    .active_prompts
                    .lock()
                    .await
                    .contains_key(&update.thread_id)
                {
                    return Ok(());
                }
                let session_id = SessionId::new(update.thread_id.clone());
                publish_agent_message(&session_id, turn_moderation_metadata_message(&update), cx)
                    .map_err(acp_internal_error)?;
            }
            "mcpServer/startupStatus/updated" => {
                let update = decode_mcp_server_startup_status_updated(&params)
                    .map_err(acp_internal_error)?;
                match update.thread_id.as_ref() {
                    Some(thread_id) => {
                        if self.active_prompts.lock().await.contains_key(thread_id) {
                            return Ok(());
                        }
                        let session_id = SessionId::new(thread_id.clone());
                        publish_agent_message(&session_id, mcp_startup_status_message(&update), cx)
                            .map_err(acp_internal_error)?;
                    }
                    None => {
                        self.publish_global_agent_message_to_inactive(
                            mcp_startup_status_message(&update),
                            cx,
                        )
                        .await?;
                    }
                }
            }
            "thread/realtime/started"
            | "thread/realtime/sdp"
            | "thread/realtime/itemAdded"
            | "thread/realtime/transcript/delta"
            | "thread/realtime/transcript/done"
            | "thread/realtime/outputAudio/delta"
            | "thread/realtime/error"
            | "thread/realtime/closed" => {
                let update = decode_realtime_update(method, &params).map_err(acp_internal_error)?;
                let thread_id = update.thread_id();
                if self.active_prompts.lock().await.contains_key(thread_id) {
                    return Ok(());
                }
                let session_id = SessionId::new(thread_id.to_owned());
                publish_agent_message(&session_id, realtime_message(&update), cx)
                    .map_err(acp_internal_error)?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn publish_global_agent_message_to_inactive(
        &self,
        message: String,
        cx: &ConnectionTo<Client>,
    ) -> Result<(), Error> {
        let session_cwds = self.session_cwds.lock().await.clone();
        let active_prompts = self.active_prompts.lock().await;
        let session_ids = session_cwds
            .keys()
            .filter(|thread_id| !active_prompts.contains_key(*thread_id))
            .cloned()
            .collect::<Vec<_>>();
        drop(active_prompts);
        for thread_id in session_ids {
            let session_id = SessionId::new(thread_id);
            publish_agent_message(&session_id, message.clone(), cx).map_err(acp_internal_error)?;
        }
        Ok(())
    }

    async fn new_session(
        &self,
        request: NewSessionRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<NewSessionResponse, Error> {
        let cwd = request.cwd.to_string_lossy().into_owned();
        debug!(method = "session/new", %cwd, "handling ACP request");
        let (session_id, config_options) = self
            .start_thread(request.cwd, request.additional_directories, &cx)
            .await?;

        Ok(NewSessionResponse::new(session_id).config_options(config_options))
    }

    async fn fork_session(
        &self,
        request: ForkSessionRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<ForkSessionResponse, Error> {
        let source_thread_id = request.session_id.0.to_string();
        let cwd = request.cwd.to_string_lossy().into_owned();
        debug!(
            method = "session/fork",
            source_thread_id,
            %cwd,
            "handling ACP request"
        );
        let (session_id, config_options) = self
            .fork_thread(
                source_thread_id,
                request.cwd,
                request.additional_directories,
                &cx,
            )
            .await?;

        Ok(ForkSessionResponse::new(session_id).config_options(config_options))
    }

    async fn load_session(
        &self,
        request: LoadSessionRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<LoadSessionResponse, Error> {
        let thread_id = request.session_id.0.to_string();
        let cwd = request.cwd.to_string_lossy().into_owned();
        debug!(method = "session/load", thread_id, %cwd, "handling ACP request");

        let runtime_workspace_roots =
            runtime_workspace_roots_for_acp_request(&request.cwd, &request.additional_directories)?;
        let resume_response = self
            .app_server
            .lock()
            .await
            .thread_resume(thread_id.clone(), cwd, Some(runtime_workspace_roots))
            .await
            .map_err(acp_internal_error)?;

        self.replay_session_history(&thread_id, &request.session_id, &cx)
            .await?;

        let mut config_options = self
            .refresh_config_options(
                &request.session_id,
                CurrentConfigSelections {
                    cwd: resume_response
                        .thread
                        .cwd
                        .as_ref()
                        .map(|cwd| cwd.to_string_lossy().into_owned()),
                    model: resume_response.model,
                    reasoning_effort: resume_response.reasoning_effort,
                    service_tier: resume_response.service_tier,
                    approval_policy: resume_response.approval_policy,
                    collaboration_mode: resume_response.collaboration_mode,
                    active_permission_profile: resume_response.active_permission_profile,
                },
            )
            .await;

        if let Some(cwd) = resume_response.thread.cwd {
            let cwd = cwd.to_string_lossy().into_owned();
            self.set_session_cwd(&request.session_id, cwd.clone()).await;
            self.set_session_additional_directories_from_runtime_roots(
                &request.session_id,
                &cwd,
                resume_response.runtime_workspace_roots,
            )
            .await;
            self.refresh_and_publish_skills(cwd, &request.session_id, &cx, false)
                .await?;
            config_options = self
                .config_options_for_session(request.session_id.0.as_ref())
                .await;
        }

        Ok(LoadSessionResponse::new().config_options(config_options))
    }

    async fn resume_session(
        &self,
        request: ResumeSessionRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<ResumeSessionResponse, Error> {
        let thread_id = request.session_id.0.to_string();
        let cwd = request.cwd.to_string_lossy().into_owned();
        debug!(method = "session/resume", thread_id, %cwd, "handling ACP request");
        let (_, config_options) = self
            .resume_thread(thread_id, request.cwd, request.additional_directories, &cx)
            .await?;

        Ok(ResumeSessionResponse::new().config_options(config_options))
    }

    async fn list_sessions(
        &self,
        request: ListSessionsRequest,
    ) -> Result<ListSessionsResponse, Error> {
        let cwd = request.cwd.map(|path| path.to_string_lossy().into_owned());
        let response = self
            .app_server
            .lock()
            .await
            .thread_list(cwd, request.cursor)
            .await
            .map_err(acp_internal_error)?;
        let additional_directories = self.session_additional_directories.lock().await.clone();

        let sessions = response
            .data
            .into_iter()
            .filter_map(|thread| {
                let additional = additional_directories
                    .get(&thread.id)
                    .cloned()
                    .unwrap_or_default();
                session_info_from_app_server_thread(thread, additional)
            })
            .collect();

        Ok(ListSessionsResponse::new(sessions).next_cursor(response.next_cursor))
    }

    async fn close_session(
        &self,
        request: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, Error> {
        let thread_id = request.session_id.0.to_string();
        self.cancel_session_work(&request.session_id).await;

        self.app_server
            .lock()
            .await
            .thread_unsubscribe(thread_id)
            .await
            .map_err(acp_internal_error)?;

        Ok(CloseSessionResponse::new())
    }

    async fn delete_session(
        &self,
        request: DeleteSessionRequest,
    ) -> Result<DeleteSessionResponse, Error> {
        let thread_id = request.session_id.0.to_string();

        self.cancel_session_work(&request.session_id).await;

        self.app_server
            .lock()
            .await
            .thread_delete(thread_id)
            .await
            .map_err(acp_internal_error)?;

        Ok(DeleteSessionResponse::new())
    }

    async fn set_config_option(
        &self,
        request: SetSessionConfigOptionRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<SetSessionConfigOptionResponse, Error> {
        let session_id = request.session_id.clone();
        let thread_id = session_id.0.to_string();
        let config_id = request.config_id.to_string();
        let value = request.value.to_string();
        debug!(
            method = "session/set_config_option",
            thread_id, config_id, value, "handling ACP request"
        );

        match config_id.as_str() {
            MODEL_CONFIG_ID => {
                if !self
                    .is_known_config_value(&thread_id, MODEL_CONFIG_ID, &value)
                    .await
                {
                    return Err(Error::invalid_params().data(format!("unknown model `{value}`")));
                }
                self.set_model_config(&session_id, value)
                    .await
                    .map_err(|error| acp_app_server_method_error("thread/settings/update", error))?
            }
            REASONING_EFFORT_CONFIG_ID => {
                if !self
                    .is_known_config_value(&thread_id, REASONING_EFFORT_CONFIG_ID, &value)
                    .await
                {
                    return Err(
                        Error::invalid_params().data(format!("unknown reasoning effort `{value}`"))
                    );
                }
                self.set_reasoning_effort_config(&session_id, value)
                    .await
                    .map_err(|error| acp_app_server_method_error("thread/settings/update", error))?
            }
            SERVICE_TIER_CONFIG_ID => {
                if !self
                    .is_known_config_value(&thread_id, SERVICE_TIER_CONFIG_ID, &value)
                    .await
                {
                    return Err(
                        Error::invalid_params().data(format!("unknown service tier `{value}`"))
                    );
                }
                self.set_service_tier_config(&session_id, value)
                    .await
                    .map_err(|error| acp_app_server_method_error("thread/settings/update", error))?
            }
            APPROVAL_POLICY_CONFIG_ID => {
                if !self
                    .is_known_config_value(&thread_id, APPROVAL_POLICY_CONFIG_ID, &value)
                    .await
                {
                    return Err(
                        Error::invalid_params().data(format!("unknown approval policy `{value}`"))
                    );
                }
                self.set_approval_policy_config(&session_id, value)
                    .await
                    .map_err(|error| acp_app_server_method_error("thread/settings/update", error))?
            }
            COLLABORATION_MODE_CONFIG_ID => {
                if !self
                    .is_known_config_value(&thread_id, COLLABORATION_MODE_CONFIG_ID, &value)
                    .await
                {
                    return Err(Error::invalid_params()
                        .data(format!("unknown collaboration mode `{value}`")));
                }
                self.set_collaboration_mode_config(&session_id, value)
                    .await
                    .map_err(|error| acp_app_server_method_error("thread/settings/update", error))?
            }
            PERMISSION_PROFILE_CONFIG_ID => {
                if !self
                    .is_known_config_value(&thread_id, PERMISSION_PROFILE_CONFIG_ID, &value)
                    .await
                {
                    return Err(Error::invalid_params()
                        .data(format!("unknown permission profile `{value}`")));
                }
                self.set_permission_profile_config(&session_id, value)
                    .await
                    .map_err(|error| acp_app_server_method_error("thread/settings/update", error))?
            }
            _ if config_id.starts_with(SKILL_CONFIG_PREFIX) => {
                let skill_name = config_id.trim_start_matches(SKILL_CONFIG_PREFIX);
                let enabled = match value.as_str() {
                    SKILL_ENABLED_VALUE => true,
                    SKILL_DISABLED_VALUE => false,
                    _ => {
                        return Err(Error::invalid_params().data(format!(
                            "unknown skill config value `{value}`; expected `{SKILL_ENABLED_VALUE}` or `{SKILL_DISABLED_VALUE}`"
                        )));
                    }
                };
                self.set_skill_config(&session_id, skill_name, enabled, &cx)
                    .await?
            }
            _ => {
                return Err(Error::invalid_params().data(format!(
                    "unknown config option `{config_id}`; supported options are `{MODEL_CONFIG_ID}`, `{REASONING_EFFORT_CONFIG_ID}`, `{SERVICE_TIER_CONFIG_ID}`, `{APPROVAL_POLICY_CONFIG_ID}`, `{COLLABORATION_MODE_CONFIG_ID}`, `{PERMISSION_PROFILE_CONFIG_ID}`, and `{SKILL_CONFIG_PREFIX}<skill-name>`"
                )));
            }
        }

        let config_options = self.config_options_for_session(&thread_id).await;
        send_session_update(
            &cx,
            session_id,
            SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(config_options.clone())),
        )
        .map_err(acp_internal_error)?;

        Ok(SetSessionConfigOptionResponse::new(config_options))
    }

    async fn prompt(
        &self,
        request: PromptRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<PromptResponse, Error> {
        let mut input = prompt_input_blocks(request.prompt)?;
        let session_id = request.session_id.clone();
        let thread_id = request.session_id.0.to_string();
        debug!(method = "session/prompt", thread_id, "handling ACP request");

        if let Some(text) = prompt_command_text(&input) {
            if let Some(command) = parse_shell_command(text)? {
                return self
                    .run_shell_command(command, &session_id, &thread_id, &cx)
                    .await;
            }

            if let Some(command) = parse_builtin_command(text)? {
                return self
                    .run_builtin_command(command, &session_id, &thread_id, &cx)
                    .await;
            }
        } else if prompt_starts_with_command_prefix(&input) {
            return Err(Error::invalid_params()
                .data("slash and bang commands are only supported for text-only prompts"));
        }

        input = self.prompt_input(&request.session_id, input).await;
        self.run_turn_inputs(&session_id, &thread_id, input, &cx)
            .await
    }

    async fn run_turn_inputs(
        &self,
        session_id: &SessionId,
        thread_id: &str,
        input: PromptTurnInput,
        cx: &ConnectionTo<Client>,
    ) -> Result<PromptResponse, Error> {
        let (cancel_tx, cancel_rx) = oneshot::channel();
        {
            let mut active_prompts = self.active_prompts.lock().await;
            if active_prompts.contains_key(thread_id) {
                return Err(
                    Error::invalid_request().data("session already has an active prompt turn")
                );
            }
            active_prompts.insert(thread_id.to_owned(), cancel_tx);
        }

        let mut event_state = AcpEventState::default();
        let mut pending_updates = PendingAppServerUpdates::default();
        let outstanding_approvals = self.outstanding_approvals.clone();
        let completion = self
            .app_server
            .lock()
            .await
            .turn_start_until_complete_with_interactive(
                thread_id.to_owned(),
                AppServerTurnStartInput {
                    input: input.input,
                    additional_context: input.additional_context,
                },
                Some(cancel_rx),
                |event| {
                    handle_app_server_event(
                        cx,
                        session_id.to_owned(),
                        event,
                        &mut event_state,
                        &mut pending_updates,
                    )
                },
                |approval| {
                    request_permission(
                        cx,
                        session_id.to_owned(),
                        approval,
                        outstanding_approvals.clone(),
                    )
                },
                |request| request_interactive(cx, session_id.to_owned(), request),
            )
            .await
            .map_err(acp_internal_error);

        self.active_prompts.lock().await.remove(thread_id);

        let stop_reason = match completion? {
            AppServerPromptCompletion::EndTurn => StopReason::EndTurn,
            AppServerPromptCompletion::Cancelled => StopReason::Cancelled,
        };

        self.publish_pending_updates(session_id, pending_updates, cx)
            .await?;

        Ok(PromptResponse::new(stop_reason))
    }

    async fn cancel_session(&self, notification: CancelNotification) -> Result<(), Error> {
        self.cancel_session_work(&notification.session_id).await;
        Ok(())
    }

    async fn cancel_session_work(&self, session_id: &SessionId) {
        if let Some(cancel) = self
            .active_prompts
            .lock()
            .await
            .remove(session_id.0.as_ref())
        {
            let _ = cancel.send(());
        }

        cancel_outstanding_approvals_for_session(&self.outstanding_approvals, session_id).await;
    }

    async fn prompt_input(
        &self,
        session_id: &SessionId,
        mut input: PromptTurnInput,
    ) -> PromptTurnInput {
        let [AppServerTurnInput::Text { text }] = input.input.as_slice() else {
            return input;
        };
        let text = text.clone();

        let Some(cwd) = self
            .session_cwds
            .lock()
            .await
            .get(session_id.0.as_ref())
            .cloned()
        else {
            input.input = vec![AppServerTurnInput::Text { text }];
            return input;
        };

        let skills = self
            .skills_by_cwd
            .lock()
            .await
            .get(&cwd)
            .cloned()
            .unwrap_or_default();

        input.input = prompt_input_with_skills(text, &skills);
        input
    }

    async fn run_builtin_command(
        &self,
        command: BuiltinCommand,
        session_id: &SessionId,
        thread_id: &str,
        cx: &ConnectionTo<Client>,
    ) -> Result<PromptResponse, Error> {
        match command {
            BuiltinCommand::Account => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .account_read(false)
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(session_id, "Account", account_summary(&response), cx)
            }
            BuiltinCommand::Archive => {
                self.app_server
                    .lock()
                    .await
                    .thread_archive(thread_id.to_owned())
                    .await
                    .map_err(acp_internal_error)?;
                publish_session_archived_update(session_id, true, cx)
                    .map_err(acp_internal_error)?;
                Ok(PromptResponse::new(StopReason::EndTurn))
            }
            BuiltinCommand::Apps => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .app_list()
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(session_id, "Apps", catalog_summary("Apps", &response), cx)
            }
            BuiltinCommand::Config { cwd } => {
                let cwd = match cwd {
                    Some(cwd) => Some(cwd),
                    None => self.session_cwd(session_id).await,
                };
                let response = self
                    .app_server
                    .lock()
                    .await
                    .config_read(cwd.clone(), true)
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Config",
                    config_summary(cwd.as_deref(), &response),
                    cx,
                )
            }
            BuiltinCommand::ConfigRequirements => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .config_requirements_read()
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Config requirements",
                    config_requirements_summary(&response),
                    cx,
                )
            }
            BuiltinCommand::Delete => {
                self.cancel_session_work(session_id).await;
                self.app_server
                    .lock()
                    .await
                    .thread_delete(thread_id.to_owned())
                    .await
                    .map_err(acp_internal_error)?;
                publish_session_deleted_update(session_id, true, cx).map_err(acp_internal_error)?;
                publish_session_closed_update(session_id, true, cx).map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Delete",
                    format!("Deleted Codex session `{}`.", session_id.0),
                    cx,
                )
            }
            BuiltinCommand::Feature { name, enabled } => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .experimental_feature_enablement_set(name.clone(), enabled)
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Feature",
                    experimental_feature_enablement_summary(&name, enabled, &response),
                    cx,
                )
            }
            BuiltinCommand::Features => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .experimental_feature_list(thread_id.to_owned())
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Features",
                    experimental_features_summary(&response),
                    cx,
                )
            }
            BuiltinCommand::GoalSet { objective } => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .thread_goal_set(thread_id.to_owned(), Some(objective), None, None)
                    .await
                    .map_err(acp_internal_error)?;
                publish_session_goal_update(session_id, Some(response.goal), cx)
                    .map_err(acp_internal_error)?;
                Ok(PromptResponse::new(StopReason::EndTurn))
            }
            BuiltinCommand::GoalGet => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .thread_goal_get(thread_id.to_owned())
                    .await
                    .map_err(acp_internal_error)?;
                publish_session_goal_update(session_id, response.goal, cx)
                    .map_err(acp_internal_error)?;
                Ok(PromptResponse::new(StopReason::EndTurn))
            }
            BuiltinCommand::GoalClear => {
                self.app_server
                    .lock()
                    .await
                    .thread_goal_clear(thread_id.to_owned())
                    .await
                    .map_err(acp_internal_error)?;
                publish_session_goal_update(session_id, None, cx).map_err(acp_internal_error)?;
                Ok(PromptResponse::new(StopReason::EndTurn))
            }
            BuiltinCommand::Hooks => {
                let cwd = self
                    .session_cwd(session_id)
                    .await
                    .ok_or_else(|| Error::invalid_request().data("session cwd is not known yet"))?;
                let response = self
                    .app_server
                    .lock()
                    .await
                    .hooks_list(cwd)
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Hooks",
                    catalog_summary("Hooks", &response),
                    cx,
                )
            }
            BuiltinCommand::Kill { process_id } => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .thread_background_terminals_terminate(thread_id.to_owned(), process_id.clone())
                    .await
                    .map_err(|error| {
                        acp_app_server_method_error("thread/backgroundTerminals/terminate", error)
                    })?;
                let terminated = response
                    .get("terminated")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                let message = if terminated {
                    format!("Terminated background terminal process `{process_id}`.")
                } else {
                    format!("No running background terminal process `{process_id}` was found.")
                };
                publish_catalog_message(session_id, "Kill", message, cx)
            }
            BuiltinCommand::Memory { action } => match action {
                MemoryCommandAction::Enable => {
                    self.app_server
                        .lock()
                        .await
                        .thread_memory_mode_set(thread_id.to_owned(), ThreadMemoryMode::Enabled)
                        .await
                        .map_err(|error| {
                            acp_app_server_method_error("thread/memoryMode/set", error)
                        })?;
                    publish_catalog_message(
                        session_id,
                        "Memory",
                        "Codex memory is now enabled for this thread.".to_owned(),
                        cx,
                    )
                }
                MemoryCommandAction::Disable => {
                    self.app_server
                        .lock()
                        .await
                        .thread_memory_mode_set(thread_id.to_owned(), ThreadMemoryMode::Disabled)
                        .await
                        .map_err(|error| {
                            acp_app_server_method_error("thread/memoryMode/set", error)
                        })?;
                    publish_catalog_message(
                        session_id,
                        "Memory",
                        "Codex memory is now disabled for this thread.".to_owned(),
                        cx,
                    )
                }
                MemoryCommandAction::Reset => {
                    self.app_server
                        .lock()
                        .await
                        .memory_reset()
                        .await
                        .map_err(|error| acp_app_server_method_error("memory/reset", error))?;
                    publish_catalog_message(
                        session_id,
                        "Memory",
                        "Reset Codex global memory data. Thread memory modes were preserved."
                            .to_owned(),
                        cx,
                    )
                }
            },
            BuiltinCommand::Init => {
                let input = PromptTurnInput {
                    input: vec![AppServerTurnInput::Text {
                        text: init_prompt(),
                    }],
                    additional_context: None,
                };
                self.run_turn_inputs(session_id, thread_id, input, cx).await
            }
            BuiltinCommand::Mcp => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .mcp_server_status_list(thread_id.to_owned())
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(session_id, "MCP", catalog_summary("MCP", &response), cx)
            }
            BuiltinCommand::McpReload => {
                let response = {
                    let mut app_server = self.app_server.lock().await;
                    app_server
                        .mcp_server_reload()
                        .await
                        .map_err(acp_internal_error)?;
                    app_server
                        .mcp_server_status_list(thread_id.to_owned())
                        .await
                        .map_err(acp_internal_error)?
                };
                publish_catalog_message(session_id, "MCP reload", mcp_reload_summary(&response), cx)
            }
            BuiltinCommand::McpResource { server, uri } => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .mcp_server_resource_read(thread_id.to_owned(), server.clone(), uri.clone())
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "MCP resource",
                    mcp_resource_summary(&server, &uri, &response),
                    cx,
                )
            }
            BuiltinCommand::McpTool {
                server,
                tool,
                arguments,
            } => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .mcp_server_tool_call(
                        thread_id.to_owned(),
                        server.clone(),
                        tool.clone(),
                        arguments,
                    )
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "MCP tool",
                    mcp_tool_summary(&server, &tool, &response),
                    cx,
                )
            }
            BuiltinCommand::Model => {
                let config_options = self.refresh_current_config_options(session_id).await;
                publish_config_options_for_command(
                    session_id,
                    config_options,
                    "Model options refreshed. Use the `model`, `reasoning_effort`, and `service_tier` session config options to change Codex model settings.",
                    cx,
                )
            }
            BuiltinCommand::New => {
                let cwd = self
                    .session_cwd(session_id)
                    .await
                    .ok_or_else(|| Error::invalid_request().data("session cwd is not known yet"))?;
                let (new_session_id, _) = self
                    .start_thread(PathBuf::from(cwd), Vec::new(), cx)
                    .await?;
                publish_catalog_message(
                    session_id,
                    "New",
                    format!("Started a new Codex session `{}`.", new_session_id.0),
                    cx,
                )
            }
            BuiltinCommand::Fork => {
                let cwd = self
                    .session_cwd(session_id)
                    .await
                    .ok_or_else(|| Error::invalid_request().data("session cwd is not known yet"))?;
                let (forked_session_id, _) = self
                    .fork_thread(thread_id.to_owned(), PathBuf::from(cwd), Vec::new(), cx)
                    .await?;

                publish_catalog_message(
                    session_id,
                    "Fork",
                    format!(
                        "Forked this Codex thread into session `{}`.",
                        forked_session_id.0
                    ),
                    cx,
                )
            }
            BuiltinCommand::Permissions => {
                let config_options = self.refresh_current_config_options(session_id).await;
                publish_config_options_for_command(
                    session_id,
                    config_options,
                    "Permission options refreshed. Use the `permission_profile` and `approval_policy` session config options to change Codex permission behavior.",
                    cx,
                )
            }
            BuiltinCommand::Plan => {
                let thread_id = session_id.0.to_string();
                self.refresh_current_config_options(session_id).await;
                if !self
                    .is_known_config_value(&thread_id, COLLABORATION_MODE_CONFIG_ID, PLAN_COMMAND)
                    .await
                {
                    return Err(Error::invalid_request()
                        .data("Codex collaboration mode `plan` is not available"));
                }
                self.set_collaboration_mode_config(session_id, PLAN_COMMAND.to_owned())
                    .await
                    .map_err(|error| {
                        acp_app_server_method_error("thread/settings/update", error)
                    })?;
                let config_options = self.config_options_for_session(&thread_id).await;
                publish_config_options_for_command(
                    session_id,
                    config_options,
                    "Plan mode enabled for subsequent Codex turns.",
                    cx,
                )
            }
            BuiltinCommand::MarketplaceAdd {
                source,
                ref_name,
                sparse_paths,
            } => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .marketplace_add(source, ref_name, sparse_paths)
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Marketplace add",
                    marketplace_add_summary(&response),
                    cx,
                )
            }
            BuiltinCommand::MarketplaceRemove { marketplace_name } => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .marketplace_remove(marketplace_name)
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Marketplace remove",
                    marketplace_remove_summary(&response),
                    cx,
                )
            }
            BuiltinCommand::Plugin {
                marketplace_path,
                plugin_name,
            } => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .plugin_read(marketplace_path, plugin_name)
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(session_id, "Plugin", plugin_detail_summary(&response), cx)
            }
            BuiltinCommand::PluginInstall {
                marketplace_path,
                plugin_name,
            } => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .plugin_install(marketplace_path, plugin_name.clone())
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Plugin install",
                    plugin_install_summary(&plugin_name, &response),
                    cx,
                )
            }
            BuiltinCommand::PluginUninstall { plugin_id } => {
                self.app_server
                    .lock()
                    .await
                    .plugin_uninstall(plugin_id.clone())
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Plugin uninstall",
                    format!("Uninstalled Codex plugin `{plugin_id}`."),
                    cx,
                )
            }
            BuiltinCommand::Plugins => {
                let (plugins, installed) = {
                    let mut app_server = self.app_server.lock().await;
                    let plugins = app_server.plugin_list().await.map_err(acp_internal_error)?;
                    let installed = app_server
                        .plugin_installed()
                        .await
                        .map_err(acp_internal_error)?;
                    (plugins, installed)
                };
                let message = format!(
                    "{}\n\n{}",
                    catalog_summary("Plugins", &plugins),
                    catalog_summary("Installed plugins", &installed)
                );
                publish_catalog_message(session_id, "Plugins", message, cx)
            }
            BuiltinCommand::Ps => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .thread_background_terminals_list(thread_id.to_owned())
                    .await
                    .map_err(|error| {
                        acp_app_server_method_error("thread/backgroundTerminals/list", error)
                    })?;
                publish_catalog_message(
                    session_id,
                    "Background terminals",
                    catalog_summary("Background terminals", &response),
                    cx,
                )
            }
            BuiltinCommand::RateLimits => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .account_rate_limits_read()
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Rate limits",
                    rate_limits_summary(&response),
                    cx,
                )
            }
            BuiltinCommand::SkillRoots { roots } => {
                let cwd = self
                    .session_cwd(session_id)
                    .await
                    .ok_or_else(|| Error::invalid_request().data("session cwd is not known yet"))?;
                self.app_server
                    .lock()
                    .await
                    .skills_extra_roots_set(roots.clone())
                    .await
                    .map_err(|error| acp_app_server_method_error("skills/extraRoots/set", error))?;
                self.refresh_and_publish_skills(cwd, session_id, cx, true)
                    .await?;
                publish_catalog_message(session_id, "Skill roots", skill_roots_summary(&roots), cx)
            }
            BuiltinCommand::Rename { title } => {
                self.app_server
                    .lock()
                    .await
                    .thread_name_set(thread_id.to_owned(), title.clone())
                    .await
                    .map_err(acp_internal_error)?;
                publish_session_name_update(session_id, Some(title), cx)
                    .map_err(acp_internal_error)?;
                Ok(PromptResponse::new(StopReason::EndTurn))
            }
            BuiltinCommand::Stop => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .thread_background_terminals_clean(thread_id.to_owned())
                    .await
                    .map_err(|error| {
                        acp_app_server_method_error("thread/backgroundTerminals/clean", error)
                    })?;
                publish_catalog_message(
                    session_id,
                    "Background terminals",
                    catalog_summary("Background terminals cleaned", &response),
                    cx,
                )
            }
            BuiltinCommand::Unarchive => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .thread_unarchive(thread_id.to_owned())
                    .await
                    .map_err(acp_internal_error)?;
                if let Some(cwd) = response.thread.cwd {
                    self.set_session_cwd(session_id, cwd.to_string_lossy().into_owned())
                        .await;
                }
                publish_session_archived_update(session_id, false, cx)
                    .map_err(acp_internal_error)?;
                Ok(PromptResponse::new(StopReason::EndTurn))
            }
            BuiltinCommand::Usage => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .account_usage_read()
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(session_id, "Usage", account_usage_summary(&response), cx)
            }
            BuiltinCommand::WorkspaceMessages => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .account_workspace_messages_read()
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Workspace messages",
                    workspace_messages_summary(&response),
                    cx,
                )
            }
            BuiltinCommand::Resume { target } => {
                let cwd = self
                    .session_cwd(session_id)
                    .await
                    .ok_or_else(|| Error::invalid_request().data("session cwd is not known yet"))?;
                let target_thread_id = self.resolve_resume_target(&cwd, &target).await;
                let (resumed_session_id, _) = self
                    .resume_thread(target_thread_id, PathBuf::from(cwd), Vec::new(), cx)
                    .await?;
                publish_catalog_message(
                    session_id,
                    "Resume",
                    format!("Resumed Codex session `{}`.", resumed_session_id.0),
                    cx,
                )
            }
            BuiltinCommand::Rollback { num_turns } => {
                let response = self
                    .app_server
                    .lock()
                    .await
                    .thread_rollback(thread_id.to_owned(), num_turns)
                    .await
                    .map_err(|error| acp_app_server_method_error("thread/rollback", error))?;
                if let Some(cwd) = response.thread.cwd {
                    self.set_session_cwd(session_id, cwd.to_string_lossy().into_owned())
                        .await;
                }
                let turn_count = response.thread.turns.len();
                let turn_label = if num_turns == 1 { "turn" } else { "turns" };
                publish_catalog_message(
                    session_id,
                    "Rollback",
                    format!(
                        "Rolled back the last {num_turns} {turn_label}. The thread now contains {turn_count} turn(s). Local file changes made by rolled-back turns were not reverted."
                    ),
                    cx,
                )
            }
            BuiltinCommand::Compact => {
                self.run_builtin_turn_command(
                    BuiltinTurnCommand::Compact,
                    session_id,
                    thread_id,
                    cx,
                )
                .await
            }
            BuiltinCommand::Review => {
                self.run_builtin_turn_command(BuiltinTurnCommand::Review, session_id, thread_id, cx)
                    .await
            }
            BuiltinCommand::Status => {
                let cwd = self
                    .session_cwd(session_id)
                    .await
                    .unwrap_or_else(|| "<unknown>".to_owned());
                let loaded_threads = self
                    .app_server
                    .lock()
                    .await
                    .thread_loaded_list()
                    .await
                    .map_err(acp_internal_error)?;
                publish_catalog_message(
                    session_id,
                    "Status",
                    status_summary(thread_id, &cwd, &loaded_threads),
                    cx,
                )
            }
        }
    }

    async fn session_cwd(&self, session_id: &SessionId) -> Option<String> {
        self.session_cwds
            .lock()
            .await
            .get(session_id.0.as_ref())
            .cloned()
    }

    async fn set_session_additional_directories_from_runtime_roots(
        &self,
        session_id: &SessionId,
        cwd: &str,
        runtime_workspace_roots: Vec<PathBuf>,
    ) {
        let additional_directories =
            additional_directories_from_runtime_roots(Path::new(cwd), runtime_workspace_roots);
        let snapshot = {
            let mut session_additional_directories =
                self.session_additional_directories.lock().await;
            session_additional_directories.insert(session_id.0.to_string(), additional_directories);
            session_additional_directories.clone()
        };
        self.persist_session_additional_directories(&snapshot);
    }

    fn persist_session_additional_directories(&self, sessions: &HashMap<String, Vec<PathBuf>>) {
        let Some(path) = self.session_additional_directories_store.as_deref() else {
            return;
        };
        persist_session_additional_directories(path, sessions);
    }

    async fn start_thread(
        &self,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        cx: &ConnectionTo<Client>,
    ) -> Result<(SessionId, Vec<SessionConfigOption>), Error> {
        let cwd_string = cwd.to_string_lossy().into_owned();
        let runtime_workspace_roots =
            runtime_workspace_roots_for_acp_request(&cwd, &additional_directories)?;
        let response = self
            .app_server
            .lock()
            .await
            .thread_start(cwd_string, Some(runtime_workspace_roots))
            .await
            .map_err(acp_internal_error)?;

        let session_id = SessionId::new(response.thread.id);
        let mut config_options = self
            .refresh_config_options(
                &session_id,
                CurrentConfigSelections {
                    cwd: response
                        .thread
                        .cwd
                        .as_ref()
                        .map(|cwd| cwd.to_string_lossy().into_owned()),
                    model: response.model,
                    reasoning_effort: response.reasoning_effort,
                    service_tier: response.service_tier,
                    approval_policy: response.approval_policy,
                    collaboration_mode: response.collaboration_mode,
                    active_permission_profile: response.active_permission_profile,
                },
            )
            .await;
        self.replay_thread_turns(&session_id, &response.thread.turns, cx)?;
        if let Some(cwd) = response.thread.cwd {
            let cwd = cwd.to_string_lossy().into_owned();
            self.set_session_cwd(&session_id, cwd.clone()).await;
            self.set_session_additional_directories_from_runtime_roots(
                &session_id,
                &cwd,
                response.runtime_workspace_roots,
            )
            .await;
            self.refresh_and_publish_skills(cwd, &session_id, cx, false)
                .await?;
            config_options = self.config_options_for_session(session_id.0.as_ref()).await;
        }

        Ok((session_id, config_options))
    }

    async fn fork_thread(
        &self,
        source_thread_id: String,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        cx: &ConnectionTo<Client>,
    ) -> Result<(SessionId, Vec<SessionConfigOption>), Error> {
        let cwd_string = cwd.to_string_lossy().into_owned();
        let runtime_workspace_roots =
            runtime_workspace_roots_for_acp_request(&cwd, &additional_directories)?;
        let response = self
            .app_server
            .lock()
            .await
            .thread_fork(source_thread_id, cwd_string, Some(runtime_workspace_roots))
            .await
            .map_err(acp_internal_error)?;

        let session_id = SessionId::new(response.thread.id);
        let mut config_options = self
            .refresh_config_options(
                &session_id,
                CurrentConfigSelections {
                    cwd: response
                        .thread
                        .cwd
                        .as_ref()
                        .map(|cwd| cwd.to_string_lossy().into_owned()),
                    model: response.model,
                    reasoning_effort: response.reasoning_effort,
                    service_tier: response.service_tier,
                    approval_policy: response.approval_policy,
                    collaboration_mode: response.collaboration_mode,
                    active_permission_profile: response.active_permission_profile,
                },
            )
            .await;
        if let Some(cwd) = response.thread.cwd {
            let cwd = cwd.to_string_lossy().into_owned();
            self.set_session_cwd(&session_id, cwd.clone()).await;
            self.set_session_additional_directories_from_runtime_roots(
                &session_id,
                &cwd,
                response.runtime_workspace_roots,
            )
            .await;
            self.refresh_and_publish_skills(cwd, &session_id, cx, false)
                .await?;
            config_options = self.config_options_for_session(session_id.0.as_ref()).await;
        }

        Ok((session_id, config_options))
    }

    fn replay_thread_turns(
        &self,
        session_id: &SessionId,
        turns: &[crate::app_server::AppServerTurnHistory],
        cx: &ConnectionTo<Client>,
    ) -> Result<(), Error> {
        let mut event_state = AcpEventState::default();
        for event in history_events_for_turns(turns) {
            match event {
                AppServerHistoryEvent::UserMessage(text) => {
                    send_session_update(
                        cx,
                        session_id.clone(),
                        SessionUpdate::UserMessageChunk(text_chunk(text)),
                    )
                    .map_err(acp_internal_error)?;
                }
                AppServerHistoryEvent::PromptEvent(event) => {
                    send_prompt_event(cx, session_id.clone(), *event, &mut event_state)
                        .map_err(acp_internal_error)?;
                }
            }
        }
        Ok(())
    }

    async fn replay_session_history(
        &self,
        thread_id: &str,
        session_id: &SessionId,
        cx: &ConnectionTo<Client>,
    ) -> Result<(), Error> {
        let mut cursor = None;
        loop {
            let page = self
                .app_server
                .lock()
                .await
                .thread_turns_list(
                    thread_id.to_owned(),
                    cursor.clone(),
                    HISTORY_REPLAY_PAGE_SIZE,
                )
                .await;

            match page {
                Ok(page) => {
                    self.replay_thread_turns(session_id, &page.data, cx)?;
                    let next_cursor = page.next_cursor;
                    if next_cursor.is_none() {
                        return Ok(());
                    }
                    if next_cursor == cursor {
                        return Err(acp_internal_error(anyhow::anyhow!(
                            "app-server thread/turns/list returned a repeated cursor"
                        )));
                    }
                    cursor = next_cursor;
                }
                Err(error) if is_app_server_method_unavailable(&error).is_some() => {
                    debug!(
                        %error,
                        "falling back to thread/read history replay because thread/turns/list is unavailable"
                    );
                    let thread = self
                        .app_server
                        .lock()
                        .await
                        .thread_read(thread_id.to_owned())
                        .await
                        .map_err(acp_internal_error)?
                        .thread;
                    self.replay_thread_turns(session_id, &thread.turns, cx)?;
                    return Ok(());
                }
                Err(error) => return Err(acp_internal_error(error)),
            }
        }
    }

    async fn resume_thread(
        &self,
        thread_id: String,
        cwd: PathBuf,
        additional_directories: Vec<PathBuf>,
        cx: &ConnectionTo<Client>,
    ) -> Result<(SessionId, Vec<SessionConfigOption>), Error> {
        let cwd_string = cwd.to_string_lossy().into_owned();
        let runtime_workspace_roots =
            runtime_workspace_roots_for_acp_request(&cwd, &additional_directories)?;
        let response = self
            .app_server
            .lock()
            .await
            .thread_resume(thread_id, cwd_string, Some(runtime_workspace_roots))
            .await
            .map_err(acp_internal_error)?;

        let session_id = SessionId::new(response.thread.id);
        let mut config_options = self
            .refresh_config_options(
                &session_id,
                CurrentConfigSelections {
                    cwd: response
                        .thread
                        .cwd
                        .as_ref()
                        .map(|cwd| cwd.to_string_lossy().into_owned()),
                    model: response.model,
                    reasoning_effort: response.reasoning_effort,
                    service_tier: response.service_tier,
                    approval_policy: response.approval_policy,
                    collaboration_mode: response.collaboration_mode,
                    active_permission_profile: response.active_permission_profile,
                },
            )
            .await;
        if let Some(cwd) = response.thread.cwd {
            let cwd = cwd.to_string_lossy().into_owned();
            self.set_session_cwd(&session_id, cwd.clone()).await;
            self.set_session_additional_directories_from_runtime_roots(
                &session_id,
                &cwd,
                response.runtime_workspace_roots,
            )
            .await;
            self.refresh_and_publish_skills(cwd, &session_id, cx, false)
                .await?;
            config_options = self.config_options_for_session(session_id.0.as_ref()).await;
        }

        Ok((session_id, config_options))
    }

    async fn resolve_resume_target(&self, cwd: &str, target: &str) -> String {
        let response = self
            .app_server
            .lock()
            .await
            .thread_list(Some(cwd.to_owned()), None)
            .await;
        let Ok(response) = response else {
            return target.to_owned();
        };

        response
            .data
            .into_iter()
            .find(|thread| {
                thread.id == target
                    || thread.name.as_deref() == Some(target)
                    || thread.preview.as_deref() == Some(target)
            })
            .map(|thread| thread.id)
            .unwrap_or_else(|| target.to_owned())
    }

    async fn refresh_current_config_options(
        &self,
        session_id: &SessionId,
    ) -> Vec<SessionConfigOption> {
        let thread_id = session_id.0.to_string();
        let state = self
            .config_by_session
            .lock()
            .await
            .get(&thread_id)
            .cloned()
            .unwrap_or_default();
        let cwd = self.session_cwd(session_id).await;
        let current_collaboration_mode =
            state
                .current_collaboration_mode
                .as_ref()
                .map(|mode| AppServerCollaborationMode {
                    mode: mode.clone(),
                    settings: AppServerCollaborationModeSettings {
                        model: state.current_model.clone().unwrap_or_default(),
                        reasoning_effort: state.current_reasoning_effort.clone(),
                        developer_instructions: None,
                    },
                });
        let active_permission_profile =
            state
                .current_permission_profile
                .as_ref()
                .map(|id| AppServerActivePermissionProfile {
                    id: id.clone(),
                    extends: None,
                });

        self.refresh_config_options(
            session_id,
            CurrentConfigSelections {
                cwd,
                model: state.current_model,
                reasoning_effort: state.current_reasoning_effort,
                service_tier: state
                    .current_service_tier
                    .filter(|tier| tier != DEFAULT_SERVICE_TIER_VALUE),
                approval_policy: state.current_approval_policy,
                collaboration_mode: current_collaboration_mode,
                active_permission_profile,
            },
        )
        .await
    }

    async fn run_builtin_turn_command(
        &self,
        command: BuiltinTurnCommand,
        session_id: &SessionId,
        thread_id: &str,
        cx: &ConnectionTo<Client>,
    ) -> Result<PromptResponse, Error> {
        let (cancel_tx, cancel_rx) = oneshot::channel();
        if self
            .active_prompts
            .lock()
            .await
            .insert(thread_id.to_owned(), cancel_tx)
            .is_some()
        {
            return Err(Error::invalid_request().data("session already has an active prompt turn"));
        }

        let mut event_state = AcpEventState::default();
        let mut pending_updates = PendingAppServerUpdates::default();
        let outstanding_approvals = self.outstanding_approvals.clone();
        let completion = match command {
            BuiltinTurnCommand::Compact => {
                self.app_server
                    .lock()
                    .await
                    .thread_compact_start_until_complete(
                        thread_id.to_owned(),
                        Some(cancel_rx),
                        |event| {
                            handle_app_server_event(
                                cx,
                                session_id.clone(),
                                event,
                                &mut event_state,
                                &mut pending_updates,
                            )
                        },
                        |approval| {
                            request_permission(
                                cx,
                                session_id.clone(),
                                approval,
                                outstanding_approvals.clone(),
                            )
                        },
                    )
                    .await
            }
            BuiltinTurnCommand::Review => {
                self.app_server
                    .lock()
                    .await
                    .review_start_until_complete(
                        thread_id.to_owned(),
                        Some(cancel_rx),
                        |event| {
                            handle_app_server_event(
                                cx,
                                session_id.clone(),
                                event,
                                &mut event_state,
                                &mut pending_updates,
                            )
                        },
                        |approval| {
                            request_permission(
                                cx,
                                session_id.clone(),
                                approval,
                                outstanding_approvals.clone(),
                            )
                        },
                    )
                    .await
            }
        }
        .map_err(acp_internal_error);

        self.active_prompts.lock().await.remove(thread_id);

        let stop_reason = match completion? {
            AppServerPromptCompletion::EndTurn => StopReason::EndTurn,
            AppServerPromptCompletion::Cancelled => StopReason::Cancelled,
        };

        self.publish_pending_updates(session_id, pending_updates, cx)
            .await?;

        Ok(PromptResponse::new(stop_reason))
    }

    async fn run_shell_command(
        &self,
        command: String,
        session_id: &SessionId,
        thread_id: &str,
        cx: &ConnectionTo<Client>,
    ) -> Result<PromptResponse, Error> {
        let (cancel_tx, cancel_rx) = oneshot::channel();
        if self
            .active_prompts
            .lock()
            .await
            .insert(thread_id.to_owned(), cancel_tx)
            .is_some()
        {
            return Err(Error::invalid_request().data("session already has an active prompt turn"));
        }

        let mut event_state = AcpEventState::default();
        let mut pending_updates = PendingAppServerUpdates::default();
        let outstanding_approvals = self.outstanding_approvals.clone();
        let completion = self
            .app_server
            .lock()
            .await
            .thread_shell_command_until_complete(
                thread_id.to_owned(),
                command,
                Some(cancel_rx),
                |event| {
                    handle_app_server_event(
                        cx,
                        session_id.clone(),
                        event,
                        &mut event_state,
                        &mut pending_updates,
                    )
                },
                |approval| {
                    request_permission(
                        cx,
                        session_id.clone(),
                        approval,
                        outstanding_approvals.clone(),
                    )
                },
            )
            .await
            .map_err(acp_internal_error);

        self.active_prompts.lock().await.remove(thread_id);

        let stop_reason = match completion? {
            AppServerPromptCompletion::EndTurn => StopReason::EndTurn,
            AppServerPromptCompletion::Cancelled => StopReason::Cancelled,
        };

        self.publish_pending_updates(session_id, pending_updates, cx)
            .await?;

        Ok(PromptResponse::new(stop_reason))
    }

    async fn set_session_cwd(&self, session_id: &SessionId, cwd: String) {
        self.session_cwds
            .lock()
            .await
            .insert(session_id.0.to_string(), cwd);
    }

    async fn refresh_and_publish_skills(
        &self,
        cwd: String,
        session_id: &SessionId,
        cx: &ConnectionTo<Client>,
        force_reload: bool,
    ) -> Result<(), Error> {
        let skills_response = match self
            .app_server
            .lock()
            .await
            .skills_list(cwd.clone(), force_reload)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                debug!(%cwd, %error, "failed to refresh Codex skills");
                return send_available_commands_update(cx, session_id.clone(), builtin_commands());
            }
        };

        let skills = skills_response
            .data
            .into_iter()
            .find(|entry| entry.cwd == cwd)
            .map(|entry| entry.skills)
            .unwrap_or_default();

        self.skills_by_cwd.lock().await.insert(cwd, skills.clone());

        send_available_commands_update(cx, session_id.clone(), available_commands(skills))?;
        let config_options = self.config_options_for_session(session_id.0.as_ref()).await;
        send_session_update(
            cx,
            session_id.clone(),
            SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(config_options)),
        )
        .map_err(acp_internal_error)
    }

    async fn publish_pending_updates(
        &self,
        session_id: &SessionId,
        pending: PendingAppServerUpdates,
        cx: &ConnectionTo<Client>,
    ) -> Result<(), Error> {
        if pending.skills_changed
            && let Some(cwd) = self
                .session_cwds
                .lock()
                .await
                .get(session_id.0.as_ref())
                .cloned()
        {
            self.refresh_and_publish_skills(cwd, session_id, cx, true)
                .await?;
        }

        if let Some(settings) = pending.thread_settings {
            self.publish_thread_settings_update(session_id, settings, cx)
                .await?;
        }

        Ok(())
    }

    async fn publish_thread_settings_update(
        &self,
        session_id: &SessionId,
        settings: AppServerThreadSettingsUpdate,
        cx: &ConnectionTo<Client>,
    ) -> Result<(), Error> {
        if let Some(cwd) = settings.cwd.clone() {
            self.set_session_cwd(session_id, cwd).await;
        }
        let config_options = self
            .refresh_config_options(
                session_id,
                CurrentConfigSelections {
                    cwd: settings.cwd,
                    model: settings.model,
                    reasoning_effort: settings.reasoning_effort,
                    service_tier: settings.service_tier,
                    approval_policy: settings.approval_policy,
                    collaboration_mode: settings.collaboration_mode,
                    active_permission_profile: settings.active_permission_profile,
                },
            )
            .await;
        send_session_update(
            cx,
            session_id.clone(),
            SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(config_options)),
        )
        .map_err(acp_internal_error)
    }

    async fn refresh_config_options(
        &self,
        session_id: &SessionId,
        current: CurrentConfigSelections,
    ) -> Vec<SessionConfigOption> {
        let (models, collaboration_modes, permission_profiles) = {
            let mut app_server = self.app_server.lock().await;
            let models = match app_server.model_list().await {
                Ok(response) => response.data,
                Err(error) => {
                    debug!(%error, "failed to refresh Codex model catalog");
                    Vec::new()
                }
            };
            let collaboration_modes = match app_server.collaboration_mode_list().await {
                Ok(response) => response.data,
                Err(error) => {
                    debug!(%error, "failed to refresh Codex collaboration mode catalog");
                    Vec::new()
                }
            };
            let permission_profiles = match app_server.permission_profile_list(current.cwd).await {
                Ok(response) => response.data,
                Err(error) => {
                    debug!(%error, "failed to refresh Codex permission profile catalog");
                    Vec::new()
                }
            };
            (models, collaboration_modes, permission_profiles)
        };

        let selected_model = current.model.or_else(|| default_model_id(&models));
        let selected_model_catalog = selected_model
            .as_deref()
            .and_then(|model_id| models.iter().find(|model| model.id == model_id));
        let state = AcpConfigState {
            current_model: selected_model,
            current_reasoning_effort: current
                .reasoning_effort
                .or_else(|| selected_model_catalog.and_then(default_reasoning_effort_id)),
            current_service_tier: current
                .service_tier
                .or_else(|| Some(DEFAULT_SERVICE_TIER_VALUE.to_owned())),
            current_approval_policy: current
                .approval_policy
                .or_else(|| Some(DEFAULT_APPROVAL_POLICY.to_owned())),
            current_collaboration_mode: current
                .collaboration_mode
                .map(|mode| mode.mode)
                .or_else(|| default_collaboration_mode_id(&collaboration_modes)),
            current_permission_profile: current
                .active_permission_profile
                .map(|profile| profile.id)
                .or_else(|| default_permission_profile_id(&permission_profiles)),
            models,
            collaboration_modes,
            permission_profiles,
        };
        let options = config_options(&state);
        self.config_by_session
            .lock()
            .await
            .insert(session_id.0.to_string(), state);
        options
    }

    async fn set_model_config(&self, session_id: &SessionId, model: String) -> anyhow::Result<()> {
        let thread_id = session_id.0.to_string();
        let mut state = self
            .config_by_session
            .lock()
            .await
            .get(&thread_id)
            .cloned()
            .unwrap_or_default();

        self.app_server
            .lock()
            .await
            .thread_settings_update(
                ThreadSettingsUpdateParams::new(thread_id.clone()).with_model(model.clone()),
            )
            .await?;

        if let Some(model_catalog) = state.models.iter().find(|item| item.id == model) {
            state.current_reasoning_effort = default_reasoning_effort_id(model_catalog);
            state.current_service_tier = Some(DEFAULT_SERVICE_TIER_VALUE.to_owned());
        }
        state.current_model = Some(model);
        self.config_by_session.lock().await.insert(thread_id, state);
        Ok(())
    }

    async fn set_reasoning_effort_config(
        &self,
        session_id: &SessionId,
        effort: String,
    ) -> anyhow::Result<()> {
        let thread_id = session_id.0.to_string();
        let mut state = self
            .config_by_session
            .lock()
            .await
            .get(&thread_id)
            .cloned()
            .unwrap_or_default();

        self.app_server
            .lock()
            .await
            .thread_settings_update(
                ThreadSettingsUpdateParams::new(thread_id.clone()).with_effort(effort.clone()),
            )
            .await?;

        state.current_reasoning_effort = Some(effort);
        self.config_by_session.lock().await.insert(thread_id, state);
        Ok(())
    }

    async fn set_service_tier_config(
        &self,
        session_id: &SessionId,
        service_tier: String,
    ) -> anyhow::Result<()> {
        let thread_id = session_id.0.to_string();
        let mut state = self
            .config_by_session
            .lock()
            .await
            .get(&thread_id)
            .cloned()
            .unwrap_or_default();
        let selected = (service_tier != DEFAULT_SERVICE_TIER_VALUE).then_some(service_tier.clone());

        self.app_server
            .lock()
            .await
            .thread_settings_update(
                ThreadSettingsUpdateParams::new(thread_id.clone())
                    .with_service_tier(selected.clone()),
            )
            .await?;

        state.current_service_tier = selected.or(Some(DEFAULT_SERVICE_TIER_VALUE.to_owned()));
        self.config_by_session.lock().await.insert(thread_id, state);
        Ok(())
    }

    async fn set_approval_policy_config(
        &self,
        session_id: &SessionId,
        approval_policy: String,
    ) -> anyhow::Result<()> {
        let thread_id = session_id.0.to_string();
        let mut state = self
            .config_by_session
            .lock()
            .await
            .get(&thread_id)
            .cloned()
            .unwrap_or_default();

        self.app_server
            .lock()
            .await
            .thread_settings_update(
                ThreadSettingsUpdateParams::new(thread_id.clone())
                    .with_approval_policy(approval_policy.clone()),
            )
            .await?;

        state.current_approval_policy = Some(approval_policy);
        self.config_by_session.lock().await.insert(thread_id, state);
        Ok(())
    }

    async fn set_collaboration_mode_config(
        &self,
        session_id: &SessionId,
        collaboration_mode: String,
    ) -> anyhow::Result<()> {
        let thread_id = session_id.0.to_string();
        let mut state = self
            .config_by_session
            .lock()
            .await
            .get(&thread_id)
            .cloned()
            .unwrap_or_default();

        let Some(mode) = collaboration_mode_for_config(&state, &collaboration_mode) else {
            anyhow::bail!("unknown collaboration mode `{collaboration_mode}`");
        };

        self.app_server
            .lock()
            .await
            .thread_settings_update(
                ThreadSettingsUpdateParams::new(thread_id.clone()).with_collaboration_mode(mode),
            )
            .await?;

        state.current_collaboration_mode = Some(collaboration_mode);
        self.config_by_session.lock().await.insert(thread_id, state);
        Ok(())
    }

    async fn set_permission_profile_config(
        &self,
        session_id: &SessionId,
        permission_profile: String,
    ) -> anyhow::Result<()> {
        let thread_id = session_id.0.to_string();
        let mut state = self
            .config_by_session
            .lock()
            .await
            .get(&thread_id)
            .cloned()
            .unwrap_or_default();

        self.app_server
            .lock()
            .await
            .thread_settings_update(
                ThreadSettingsUpdateParams::new(thread_id.clone())
                    .with_permissions(permission_profile.clone()),
            )
            .await?;

        state.current_permission_profile = Some(permission_profile);
        self.config_by_session.lock().await.insert(thread_id, state);
        Ok(())
    }

    async fn set_skill_config(
        &self,
        session_id: &SessionId,
        skill_name: &str,
        enabled: bool,
        cx: &ConnectionTo<Client>,
    ) -> Result<(), Error> {
        let cwd = self
            .session_cwd(session_id)
            .await
            .ok_or_else(|| Error::invalid_request().data("session cwd is not known yet"))?;
        let skill = self
            .skills_by_cwd
            .lock()
            .await
            .get(&cwd)
            .and_then(|skills| {
                skills
                    .iter()
                    .find(|skill| skill.name == skill_name)
                    .cloned()
            })
            .ok_or_else(|| Error::invalid_params().data(format!("unknown skill `{skill_name}`")))?;

        self.app_server
            .lock()
            .await
            .skills_config_write(Some(skill.name), skill.path, enabled)
            .await
            .map_err(acp_internal_error)?;
        self.refresh_and_publish_skills(cwd, session_id, cx, true)
            .await
    }

    async fn config_options_for_session(&self, thread_id: &str) -> Vec<SessionConfigOption> {
        let mut options = self
            .config_by_session
            .lock()
            .await
            .get(thread_id)
            .map(config_options)
            .unwrap_or_default();
        if let Some(cwd) = self.session_cwds.lock().await.get(thread_id).cloned() {
            let skills = self
                .skills_by_cwd
                .lock()
                .await
                .get(&cwd)
                .cloned()
                .unwrap_or_default();
            options.extend(skill_config_options(&skills));
        }
        options
    }

    async fn is_known_config_value(&self, thread_id: &str, config_id: &str, value: &str) -> bool {
        let Some(state) = self.config_by_session.lock().await.get(thread_id).cloned() else {
            return true;
        };

        match config_id {
            MODEL_CONFIG_ID => {
                state.models.is_empty() || state.models.iter().any(|model| model.id == value)
            }
            REASONING_EFFORT_CONFIG_ID => active_model_catalog(&state).is_none_or(|model| {
                model.supported_reasoning_efforts.is_empty()
                    || model
                        .supported_reasoning_efforts
                        .iter()
                        .any(|effort| effort.reasoning_effort == value)
            }),
            SERVICE_TIER_CONFIG_ID => {
                value == DEFAULT_SERVICE_TIER_VALUE
                    || active_model_catalog(&state).is_none_or(|model| {
                        model.service_tiers.is_empty()
                            || model.service_tiers.iter().any(|tier| tier.id == value)
                    })
            }
            APPROVAL_POLICY_CONFIG_ID => is_known_approval_policy(value),
            COLLABORATION_MODE_CONFIG_ID => {
                state.collaboration_modes.is_empty()
                    || state
                        .collaboration_modes
                        .iter()
                        .filter_map(collaboration_mode_id)
                        .any(|mode| mode == value)
            }
            PERMISSION_PROFILE_CONFIG_ID => {
                state.permission_profiles.is_empty()
                    || state
                        .permission_profiles
                        .iter()
                        .any(|profile| profile.id == value)
            }
            _ => false,
        }
    }

    fn capabilities() -> AgentCapabilities {
        AgentCapabilities::new()
            .load_session(true)
            .prompt_capabilities(PromptCapabilities::new().image(true).embedded_context(true))
            .session_capabilities(
                // Fork is exposed by the Rust ACP crate behind its unstable
                // RFD/extension feature; it is not stable ACP v1 behavior.
                SessionCapabilities::new()
                    .list(SessionListCapabilities::new())
                    .resume(SessionResumeCapabilities::new())
                    .close(SessionCloseCapabilities::new())
                    .delete(SessionDeleteCapabilities::new())
                    .additional_directories(SessionAdditionalDirectoriesCapabilities::new())
                    .fork(SessionForkCapabilities::new()),
            )
    }
}

fn acp_internal_error(error: anyhow::Error) -> Error {
    Error::internal_error().data(error.to_string())
}

fn acp_app_server_method_error(method: &str, error: anyhow::Error) -> Error {
    if is_app_server_method_unavailable(&error).is_some() {
        Error::invalid_request().data(format!(
            "Codex app-server method `{method}` is unavailable in this Codex version"
        ))
    } else {
        acp_internal_error(error)
    }
}

#[derive(Debug, Default)]
struct PromptTurnInput {
    input: Vec<AppServerTurnInput>,
    additional_context: Option<AppServerAdditionalContext>,
}

fn prompt_input_blocks(prompt: Vec<ContentBlock>) -> Result<PromptTurnInput, Error> {
    let mut input = Vec::new();
    let mut additional_context = AppServerAdditionalContext::new();
    let mut text = String::new();

    fn append_text(text: &mut String, value: &str) {
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(value);
    }

    fn flush_text(input: &mut Vec<AppServerTurnInput>, text: &mut String) {
        if !text.trim().is_empty() {
            input.push(AppServerTurnInput::Text {
                text: std::mem::take(text),
            });
        } else {
            text.clear();
        }
    }

    for block in prompt {
        match block {
            ContentBlock::Text(text_content) => {
                append_text(&mut text, &text_content.text);
            }
            ContentBlock::ResourceLink(resource) => {
                append_text(&mut text, &format!("@{}", resource.uri));
            }
            ContentBlock::Resource(resource) => {
                let (uri, value) = embedded_resource_context(resource.resource)?;
                append_text(&mut text, &format!("@{uri}"));
                let key = unique_additional_context_key(&additional_context, &uri);
                additional_context.insert(
                    key,
                    AppServerAdditionalContextEntry {
                        value,
                        kind: AppServerAdditionalContextKind::Untrusted,
                    },
                );
            }
            ContentBlock::Image(image) => {
                flush_text(&mut input, &mut text);
                let url = if let Some(uri) = image.uri.filter(|uri| !uri.trim().is_empty()) {
                    uri
                } else if !image.data.trim().is_empty() {
                    format!("data:{};base64,{}", image.mime_type, image.data)
                } else {
                    return Err(
                        Error::invalid_params().data("image prompt blocks require data or uri")
                    );
                };
                input.push(AppServerTurnInput::Image { url });
            }
            _ => {
                return Err(Error::invalid_params().data(
                    "only text, resource link, embedded resource, and image prompt blocks are supported so far",
                ));
            }
        }
    }

    flush_text(&mut input, &mut text);

    if input.is_empty() {
        return Err(Error::invalid_params().data("prompt cannot be empty"));
    }

    Ok(PromptTurnInput {
        input,
        additional_context: (!additional_context.is_empty()).then_some(additional_context),
    })
}

fn embedded_resource_context(
    resource: EmbeddedResourceResource,
) -> Result<(String, String), Error> {
    match resource {
        EmbeddedResourceResource::TextResourceContents(resource) => {
            let mut value = String::new();
            if let Some(mime_type) = resource.mime_type {
                value.push_str(&format!("MIME type: {mime_type}\n\n"));
            }
            value.push_str(&resource.text);
            Ok((resource.uri, value))
        }
        EmbeddedResourceResource::BlobResourceContents(resource) => {
            let mut value = String::new();
            if let Some(mime_type) = resource.mime_type {
                value.push_str(&format!("MIME type: {mime_type}\n"));
            }
            value.push_str("Encoding: base64\n\n");
            value.push_str(&resource.blob);
            Ok((resource.uri, value))
        }
        _ => Err(Error::invalid_params().data("unsupported embedded resource prompt block")),
    }
}

fn unique_additional_context_key(
    additional_context: &AppServerAdditionalContext,
    uri: &str,
) -> String {
    let base = if uri.trim().is_empty() {
        "embedded-resource".to_owned()
    } else {
        uri.to_owned()
    };

    if !additional_context.contains_key(&base) {
        return base;
    }

    for index in 2.. {
        let candidate = format!("{base}#{index}");
        if !additional_context.contains_key(&candidate) {
            return candidate;
        }
    }

    unreachable!("unbounded iterator should always find a unique context key")
}

fn prompt_command_text(input: &PromptTurnInput) -> Option<&str> {
    match (input.input.as_slice(), input.additional_context.as_ref()) {
        ([AppServerTurnInput::Text { text }], None) => Some(text),
        _ => None,
    }
}

fn prompt_starts_with_command_prefix(input: &PromptTurnInput) -> bool {
    matches!(
        input.input.first(),
        Some(AppServerTurnInput::Text { text }) if text.trim_start().starts_with(['/', '!'])
    )
}

fn prompt_input_with_skills(text: String, skills: &[AppServerSkill]) -> Vec<AppServerTurnInput> {
    let Some(invocation) = parse_skill_invocation(&text) else {
        return vec![AppServerTurnInput::Text { text }];
    };

    let Some(skill) = skills
        .iter()
        .find(|skill| skill.enabled && skill.name == invocation.name)
    else {
        return vec![AppServerTurnInput::Text { text }];
    };

    let Some(path) = skill.path.clone() else {
        return vec![AppServerTurnInput::Text { text }];
    };

    vec![
        AppServerTurnInput::Text {
            text: invocation.visible_text,
        },
        AppServerTurnInput::Skill {
            name: skill.name.clone(),
            path,
        },
    ]
}

struct SkillInvocation {
    name: String,
    visible_text: String,
}

#[derive(Debug)]
enum BuiltinCommand {
    Account,
    Archive,
    Apps,
    Compact,
    Config {
        cwd: Option<String>,
    },
    ConfigRequirements,
    Delete,
    Feature {
        name: String,
        enabled: bool,
    },
    Features,
    Fork,
    GoalSet {
        objective: String,
    },
    GoalGet,
    GoalClear,
    Hooks,
    Kill {
        process_id: String,
    },
    MarketplaceAdd {
        source: String,
        ref_name: Option<String>,
        sparse_paths: Option<Vec<String>>,
    },
    MarketplaceRemove {
        marketplace_name: String,
    },
    Memory {
        action: MemoryCommandAction,
    },
    Init,
    Mcp,
    McpReload,
    McpResource {
        server: String,
        uri: String,
    },
    McpTool {
        server: String,
        tool: String,
        arguments: serde_json::Value,
    },
    Model,
    New,
    Permissions,
    Plan,
    Plugin {
        marketplace_path: String,
        plugin_name: String,
    },
    PluginInstall {
        marketplace_path: String,
        plugin_name: String,
    },
    PluginUninstall {
        plugin_id: String,
    },
    Plugins,
    Ps,
    RateLimits,
    Rename {
        title: String,
    },
    Resume {
        target: String,
    },
    Review,
    Rollback {
        num_turns: u32,
    },
    SkillRoots {
        roots: Vec<String>,
    },
    Status,
    Stop,
    Unarchive,
    Usage,
    WorkspaceMessages,
}

#[derive(Debug, Clone, Copy)]
enum BuiltinTurnCommand {
    Compact,
    Review,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemoryCommandAction {
    Enable,
    Disable,
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandAvailability {
    RequiresSession,
    RequiresNoActiveTurn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandHandler {
    Account,
    Archive,
    Apps,
    Compact,
    Config,
    ConfigRequirements,
    Delete,
    Feature,
    Features,
    Fork,
    Goal,
    Hooks,
    Kill,
    MarketplaceAdd,
    MarketplaceRemove,
    Memory,
    Init,
    Mcp,
    McpReload,
    McpResource,
    McpTool,
    Model,
    New,
    Permissions,
    Plan,
    Plugin,
    PluginInstall,
    PluginUninstall,
    Plugins,
    Ps,
    RateLimits,
    Rename,
    Resume,
    Review,
    Rollback,
    SkillRoots,
    Status,
    Stop,
    Unarchive,
    Usage,
    WorkspaceMessages,
}

#[derive(Debug)]
struct BuiltinCommandSpec {
    name: &'static str,
    aliases: &'static [&'static str],
    description: &'static str,
    input_hint: Option<&'static str>,
    availability: CommandAvailability,
    handler: CommandHandler,
}

const BUILTIN_COMMAND_SPECS: &[BuiltinCommandSpec] = &[
    BuiltinCommandSpec {
        name: ACCOUNT_COMMAND,
        aliases: &[],
        description: "Show Codex account status",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Account,
    },
    BuiltinCommandSpec {
        name: ARCHIVE_COMMAND,
        aliases: &[],
        description: "Archive this Codex thread",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Archive,
    },
    BuiltinCommandSpec {
        name: APPS_COMMAND,
        aliases: &[],
        description: "List available Codex apps",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Apps,
    },
    BuiltinCommandSpec {
        name: COMPACT_COMMAND,
        aliases: &[],
        description: "Compact this Codex thread",
        input_hint: None,
        availability: CommandAvailability::RequiresNoActiveTurn,
        handler: CommandHandler::Compact,
    },
    BuiltinCommandSpec {
        name: CONFIG_COMMAND,
        aliases: &[],
        description: "Show effective Codex configuration",
        input_hint: Some("optional cwd"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Config,
    },
    BuiltinCommandSpec {
        name: CONFIG_REQUIREMENTS_COMMAND,
        aliases: &[],
        description: "Show managed Codex configuration requirements",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::ConfigRequirements,
    },
    BuiltinCommandSpec {
        name: DELETE_COMMAND,
        aliases: &[],
        description: "Delete this Codex session",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Delete,
    },
    BuiltinCommandSpec {
        name: FEATURE_COMMAND,
        aliases: &[],
        description: "Enable or disable a Codex experimental feature flag",
        input_hint: Some("name enable|disable"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Feature,
    },
    BuiltinCommandSpec {
        name: FEATURES_COMMAND,
        aliases: &[],
        description: "List Codex experimental feature flags",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Features,
    },
    BuiltinCommandSpec {
        name: FORK_COMMAND,
        aliases: &[],
        description: "Fork this Codex thread",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Fork,
    },
    BuiltinCommandSpec {
        name: GOAL_COMMAND,
        aliases: &[],
        description: "Show, set, or clear this thread goal",
        input_hint: Some("objective, get, or clear"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Goal,
    },
    BuiltinCommandSpec {
        name: HOOKS_COMMAND,
        aliases: &[],
        description: "List configured Codex hooks",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Hooks,
    },
    BuiltinCommandSpec {
        name: KILL_COMMAND,
        aliases: &[],
        description: "Terminate a Codex background terminal",
        input_hint: Some("background terminal process id"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Kill,
    },
    BuiltinCommandSpec {
        name: MARKETPLACE_ADD_COMMAND,
        aliases: &[],
        description: "Add a Codex plugin marketplace",
        input_hint: Some("source [ref] [sparsePathsCsv]"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::MarketplaceAdd,
    },
    BuiltinCommandSpec {
        name: MARKETPLACE_REMOVE_COMMAND,
        aliases: &[],
        description: "Remove a Codex plugin marketplace",
        input_hint: Some("marketplaceName"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::MarketplaceRemove,
    },
    BuiltinCommandSpec {
        name: MEMORY_COMMAND,
        aliases: &[],
        description: "Set or reset Codex memory for this thread",
        input_hint: Some("enable, disable, or reset"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Memory,
    },
    BuiltinCommandSpec {
        name: INIT_COMMAND,
        aliases: &[],
        description: "Create or update AGENTS.md",
        input_hint: None,
        availability: CommandAvailability::RequiresNoActiveTurn,
        handler: CommandHandler::Init,
    },
    BuiltinCommandSpec {
        name: MCP_COMMAND,
        aliases: &[],
        description: "List configured MCP servers",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Mcp,
    },
    BuiltinCommandSpec {
        name: MCP_RELOAD_COMMAND,
        aliases: &[],
        description: "Reload Codex MCP server configuration",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::McpReload,
    },
    BuiltinCommandSpec {
        name: MCP_RESOURCE_COMMAND,
        aliases: &[],
        description: "Read a configured MCP server resource",
        input_hint: Some("server uri"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::McpResource,
    },
    BuiltinCommandSpec {
        name: MCP_TOOL_COMMAND,
        aliases: &[],
        description: "Call a configured MCP server tool",
        input_hint: Some("server tool [json arguments]"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::McpTool,
    },
    BuiltinCommandSpec {
        name: MODEL_COMMAND,
        aliases: &[],
        description: "Refresh Codex model options",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Model,
    },
    BuiltinCommandSpec {
        name: NEW_COMMAND,
        aliases: &[],
        description: "Start a new Codex session",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::New,
    },
    BuiltinCommandSpec {
        name: PERMISSIONS_COMMAND,
        aliases: &[],
        description: "Refresh Codex permission options",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Permissions,
    },
    BuiltinCommandSpec {
        name: PLAN_COMMAND,
        aliases: &[],
        description: "Switch subsequent Codex turns into plan mode",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Plan,
    },
    BuiltinCommandSpec {
        name: PLUGIN_COMMAND,
        aliases: &[],
        description: "Read Codex plugin details",
        input_hint: Some("pluginName@marketplacePath"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Plugin,
    },
    BuiltinCommandSpec {
        name: PLUGIN_INSTALL_COMMAND,
        aliases: &[],
        description: "Install a Codex plugin",
        input_hint: Some("pluginName@marketplacePath"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::PluginInstall,
    },
    BuiltinCommandSpec {
        name: PLUGIN_UNINSTALL_COMMAND,
        aliases: &[],
        description: "Uninstall a Codex plugin",
        input_hint: Some("pluginId"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::PluginUninstall,
    },
    BuiltinCommandSpec {
        name: PLUGINS_COMMAND,
        aliases: &[],
        description: "List available and installed Codex plugins",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Plugins,
    },
    BuiltinCommandSpec {
        name: PS_COMMAND,
        aliases: &[],
        description: "List Codex background terminals",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Ps,
    },
    BuiltinCommandSpec {
        name: RATE_LIMITS_COMMAND,
        aliases: &[],
        description: "Show Codex account rate limits",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::RateLimits,
    },
    BuiltinCommandSpec {
        name: RENAME_COMMAND,
        aliases: &[],
        description: "Rename this Codex thread",
        input_hint: Some("new thread title"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Rename,
    },
    BuiltinCommandSpec {
        name: RESUME_COMMAND,
        aliases: &[],
        description: "Resume a Codex session",
        input_hint: Some("thread id or name"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Resume,
    },
    BuiltinCommandSpec {
        name: REVIEW_COMMAND,
        aliases: &[],
        description: "Run Codex review for this thread",
        input_hint: None,
        availability: CommandAvailability::RequiresNoActiveTurn,
        handler: CommandHandler::Review,
    },
    BuiltinCommandSpec {
        name: ROLLBACK_COMMAND,
        aliases: &[],
        description: "Rollback recent Codex thread turns",
        input_hint: Some("number of turns"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Rollback,
    },
    BuiltinCommandSpec {
        name: SKILL_ROOTS_COMMAND,
        aliases: &[],
        description: "Set process-local Codex extra skill roots",
        input_hint: Some("absolute skill root paths"),
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::SkillRoots,
    },
    BuiltinCommandSpec {
        name: STATUS_COMMAND,
        aliases: &[],
        description: "Show Codex thread status",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Status,
    },
    BuiltinCommandSpec {
        name: STOP_COMMAND,
        aliases: &[],
        description: "Clean Codex background terminals",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Stop,
    },
    BuiltinCommandSpec {
        name: UNARCHIVE_COMMAND,
        aliases: &[],
        description: "Unarchive this Codex thread",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Unarchive,
    },
    BuiltinCommandSpec {
        name: USAGE_COMMAND,
        aliases: &[],
        description: "Show Codex account usage",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::Usage,
    },
    BuiltinCommandSpec {
        name: WORKSPACE_MESSAGES_COMMAND,
        aliases: &[],
        description: "Show active Codex workspace messages",
        input_hint: None,
        availability: CommandAvailability::RequiresSession,
        handler: CommandHandler::WorkspaceMessages,
    },
];

fn parse_builtin_command(text: &str) -> Result<Option<BuiltinCommand>, Error> {
    let text = text.trim_start();
    let Some(stripped) = text.strip_prefix('/') else {
        return Ok(None);
    };
    let Some((command, rest)) = split_first_token(stripped) else {
        return Ok(None);
    };

    if command == SKILL_COMMAND {
        return Ok(None);
    }

    let Some(spec) = builtin_command_spec(command) else {
        return Err(Error::invalid_params().data(format!("unsupported slash command `/{command}`")));
    };

    parse_command_from_spec(spec, rest)
}

fn parse_shell_command(text: &str) -> Result<Option<String>, Error> {
    let text = text.trim_start();
    let Some(command) = text.strip_prefix('!') else {
        return Ok(None);
    };

    let command = command.trim();
    if command.is_empty() {
        return Err(Error::invalid_params().data("! shell command requires a command"));
    }

    Ok(Some(command.to_owned()))
}

fn builtin_command_spec(command: &str) -> Option<&'static BuiltinCommandSpec> {
    BUILTIN_COMMAND_SPECS
        .iter()
        .find(|spec| spec.name == command || spec.aliases.contains(&command))
}

fn parse_command_from_spec(
    spec: &BuiltinCommandSpec,
    rest: &str,
) -> Result<Option<BuiltinCommand>, Error> {
    match spec.handler {
        CommandHandler::Account => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Account)
        }
        CommandHandler::Archive => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Archive)
        }
        CommandHandler::Apps => parse_no_argument_command(rest, spec.name, BuiltinCommand::Apps),
        CommandHandler::Compact => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Compact)
        }
        CommandHandler::Config => parse_config_command(rest),
        CommandHandler::ConfigRequirements => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::ConfigRequirements)
        }
        CommandHandler::Delete => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Delete)
        }
        CommandHandler::Feature => parse_feature_command(rest),
        CommandHandler::Features => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Features)
        }
        CommandHandler::Fork => parse_no_argument_command(rest, spec.name, BuiltinCommand::Fork),
        CommandHandler::Goal => parse_goal_command(rest),
        CommandHandler::Hooks => parse_no_argument_command(rest, spec.name, BuiltinCommand::Hooks),
        CommandHandler::Kill => {
            let process_id = rest.trim();
            if process_id.is_empty() {
                return Err(Error::invalid_params().data("/kill requires a process id"));
            }
            Ok(Some(BuiltinCommand::Kill {
                process_id: process_id.to_owned(),
            }))
        }
        CommandHandler::MarketplaceAdd => parse_marketplace_add_command(rest),
        CommandHandler::MarketplaceRemove => parse_marketplace_remove_command(rest),
        CommandHandler::Memory => parse_memory_command(rest),
        CommandHandler::Init => parse_no_argument_command(rest, spec.name, BuiltinCommand::Init),
        CommandHandler::Mcp => parse_no_argument_command(rest, spec.name, BuiltinCommand::Mcp),
        CommandHandler::McpReload => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::McpReload)
        }
        CommandHandler::McpResource => parse_mcp_resource_command(rest),
        CommandHandler::McpTool => parse_mcp_tool_command(rest),
        CommandHandler::Model => parse_no_argument_command(rest, spec.name, BuiltinCommand::Model),
        CommandHandler::New => parse_no_argument_command(rest, spec.name, BuiltinCommand::New),
        CommandHandler::Permissions => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Permissions)
        }
        CommandHandler::Plan => parse_no_argument_command(rest, spec.name, BuiltinCommand::Plan),
        CommandHandler::Plugin => parse_plugin_command(rest),
        CommandHandler::PluginInstall => parse_plugin_install_command(rest),
        CommandHandler::PluginUninstall => parse_plugin_uninstall_command(rest),
        CommandHandler::Plugins => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Plugins)
        }
        CommandHandler::Ps => parse_no_argument_command(rest, spec.name, BuiltinCommand::Ps),
        CommandHandler::RateLimits => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::RateLimits)
        }
        CommandHandler::Rename => {
            let title = rest.trim();
            if title.is_empty() {
                return Err(Error::invalid_params().data("/rename requires a title"));
            }
            Ok(Some(BuiltinCommand::Rename {
                title: title.to_owned(),
            }))
        }
        CommandHandler::Resume => {
            let target = rest.trim();
            if target.is_empty() {
                return Err(Error::invalid_params().data("/resume requires a thread id or name"));
            }
            Ok(Some(BuiltinCommand::Resume {
                target: target.to_owned(),
            }))
        }
        CommandHandler::Review => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Review)
        }
        CommandHandler::Rollback => {
            let num_turns = parse_positive_u32(rest.trim(), "/rollback requires a turn count")?;
            Ok(Some(BuiltinCommand::Rollback { num_turns }))
        }
        CommandHandler::SkillRoots => parse_skill_roots_command(rest),
        CommandHandler::Status => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Status)
        }
        CommandHandler::Stop => parse_no_argument_command(rest, spec.name, BuiltinCommand::Stop),
        CommandHandler::Unarchive => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Unarchive)
        }
        CommandHandler::Usage => parse_no_argument_command(rest, spec.name, BuiltinCommand::Usage),
        CommandHandler::WorkspaceMessages => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::WorkspaceMessages)
        }
    }
}

fn parse_no_argument_command(
    rest: &str,
    command_name: &str,
    command: BuiltinCommand,
) -> Result<Option<BuiltinCommand>, Error> {
    if !rest.trim().is_empty() {
        return Err(
            Error::invalid_params().data(format!("/{command_name} does not accept arguments"))
        );
    }
    Ok(Some(command))
}

fn parse_config_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    let cwd = rest.trim();
    Ok(Some(BuiltinCommand::Config {
        cwd: (!cwd.is_empty()).then(|| cwd.to_owned()),
    }))
}

fn parse_memory_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    let action = match rest.trim() {
        "enable" => MemoryCommandAction::Enable,
        "disable" => MemoryCommandAction::Disable,
        "reset" => MemoryCommandAction::Reset,
        "" => {
            return Err(
                Error::invalid_params().data("/memory requires `enable`, `disable`, or `reset`")
            );
        }
        _ => {
            return Err(Error::invalid_params()
                .data("/memory accepts only `enable`, `disable`, or `reset`"));
        }
    };
    Ok(Some(BuiltinCommand::Memory { action }))
}

fn parse_feature_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    let mut parts = rest.split_whitespace();
    let name = parts.next().ok_or_else(|| {
        Error::invalid_params().data("/feature requires a feature name and `enable` or `disable`")
    })?;
    let action = parts.next().ok_or_else(|| {
        Error::invalid_params().data("/feature requires a feature name and `enable` or `disable`")
    })?;
    if parts.next().is_some() {
        return Err(
            Error::invalid_params().data("/feature accepts only a feature name and one action")
        );
    }

    let enabled = match action {
        "enable" => true,
        "disable" => false,
        _ => {
            return Err(
                Error::invalid_params().data("/feature action must be `enable` or `disable`")
            );
        }
    };

    Ok(Some(BuiltinCommand::Feature {
        name: name.to_owned(),
        enabled,
    }))
}

fn parse_marketplace_add_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    let mut parts = rest.split_whitespace();
    let source = parts.next().ok_or_else(|| {
        Error::invalid_params()
            .data("/marketplace-add requires a source, optional ref, and optional sparse paths")
    })?;
    let ref_name = parts.next().map(str::to_owned);
    let sparse_paths = parts.next().map(parse_comma_separated_values).transpose()?;
    if parts.next().is_some() {
        return Err(Error::invalid_params().data(
            "/marketplace-add accepts only source, optional ref, and optional sparse paths",
        ));
    }

    Ok(Some(BuiltinCommand::MarketplaceAdd {
        source: source.to_owned(),
        ref_name,
        sparse_paths,
    }))
}

fn parse_marketplace_remove_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    let marketplace_name = rest.trim();
    if marketplace_name.is_empty() || marketplace_name.split_whitespace().count() != 1 {
        return Err(Error::invalid_params().data("/marketplace-remove requires a marketplace name"));
    }
    Ok(Some(BuiltinCommand::MarketplaceRemove {
        marketplace_name: marketplace_name.to_owned(),
    }))
}

fn parse_comma_separated_values(value: &str) -> Result<Vec<String>, Error> {
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(Error::invalid_params().data("comma-separated values cannot be empty"));
    }
    Ok(values)
}

fn parse_goal_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    let rest = rest.trim();
    if rest.is_empty() {
        return Ok(Some(BuiltinCommand::GoalGet));
    }
    if rest == "get" {
        return Ok(Some(BuiltinCommand::GoalGet));
    }
    if rest == "clear" {
        return Ok(Some(BuiltinCommand::GoalClear));
    }
    Ok(Some(BuiltinCommand::GoalSet {
        objective: rest.to_owned(),
    }))
}

fn parse_plugin_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    const MESSAGE: &str =
        "/plugin requires a plugin name and marketplace path as name@marketplacePath";

    let (plugin_name, marketplace_path) = parse_plugin_marketplace_ref(rest, MESSAGE)?;

    Ok(Some(BuiltinCommand::Plugin {
        marketplace_path,
        plugin_name,
    }))
}

fn parse_plugin_install_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    const MESSAGE: &str =
        "/plugin-install requires a plugin name and marketplace path as name@marketplacePath";

    let (plugin_name, marketplace_path) = parse_plugin_marketplace_ref(rest, MESSAGE)?;
    Ok(Some(BuiltinCommand::PluginInstall {
        marketplace_path,
        plugin_name,
    }))
}

fn parse_plugin_uninstall_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    let plugin_id = rest.trim();
    if plugin_id.is_empty() || plugin_id.split_whitespace().count() != 1 {
        return Err(Error::invalid_params().data("/plugin-uninstall requires a plugin id"));
    }
    Ok(Some(BuiltinCommand::PluginUninstall {
        plugin_id: plugin_id.to_owned(),
    }))
}

fn parse_plugin_marketplace_ref(
    rest: &str,
    message: &'static str,
) -> Result<(String, String), Error> {
    let rest = rest.trim();
    if rest.is_empty() || rest.split_whitespace().count() != 1 {
        return Err(Error::invalid_params().data(message));
    }

    let Some((plugin_name, marketplace_path)) = rest.split_once('@') else {
        return Err(Error::invalid_params().data(message));
    };
    let plugin_name = plugin_name.trim();
    let marketplace_path = marketplace_path.trim();
    if plugin_name.is_empty() || marketplace_path.is_empty() {
        return Err(Error::invalid_params().data(message));
    }

    Ok((plugin_name.to_owned(), marketplace_path.to_owned()))
}

fn parse_mcp_resource_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    let Some((server, uri)) = split_first_token(rest) else {
        return Err(
            Error::invalid_params().data("/mcp-resource requires a server and resource uri")
        );
    };
    let uri = uri.trim();
    if uri.is_empty() {
        return Err(
            Error::invalid_params().data("/mcp-resource requires a server and resource uri")
        );
    }

    Ok(Some(BuiltinCommand::McpResource {
        server: server.to_owned(),
        uri: uri.to_owned(),
    }))
}

fn parse_mcp_tool_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    const MESSAGE: &str = "/mcp-tool requires a server and tool name";

    let Some((server, rest)) = split_first_token(rest) else {
        return Err(Error::invalid_params().data(MESSAGE));
    };
    let Some((tool, arguments_text)) = split_first_token(rest) else {
        return Err(Error::invalid_params().data(MESSAGE));
    };
    let arguments = if arguments_text.trim().is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_str(arguments_text.trim()).map_err(|error| {
            Error::invalid_params().data(format!("/mcp-tool arguments must be valid JSON: {error}"))
        })?
    };

    Ok(Some(BuiltinCommand::McpTool {
        server: server.to_owned(),
        tool: tool.to_owned(),
        arguments,
    }))
}

fn parse_skill_roots_command(rest: &str) -> Result<Option<BuiltinCommand>, Error> {
    let roots = rest
        .split([',', ';'])
        .flat_map(str::split_whitespace)
        .filter(|root| !root.trim().is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if roots.is_empty() {
        return Err(Error::invalid_params().data("/skill-roots requires at least one path"));
    }
    Ok(Some(BuiltinCommand::SkillRoots { roots }))
}

fn parse_positive_u32(text: &str, empty_message: &'static str) -> Result<u32, Error> {
    if text.is_empty() {
        return Err(Error::invalid_params().data(empty_message));
    }
    let value = text.parse::<u32>().map_err(|_| {
        Error::invalid_params().data(format!("expected a positive integer, got `{text}`"))
    })?;
    if value == 0 {
        return Err(Error::invalid_params().data("turn count must be greater than zero"));
    }
    Ok(value)
}

fn init_prompt() -> String {
    [
        "Create or update AGENTS.md for this repository.",
        "",
        "Inspect the repository structure, build and test commands, style conventions, architecture notes, and any existing contributor guidance. Preserve useful existing instructions. Keep the file concise, accurate, and actionable for coding agents working in this repository. Write repository instructions in English.",
    ]
    .join("\n")
}

fn parse_skill_invocation(text: &str) -> Option<SkillInvocation> {
    if let Some(stripped) = text.strip_prefix('$') {
        let (name, _) = split_first_token(stripped)?;
        return Some(SkillInvocation {
            name: name.to_owned(),
            visible_text: text.to_owned(),
        });
    }

    let stripped = text.strip_prefix("/skill ")?;
    let (name, rest) = split_first_token(stripped)?;
    let visible_text = if rest.is_empty() {
        format!("${name}")
    } else {
        format!("${name} {rest}")
    };

    Some(SkillInvocation {
        name: name.to_owned(),
        visible_text,
    })
}

fn split_first_token(text: &str) -> Option<(&str, &str)> {
    let text = text.trim_start();
    if text.is_empty() {
        return None;
    }

    match text.find(char::is_whitespace) {
        Some(index) => Some((&text[..index], text[index..].trim_start())),
        None => Some((text, "")),
    }
}

fn config_options(state: &AcpConfigState) -> Vec<SessionConfigOption> {
    let mut options = Vec::new();

    if !state.models.is_empty()
        && let Some(current_model) = state.current_model.as_deref()
    {
        options.push(
            SessionConfigOption::select(
                MODEL_CONFIG_ID,
                "Model",
                current_model.to_owned(),
                state
                    .models
                    .iter()
                    .map(|model| {
                        SessionConfigSelectOption::new(model.id.clone(), model.display_name.clone())
                            .description(model.description.clone())
                    })
                    .collect::<Vec<_>>(),
            )
            .category(SessionConfigOptionCategory::Model),
        );
    }

    if let Some(model) = active_model_catalog(state) {
        let current_effort = state
            .current_reasoning_effort
            .clone()
            .or_else(|| default_reasoning_effort_id(model));
        if !model.supported_reasoning_efforts.is_empty()
            && let Some(current_effort) = current_effort
        {
            options.push(
                SessionConfigOption::select(
                    REASONING_EFFORT_CONFIG_ID,
                    "Reasoning",
                    current_effort,
                    model
                        .supported_reasoning_efforts
                        .iter()
                        .map(|effort| {
                            SessionConfigSelectOption::new(
                                effort.reasoning_effort.clone(),
                                reasoning_effort_name(&effort.reasoning_effort),
                            )
                            .description(effort.description.clone())
                        })
                        .collect::<Vec<_>>(),
                )
                .description("Controls reasoning effort for subsequent turns.")
                .category(SessionConfigOptionCategory::ThoughtLevel),
            );
        }

        if !model.service_tiers.is_empty() {
            let current_tier = state
                .current_service_tier
                .clone()
                .unwrap_or_else(|| DEFAULT_SERVICE_TIER_VALUE.to_owned());
            let mut tier_options = vec![
                SessionConfigSelectOption::new(DEFAULT_SERVICE_TIER_VALUE, "Automatic")
                    .description(default_service_tier_description(model)),
            ];
            tier_options.extend(model.service_tiers.iter().map(|tier| {
                SessionConfigSelectOption::new(tier.id.clone(), tier.name.clone())
                    .description(tier.description.clone())
            }));

            options.push(
                SessionConfigOption::select(
                    SERVICE_TIER_CONFIG_ID,
                    "Service tier",
                    current_tier,
                    tier_options,
                )
                .description("Controls the service tier Codex requests for subsequent turns.")
                .category(SessionConfigOptionCategory::Other(
                    "model_config".to_owned(),
                )),
            );
        }
    }

    if let Some(current_approval_policy) = state.current_approval_policy.as_deref() {
        options.push(
            SessionConfigOption::select(
                APPROVAL_POLICY_CONFIG_ID,
                "Approval policy",
                current_approval_policy.to_owned(),
                APPROVAL_POLICY_OPTIONS
                    .iter()
                    .map(|(id, name, description)| {
                        SessionConfigSelectOption::new(*id, *name).description(*description)
                    })
                    .collect::<Vec<_>>(),
            )
            .description("Controls when Codex asks for approval on subsequent turns.")
            .category(SessionConfigOptionCategory::Mode),
        );
    }

    if !state.collaboration_modes.is_empty()
        && let Some(current_collaboration_mode) = state.current_collaboration_mode.as_deref()
    {
        let mode_options = state
            .collaboration_modes
            .iter()
            .filter_map(|mode| {
                let id = collaboration_mode_id(mode)?;
                Some(SessionConfigSelectOption::new(
                    id,
                    collaboration_mode_name(mode),
                ))
            })
            .collect::<Vec<_>>();
        if !mode_options.is_empty() {
            options.push(
                SessionConfigOption::select(
                    COLLABORATION_MODE_CONFIG_ID,
                    "Collaboration mode",
                    current_collaboration_mode.to_owned(),
                    mode_options,
                )
                .description("Controls Codex collaboration behavior for subsequent turns.")
                .category(SessionConfigOptionCategory::Mode),
            );
        }
    }

    if !state.permission_profiles.is_empty()
        && let Some(current_profile) = state.current_permission_profile.as_deref()
    {
        options.push(
            SessionConfigOption::select(
                PERMISSION_PROFILE_CONFIG_ID,
                "Permissions",
                current_profile.to_owned(),
                state
                    .permission_profiles
                    .iter()
                    .map(|profile| {
                        SessionConfigSelectOption::new(
                            profile.id.clone(),
                            permission_profile_name(&profile.id),
                        )
                        .description(profile.description.clone())
                    })
                    .collect::<Vec<_>>(),
            )
            .description("Controls the permission profile Codex uses for subsequent turns.")
            .category(SessionConfigOptionCategory::Mode),
        );
    }

    options
}

fn skill_config_options(skills: &[AppServerSkill]) -> Vec<SessionConfigOption> {
    let mut seen = HashSet::new();
    skills
        .iter()
        .filter(|skill| !skill.name.is_empty())
        .filter(|skill| seen.insert(skill.name.clone()))
        .map(|skill| {
            let description = skill
                .interface
                .as_ref()
                .and_then(|interface| interface.short_description.clone())
                .or_else(|| skill.description.clone())
                .or_else(|| skill.path.clone());
            let display_name = skill
                .interface
                .as_ref()
                .and_then(|interface| interface.display_name.clone())
                .unwrap_or_else(|| skill.name.clone());
            SessionConfigOption::select(
                skill_config_id(&skill.name),
                format!("Skill: {display_name}"),
                if skill.enabled {
                    SKILL_ENABLED_VALUE
                } else {
                    SKILL_DISABLED_VALUE
                },
                vec![
                    SessionConfigSelectOption::new(SKILL_ENABLED_VALUE, "Enabled"),
                    SessionConfigSelectOption::new(SKILL_DISABLED_VALUE, "Disabled"),
                ],
            )
            .description(description)
            .category(SessionConfigOptionCategory::Other("skills".to_owned()))
        })
        .collect()
}

fn skill_config_id(skill_name: &str) -> String {
    format!("{SKILL_CONFIG_PREFIX}{skill_name}")
}

fn default_model_id(models: &[AppServerModel]) -> Option<String> {
    models
        .iter()
        .find(|model| model.is_default)
        .or_else(|| models.first())
        .map(|model| model.id.clone())
}

fn active_model_catalog(state: &AcpConfigState) -> Option<&AppServerModel> {
    let current_model = state.current_model.as_deref()?;
    state.models.iter().find(|model| model.id == current_model)
}

fn default_reasoning_effort_id(model: &AppServerModel) -> Option<String> {
    model.default_reasoning_effort.clone().or_else(|| {
        model
            .supported_reasoning_efforts
            .first()
            .map(|effort| effort.reasoning_effort.clone())
    })
}

fn is_known_approval_policy(value: &str) -> bool {
    APPROVAL_POLICY_OPTIONS
        .iter()
        .any(|(id, _, _)| *id == value)
}

fn default_collaboration_mode_id(modes: &[AppServerCollaborationModeMask]) -> Option<String> {
    modes
        .iter()
        .find(|mode| mode.mode.as_deref() == Some("default"))
        .and_then(collaboration_mode_id)
        .or_else(|| modes.iter().find_map(collaboration_mode_id))
}

fn collaboration_mode_id(mode: &AppServerCollaborationModeMask) -> Option<String> {
    mode.mode.clone()
}

fn collaboration_mode_name(mode: &AppServerCollaborationModeMask) -> String {
    if !mode.name.is_empty() {
        return mode.name.clone();
    }

    mode.mode
        .as_deref()
        .map(humanize_identifier)
        .unwrap_or_else(|| "Unknown".to_owned())
}

fn collaboration_mode_for_config(
    state: &AcpConfigState,
    value: &str,
) -> Option<AppServerCollaborationMode> {
    let mode = state
        .collaboration_modes
        .iter()
        .find(|mode| collaboration_mode_id(mode).as_deref() == Some(value))?;
    let mode_id = mode.mode.clone()?;
    let model = mode
        .model
        .clone()
        .or_else(|| state.current_model.clone())
        .or_else(|| default_model_id(&state.models))?;
    let current_effort = state
        .current_reasoning_effort
        .clone()
        .or_else(|| active_model_catalog(state).and_then(default_reasoning_effort_id));
    let reasoning_effort = mode.reasoning_effort.clone().unwrap_or(current_effort);

    Some(AppServerCollaborationMode::new(
        mode_id,
        model,
        reasoning_effort,
        None,
    ))
}

fn humanize_identifier(id: &str) -> String {
    let words = id
        .split(['-', '_'])
        .flat_map(split_camel_words)
        .collect::<Vec<_>>();
    let Some((first, rest)) = words.split_first() else {
        return String::new();
    };
    let mut result = capitalize(first);
    for word in rest {
        result.push(' ');
        result.push_str(word);
    }
    result
}

fn split_camel_words(word: &str) -> Vec<&str> {
    if word.is_empty() {
        return Vec::new();
    }
    let mut words = Vec::new();
    let mut start = 0;
    let mut previous_lowercase = false;
    for (index, ch) in word.char_indices() {
        if index > 0 && previous_lowercase && ch.is_uppercase() {
            words.push(&word[start..index]);
            start = index;
        }
        previous_lowercase = ch.is_lowercase();
    }
    words.push(&word[start..]);
    words
}

fn capitalize(text: &str) -> String {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    first.to_uppercase().chain(chars).collect()
}

fn default_service_tier_description(model: &AppServerModel) -> Option<String> {
    let default_tier = model.default_service_tier.as_ref()?;
    let tier_name = model
        .service_tiers
        .iter()
        .find(|tier| &tier.id == default_tier)
        .map(|tier| tier.name.as_str())
        .unwrap_or(default_tier);
    Some(format!(
        "Use the catalog default service tier ({tier_name})."
    ))
}

fn reasoning_effort_name(id: &str) -> String {
    match id {
        "minimal" => "Minimal".to_owned(),
        "low" => "Low".to_owned(),
        "medium" => "Medium".to_owned(),
        "high" => "High".to_owned(),
        "max" => "Max".to_owned(),
        other => other.to_owned(),
    }
}

fn default_permission_profile_id(profiles: &[AppServerPermissionProfile]) -> Option<String> {
    profiles
        .iter()
        .find(|profile| profile.id == ":workspace")
        .or_else(|| profiles.first())
        .map(|profile| profile.id.clone())
}

fn permission_profile_name(id: &str) -> String {
    match id {
        ":read-only" => "Read only".to_owned(),
        ":workspace" => "Workspace".to_owned(),
        ":danger-full-access" => "Full access".to_owned(),
        _ => id.to_owned(),
    }
}

fn send_session_update(
    cx: &ConnectionTo<Client>,
    session_id: SessionId,
    update: SessionUpdate,
) -> anyhow::Result<()> {
    trace!(
        session_id = session_id.0.as_ref(),
        "sending ACP session update"
    );
    cx.send_notification(SessionNotification::new(session_id, update))
        .map_err(|error| anyhow::anyhow!("failed to send ACP session update: {error}"))
}

fn send_available_commands_update(
    cx: &ConnectionTo<Client>,
    session_id: SessionId,
    commands: Vec<AvailableCommand>,
) -> Result<(), Error> {
    send_session_update(
        cx,
        session_id,
        SessionUpdate::AvailableCommandsUpdate(AvailableCommandsUpdate::new(commands)),
    )
    .map_err(acp_internal_error)
}

fn publish_session_name_update(
    session_id: &SessionId,
    title: Option<String>,
    cx: &ConnectionTo<Client>,
) -> anyhow::Result<()> {
    send_session_update(
        cx,
        session_id.clone(),
        SessionUpdate::SessionInfoUpdate(SessionInfoUpdate::new().title(title)),
    )
}

fn publish_session_archived_update(
    session_id: &SessionId,
    archived: bool,
    cx: &ConnectionTo<Client>,
) -> anyhow::Result<()> {
    publish_session_adapter_meta_update(
        session_id,
        [("archived".to_owned(), serde_json::Value::Bool(archived))],
        cx,
    )
}

fn publish_session_deleted_update(
    session_id: &SessionId,
    deleted: bool,
    cx: &ConnectionTo<Client>,
) -> anyhow::Result<()> {
    publish_session_adapter_meta_update(
        session_id,
        [("deleted".to_owned(), serde_json::Value::Bool(deleted))],
        cx,
    )
}

fn publish_session_closed_update(
    session_id: &SessionId,
    closed: bool,
    cx: &ConnectionTo<Client>,
) -> anyhow::Result<()> {
    publish_session_adapter_meta_update(
        session_id,
        [("closed".to_owned(), serde_json::Value::Bool(closed))],
        cx,
    )
}

fn publish_session_status_update(
    session_id: &SessionId,
    status: serde_json::Value,
    cx: &ConnectionTo<Client>,
) -> anyhow::Result<()> {
    publish_session_adapter_meta_update(session_id, [("threadStatus".to_owned(), status)], cx)
}

fn publish_session_goal_update(
    session_id: &SessionId,
    goal: Option<serde_json::Value>,
    cx: &ConnectionTo<Client>,
) -> anyhow::Result<()> {
    publish_session_adapter_meta_update(
        session_id,
        [("goal".to_owned(), goal.unwrap_or(serde_json::Value::Null))],
        cx,
    )
}

fn publish_session_adapter_meta_update(
    session_id: &SessionId,
    fields: impl IntoIterator<Item = (String, serde_json::Value)>,
    cx: &ConnectionTo<Client>,
) -> anyhow::Result<()> {
    let adapter_meta = fields.into_iter().collect::<serde_json::Map<_, _>>();
    let mut meta = serde_json::Map::new();
    meta.insert(
        "brokk_codex_acp".to_owned(),
        serde_json::Value::Object(adapter_meta),
    );
    send_session_update(
        cx,
        session_id.clone(),
        SessionUpdate::SessionInfoUpdate(SessionInfoUpdate::new().meta(meta)),
    )
}

fn session_info_from_app_server_thread(
    thread: AppServerThread,
    additional_directories: Vec<PathBuf>,
) -> Option<SessionInfo> {
    let cwd = thread.cwd.clone()?;
    let title = thread.name.clone().or_else(|| thread.preview.clone());
    let meta = session_info_meta_from_app_server_thread(&thread);

    Some(
        SessionInfo::new(SessionId::new(thread.id), cwd)
            .additional_directories(additional_directories)
            .title(title)
            .updated_at(thread.updated_at.map(unix_timestamp_to_utc_iso8601))
            .meta(meta),
    )
}

fn session_info_meta_from_app_server_thread(
    thread: &AppServerThread,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut adapter_meta = serde_json::Map::new();

    insert_optional_string(&mut adapter_meta, "preview", thread.preview.as_ref());
    insert_optional_value(&mut adapter_meta, "threadStatus", thread.status.as_ref());
    insert_optional_string(
        &mut adapter_meta,
        "modelProvider",
        thread.model_provider.as_ref(),
    );
    insert_optional_i64(&mut adapter_meta, "createdAt", thread.created_at);
    insert_optional_i64(&mut adapter_meta, "updatedAt", thread.updated_at);
    insert_optional_i64(&mut adapter_meta, "recencyAt", thread.recency_at);
    insert_optional_string(
        &mut adapter_meta,
        "agentNickname",
        thread.agent_nickname.as_ref(),
    );
    insert_optional_string(&mut adapter_meta, "agentRole", thread.agent_role.as_ref());
    insert_optional_string(
        &mut adapter_meta,
        "parentThreadId",
        thread.parent_thread_id.as_ref(),
    );

    if adapter_meta.is_empty() {
        None
    } else {
        let mut meta = serde_json::Map::new();
        meta.insert(
            "brokk_codex_acp".to_owned(),
            serde_json::Value::Object(adapter_meta),
        );
        Some(meta)
    }
}

fn insert_optional_string(
    meta: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<&String>,
) {
    if let Some(value) = value {
        meta.insert(key.to_owned(), serde_json::Value::String(value.clone()));
    }
}

fn insert_optional_i64(
    meta: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<i64>,
) {
    if let Some(value) = value {
        meta.insert(key.to_owned(), serde_json::Value::from(value));
    }
}

fn insert_optional_value(
    meta: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<&serde_json::Value>,
) {
    if let Some(value) = value {
        meta.insert(key.to_owned(), value.clone());
    }
}

fn runtime_workspace_roots_for_acp_request(
    cwd: &Path,
    additional_directories: &[PathBuf],
) -> Result<Vec<PathBuf>, Error> {
    if !cwd.is_absolute() {
        return Err(Error::invalid_params().data("cwd must be an absolute path"));
    }

    let mut roots = vec![cwd.to_path_buf()];
    for additional_directory in additional_directories {
        if !additional_directory.is_absolute() {
            return Err(Error::invalid_params()
                .data("additionalDirectories entries must be absolute paths"));
        }
        if !roots.iter().any(|root| root == additional_directory) {
            roots.push(additional_directory.clone());
        }
    }
    Ok(roots)
}

fn additional_directories_from_runtime_roots(
    cwd: &Path,
    runtime_workspace_roots: Vec<PathBuf>,
) -> Vec<PathBuf> {
    let mut additional_directories = Vec::new();
    for root in runtime_workspace_roots {
        if root == cwd
            || additional_directories
                .iter()
                .any(|existing| existing == &root)
        {
            continue;
        }
        additional_directories.push(root);
    }
    additional_directories
}

fn session_additional_directories_store_path(codex_home: &Path) -> PathBuf {
    codex_home
        .join("brokk-codex-acp")
        .join("session-additional-directories.json")
}

fn load_session_additional_directories(path: &Path) -> HashMap<String, Vec<PathBuf>> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return HashMap::new(),
        Err(error) => {
            warn!(
                path = %path.display(),
                %error,
                "failed to read persisted session additional directories"
            );
            return HashMap::new();
        }
    };

    match serde_json::from_str::<PersistedSessionAdditionalDirectories>(&contents) {
        Ok(persisted) => persisted.sessions,
        Err(error) => {
            warn!(
                path = %path.display(),
                %error,
                "failed to decode persisted session additional directories"
            );
            HashMap::new()
        }
    }
}

fn persist_session_additional_directories(path: &Path, sessions: &HashMap<String, Vec<PathBuf>>) {
    let persisted = PersistedSessionAdditionalDirectories {
        sessions: sessions.clone(),
    };
    let contents = match serde_json::to_string_pretty(&persisted) {
        Ok(contents) => contents,
        Err(error) => {
            warn!(
                path = %path.display(),
                %error,
                "failed to encode persisted session additional directories"
            );
            return;
        }
    };

    if let Some(parent) = path.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        warn!(
            path = %parent.display(),
            %error,
            "failed to create persisted session additional directories directory"
        );
        return;
    }

    let tmp_path = path.with_extension("json.tmp");
    if let Err(error) = fs::write(&tmp_path, contents) {
        warn!(
            path = %tmp_path.display(),
            %error,
            "failed to write persisted session additional directories"
        );
        return;
    }
    if let Err(error) = fs::rename(&tmp_path, path) {
        warn!(
            source = %tmp_path.display(),
            target = %path.display(),
            %error,
            "failed to replace persisted session additional directories"
        );
    }
}

fn unix_timestamp_to_utc_iso8601(timestamp: i64) -> String {
    let days = timestamp.div_euclid(86_400);
    let seconds_of_day = timestamp.rem_euclid(86_400);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    let (year, month, day) = civil_from_unix_days(days);

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_unix_days(days: i64) -> (i64, i64, i64) {
    let shifted_days = days + 719_468;
    let era = if shifted_days >= 0 {
        shifted_days
    } else {
        shifted_days - 146_096
    } / 146_097;
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };

    (year, month, day)
}

#[derive(Default)]
struct AcpEventState {
    tool_outputs: HashMap<String, String>,
    announced_turn_diffs: HashSet<String>,
}

fn send_prompt_event(
    cx: &ConnectionTo<Client>,
    session_id: SessionId,
    event: AppServerPromptEvent,
    state: &mut AcpEventState,
) -> anyhow::Result<()> {
    match event {
        AppServerPromptEvent::AgentMessageDelta(delta) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(agent_message_chunk(delta)),
        ),
        AppServerPromptEvent::AgentThoughtDelta(delta) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentThoughtChunk(text_chunk(delta)),
        ),
        AppServerPromptEvent::ToolCallStarted(call) => send_session_update(
            cx,
            session_id,
            SessionUpdate::ToolCall(
                ToolCall::new(call.id, call.title)
                    .kind(tool_kind(call.kind))
                    .status(tool_status(call.status))
                    .raw_input(call.raw),
            ),
        ),
        AppServerPromptEvent::ToolCallUpdated(update) => {
            let mut fields = ToolCallUpdateFields::new();
            fields.title = update.title;
            fields.kind = update.kind.map(tool_kind);
            fields.status = update.status.map(tool_status);
            let is_final_update = update.raw.is_some();
            if let Some(output_text) = update.output_delta {
                let output = state.tool_outputs.entry(update.id.clone()).or_default();
                if is_final_update {
                    output.clear();
                }
                output.push_str(&output_text);
                fields.content = Some(vec![text_tool_content(output.clone())]);
            }
            if !update.diffs.is_empty() {
                fields.content = Some(update.diffs.into_iter().map(diff_tool_content).collect());
            }
            fields.raw_output = update.raw;
            send_session_update(
                cx,
                session_id,
                SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(update.id, fields)),
            )
        }
        AppServerPromptEvent::GuardianApprovalReview(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(guardian_approval_review_message(&update))),
        ),
        AppServerPromptEvent::ServerRequestResolved(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(server_request_resolved_message(&update))),
        ),
        AppServerPromptEvent::PlanUpdated(entries) => {
            let entries = entries
                .into_iter()
                .map(|entry| {
                    PlanEntry::new(
                        entry.content,
                        PlanEntryPriority::Medium,
                        plan_status(entry.status),
                    )
                })
                .collect();
            send_session_update(cx, session_id, SessionUpdate::Plan(Plan::new(entries)))
        }
        AppServerPromptEvent::TurnDiffUpdated { turn_id, diff } => {
            let tool_call_id = format!("turn-diff:{turn_id}");
            if state.announced_turn_diffs.insert(tool_call_id.clone()) {
                send_session_update(
                    cx,
                    session_id.clone(),
                    SessionUpdate::ToolCall(
                        ToolCall::new(tool_call_id.clone(), "File changes")
                            .kind(ToolKind::Edit)
                            .status(ToolCallStatus::InProgress),
                    ),
                )?;
            }
            let fields = ToolCallUpdateFields::new()
                .content(vec![text_tool_content(format!("Unified diff:\n{diff}"))]);
            send_session_update(
                cx,
                session_id,
                SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(tool_call_id, fields)),
            )
        }
        AppServerPromptEvent::UsageUpdated(usage) => send_session_update(
            cx,
            session_id,
            SessionUpdate::UsageUpdate(UsageUpdate::new(usage.used, usage.size)),
        ),
        AppServerPromptEvent::Warning(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(warning_message(&update))),
        ),
        AppServerPromptEvent::Error(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(error_message(&update))),
        ),
        AppServerPromptEvent::ModelRerouted(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(model_rerouted_message(&update))),
        ),
        AppServerPromptEvent::ModelSafetyBuffering(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(model_safety_buffering_message(&update))),
        ),
        AppServerPromptEvent::ModelVerification(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(model_verification_message(&update))),
        ),
        AppServerPromptEvent::TurnModerationMetadata(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(turn_moderation_metadata_message(&update))),
        ),
        AppServerPromptEvent::McpServerStartupStatus(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(mcp_startup_status_message(&update))),
        ),
        AppServerPromptEvent::Realtime(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(realtime_message(&update))),
        ),
        AppServerPromptEvent::ConfigWarning(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(config_warning_message(&update))),
        ),
        AppServerPromptEvent::WindowsSandboxSetup(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(windows_sandbox_setup_message(&update))),
        ),
        AppServerPromptEvent::AccountLoginCompleted(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(account_login_completed_message(&update))),
        ),
        AppServerPromptEvent::AccountUpdated(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(account_updated_message(&update))),
        ),
        AppServerPromptEvent::AccountRateLimitsUpdated(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(account_rate_limits_updated_message(
                &update,
            ))),
        ),
        AppServerPromptEvent::McpServerOAuthLoginCompleted(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(mcp_oauth_login_completed_message(
                &update,
            ))),
        ),
        AppServerPromptEvent::AppListUpdated(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(app_list_updated_message(&update))),
        ),
        AppServerPromptEvent::FuzzyFileSearch(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(fuzzy_file_search_message(&update))),
        ),
        AppServerPromptEvent::RemoteControlStatus(update) => send_session_update(
            cx,
            session_id,
            SessionUpdate::AgentMessageChunk(text_chunk(remote_control_status_message(&update))),
        ),
        AppServerPromptEvent::SkillsChanged | AppServerPromptEvent::ThreadSettingsUpdated(_) => {
            Ok(())
        }
    }
}

fn handle_app_server_event(
    cx: &ConnectionTo<Client>,
    session_id: SessionId,
    event: AppServerPromptEvent,
    state: &mut AcpEventState,
    pending_updates: &mut PendingAppServerUpdates,
) -> anyhow::Result<()> {
    match event {
        AppServerPromptEvent::SkillsChanged => {
            pending_updates.skills_changed = true;
            Ok(())
        }
        AppServerPromptEvent::ThreadSettingsUpdated(settings) => {
            pending_updates.thread_settings = Some(settings);
            Ok(())
        }
        event => send_prompt_event(cx, session_id, event, state),
    }
}

async fn request_permission(
    cx: &ConnectionTo<Client>,
    session_id: SessionId,
    approval: AppServerApprovalRequest,
    outstanding_approvals: CancelSignals,
) -> anyhow::Result<AppServerApprovalDecision> {
    let approval_key = approval_cancel_key(&session_id, &approval.item_id);
    let (cancel_tx, cancel_rx) = oneshot::channel();
    if let Some(previous_cancel) = outstanding_approvals
        .lock()
        .await
        .insert(approval_key.clone(), cancel_tx)
    {
        let _ = previous_cancel.send(());
    }

    let mut fields = ToolCallUpdateFields::new();
    fields.content = permission_request_content(&approval);
    fields.title = Some(approval.title);
    fields.kind = Some(tool_kind(approval.kind));
    fields.status = Some(ToolCallStatus::Pending);
    fields.raw_input = Some(approval.raw);

    let decisions_by_option_id = approval
        .options
        .iter()
        .map(|choice| (choice.id().to_owned(), choice.decision()))
        .collect::<HashMap<_, _>>();
    let options = approval
        .options
        .into_iter()
        .map(permission_option)
        .collect::<Vec<_>>();

    let known_option_ids = options
        .iter()
        .map(|option| option.option_id.0.to_string())
        .collect::<HashSet<_>>();

    let request = RequestPermissionRequest::new(
        session_id,
        ToolCallUpdate::new(approval.item_id.clone(), fields),
        options,
    );

    let decision = tokio::select! {
        response = cx.send_request(request).block_task() => {
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    outstanding_approvals.lock().await.remove(&approval_key);
                    return Err(anyhow::anyhow!("ACP permission request failed: {error}"));
                }
            };

            match response.outcome {
                RequestPermissionOutcome::Selected(selected) => {
                    let option_id = selected.option_id.0.as_ref();
                    if known_option_ids.contains(option_id) {
                        decisions_by_option_id
                            .get(option_id)
                            .cloned()
                            .unwrap_or(AppServerApprovalDecision::Cancel)
                    } else {
                        AppServerApprovalDecision::Cancel
                    }
                }
                RequestPermissionOutcome::Cancelled => AppServerApprovalDecision::Cancel,
                _ => AppServerApprovalDecision::Cancel,
            }
        }
        _ = cancel_rx => AppServerApprovalDecision::Cancel,
    };

    outstanding_approvals.lock().await.remove(&approval_key);
    Ok(decision)
}

async fn request_interactive(
    cx: &ConnectionTo<Client>,
    session_id: SessionId,
    request: AppServerInteractiveRequest,
) -> anyhow::Result<Option<serde_json::Value>> {
    match request {
        AppServerInteractiveRequest::McpElicitation(request) => {
            request_mcp_elicitation(cx, session_id, request).await
        }
        AppServerInteractiveRequest::UserInput(request) => {
            request_user_input(cx, session_id, request).await
        }
        AppServerInteractiveRequest::DynamicToolCall(request) => {
            request_dynamic_tool_call(cx, session_id, request).await
        }
    }
}

async fn request_mcp_elicitation(
    cx: &ConnectionTo<Client>,
    session_id: SessionId,
    request: AppServerMcpElicitationRequest,
) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(elicitation_request) = acp_elicitation_request(&session_id, &request) else {
        return Ok(None);
    };

    match cx.send_request(elicitation_request).block_task().await {
        Ok(response) => Ok(Some(app_server_elicitation_response(response.action))),
        Err(error) => {
            debug!(
                server_name = %request.server_name,
                mode = %request.mode,
                %error,
                "ACP client did not complete elicitation request; using app-server fallback"
            );
            Ok(None)
        }
    }
}

fn acp_elicitation_request(
    session_id: &SessionId,
    request: &AppServerMcpElicitationRequest,
) -> Option<CreateElicitationRequest> {
    let scope = ElicitationSessionScope::new(session_id.clone());
    let mode = match request.mode.as_str() {
        "form" | "openai/form" => {
            let schema = request.requested_schema.as_ref().and_then(|schema| {
                serde_json::from_value::<ElicitationSchema>(schema.clone()).ok()
            })?;
            ElicitationMode::Form(ElicitationFormMode::new(scope, schema))
        }
        "url" => {
            let elicitation_id = request.elicitation_id.as_ref()?;
            let url = request.url.as_ref()?;
            ElicitationMode::Url(ElicitationUrlMode::new(
                scope,
                elicitation_id.clone(),
                url.clone(),
            ))
        }
        _ => return None,
    };

    Some(CreateElicitationRequest::new(mode, request.message.clone()))
}

async fn request_user_input(
    cx: &ConnectionTo<Client>,
    session_id: SessionId,
    request: AppServerUserInputRequest,
) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(elicitation_request) = user_input_elicitation_request(&session_id, &request) else {
        return Ok(None);
    };

    match cx.send_request(elicitation_request).block_task().await {
        Ok(response) => Ok(Some(app_server_user_input_response(response.action))),
        Err(error) => {
            debug!(
                method = %request.method,
                item_id = request.item_id.as_deref().unwrap_or(""),
                %error,
                "ACP client did not complete user input request; using app-server fallback"
            );
            Ok(None)
        }
    }
}

fn user_input_elicitation_request(
    session_id: &SessionId,
    request: &AppServerUserInputRequest,
) -> Option<CreateElicitationRequest> {
    if request.questions.is_empty() {
        return None;
    }

    let mut scope = ElicitationSessionScope::new(session_id.clone());
    if let Some(item_id) = &request.item_id {
        scope = scope.tool_call_id(ToolCallId::new(item_id.clone()));
    }

    let mut schema = ElicitationSchema::new()
        .title("Additional input")
        .description("Codex needs additional input to continue.");
    for question in &request.questions {
        schema = schema.property(
            question.id.clone(),
            user_input_question_schema(question),
            true,
        );
    }

    let message = if request.questions.len() == 1 {
        request.questions[0].question.clone()
    } else {
        "Codex needs additional input to continue.".to_owned()
    };
    Some(CreateElicitationRequest::new(
        ElicitationMode::Form(ElicitationFormMode::new(scope, schema)),
        message,
    ))
}

fn user_input_question_schema(question: &AppServerUserInputQuestion) -> StringPropertySchema {
    let mut schema = StringPropertySchema::new()
        .title(question.header.clone())
        .description(question.question.clone());
    if !question.options.is_empty() {
        schema = schema.one_of(Some(
            question
                .options
                .iter()
                .map(|option| EnumOption::new(option.label.clone(), option.label.clone()))
                .collect(),
        ));
    }
    schema
}

fn app_server_elicitation_response(action: ElicitationAction) -> serde_json::Value {
    match action {
        ElicitationAction::Accept(action) => serde_json::json!({
            "action": "accept",
            "content": action.content.map(elicitation_content_map_to_json),
        }),
        ElicitationAction::Decline => serde_json::json!({
            "action": "decline",
            "content": null,
        }),
        ElicitationAction::Cancel => serde_json::json!({
            "action": "cancel",
            "content": null,
        }),
        _ => serde_json::json!({
            "action": "cancel",
            "content": null,
        }),
    }
}

fn app_server_user_input_response(action: ElicitationAction) -> serde_json::Value {
    match action {
        ElicitationAction::Accept(action) => serde_json::json!({
            "answers": action
                .content
                .map(elicitation_content_map_to_json)
                .unwrap_or_else(|| serde_json::json!({})),
        }),
        ElicitationAction::Decline | ElicitationAction::Cancel => serde_json::json!({
            "answers": {},
        }),
        _ => serde_json::json!({
            "answers": {},
        }),
    }
}

async fn request_dynamic_tool_call(
    cx: &ConnectionTo<Client>,
    session_id: SessionId,
    request: AppServerDynamicToolCallRequest,
) -> anyhow::Result<Option<serde_json::Value>> {
    let params = json!({
        "sessionId": session_id,
        "threadId": request.thread_id,
        "turnId": request.turn_id,
        "callId": request.call_id,
        "namespace": request.namespace,
        "tool": request.tool,
        "arguments": request.arguments,
        "appServerRequest": request.raw,
    });
    let raw_params = serde_json::value::to_raw_value(&params)
        .map(Into::into)
        .map_err(|error| anyhow::anyhow!("failed to encode dynamic tool request: {error}"))?;
    let request =
        AgentRequest::ExtMethodRequest(ExtRequest::new(DYNAMIC_TOOL_CALL_METHOD, raw_params));

    match cx.send_request(request).block_task().await {
        Ok(response) => {
            if is_dynamic_tool_call_response(&response) {
                Ok(Some(response))
            } else {
                debug!(
                    method = DYNAMIC_TOOL_CALL_METHOD,
                    ?response,
                    "ACP client returned invalid dynamic tool response; using app-server fallback"
                );
                Ok(None)
            }
        }
        Err(error) => {
            debug!(
                method = DYNAMIC_TOOL_CALL_METHOD,
                %error,
                "ACP client did not complete dynamic tool request; using app-server fallback"
            );
            Ok(None)
        }
    }
}

fn is_dynamic_tool_call_response(response: &serde_json::Value) -> bool {
    response
        .get("contentItems")
        .and_then(serde_json::Value::as_array)
        .is_some()
        && response
            .get("success")
            .and_then(serde_json::Value::as_bool)
            .is_some()
}

fn elicitation_content_map_to_json(
    content: BTreeMap<String, ElicitationContentValue>,
) -> serde_json::Value {
    serde_json::Value::Object(
        content
            .into_iter()
            .map(|(key, value)| {
                let value = serde_json::to_value(value).unwrap_or(serde_json::Value::Null);
                (key, value)
            })
            .collect(),
    )
}

fn approval_cancel_key(session_id: &SessionId, item_id: &str) -> String {
    format!("{}:{item_id}", session_id.0.as_ref())
}

async fn cancel_active_prompts(active_prompts: &CancelSignals) -> usize {
    let prompts = {
        let mut active_prompts = active_prompts.lock().await;
        active_prompts.drain().collect::<Vec<_>>()
    };
    let count = prompts.len();
    for (_, cancel) in prompts {
        let _ = cancel.send(());
    }
    count
}

async fn cancel_outstanding_approvals(outstanding_approvals: &CancelSignals) -> usize {
    let approvals = {
        let mut outstanding_approvals = outstanding_approvals.lock().await;
        outstanding_approvals.drain().collect::<Vec<_>>()
    };
    let count = approvals.len();
    for (_, cancel) in approvals {
        let _ = cancel.send(());
    }
    count
}

async fn cancel_outstanding_approvals_for_session(
    outstanding_approvals: &CancelSignals,
    session_id: &SessionId,
) -> usize {
    let prefix = format!("{}:", session_id.0.as_ref());
    let approvals = {
        let mut outstanding_approvals = outstanding_approvals.lock().await;
        let keys = outstanding_approvals
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|key| outstanding_approvals.remove(&key))
            .collect::<Vec<_>>()
    };
    let count = approvals.len();
    for cancel in approvals {
        let _ = cancel.send(());
    }
    count
}

fn permission_option(choice: AppServerApprovalChoice) -> PermissionOption {
    let permission_option = PermissionOption::new(
        PermissionOptionId::new(choice.id()),
        choice.label(),
        permission_option_kind(&choice),
    );
    if let Some(available_decision) = choice.available_decision {
        let adapter_meta =
            serde_json::Map::from_iter([("availableDecision".to_owned(), available_decision)]);
        let meta = serde_json::Map::from_iter([(
            "brokk_codex_acp".to_owned(),
            serde_json::Value::Object(adapter_meta),
        )]);
        permission_option.meta(meta)
    } else {
        permission_option
    }
}

fn permission_option_kind(choice: &AppServerApprovalChoice) -> PermissionOptionKind {
    match choice.option {
        AppServerApprovalOption::Accept => PermissionOptionKind::AllowOnce,
        AppServerApprovalOption::AcceptForSession => PermissionOptionKind::AllowAlways,
        AppServerApprovalOption::AcceptWithExecpolicyAmendment => PermissionOptionKind::AllowAlways,
        AppServerApprovalOption::ApplyNetworkPolicyAmendment => {
            if network_policy_amendment_action(choice) == Some("deny") {
                PermissionOptionKind::RejectAlways
            } else {
                PermissionOptionKind::AllowAlways
            }
        }
        AppServerApprovalOption::Decline => PermissionOptionKind::RejectOnce,
        AppServerApprovalOption::Cancel => PermissionOptionKind::RejectOnce,
    }
}

fn permission_request_content(approval: &AppServerApprovalRequest) -> Option<Vec<ToolCallContent>> {
    let AppServerApprovalResponseKind::Permissions {
        requested_permissions,
    } = &approval.response_kind
    else {
        return None;
    };

    Some(vec![text_tool_content(permission_request_text(
        &approval.raw,
        requested_permissions,
    ))])
}

fn permission_request_text(raw: &serde_json::Value, permissions: &serde_json::Value) -> String {
    let mut lines = vec!["Requested permissions:".to_owned()];

    if let Some(reason) = json_string_field(raw, "reason") {
        lines.push(format!("Reason: {reason}"));
    }
    if let Some(environment_id) = json_string_field(raw, "environmentId") {
        lines.push(format!("Environment: {environment_id}"));
    }
    if let Some(cwd) = json_string_field(raw, "cwd") {
        lines.push(format!("Working directory: {cwd}"));
    }

    let permission_lines = permission_detail_lines(permissions);
    if permission_lines.is_empty() {
        lines.push("- No additional permissions requested.".to_owned());
    } else {
        lines.extend(permission_lines);
    }

    lines.join("\n")
}

fn permission_detail_lines(permissions: &serde_json::Value) -> Vec<String> {
    let mut lines = Vec::new();

    if permissions
        .get("network")
        .and_then(|network| network.get("enabled"))
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        lines.push("- Network access".to_owned());
    }

    if let Some(file_system) = permissions.get("fileSystem") {
        lines.extend(permission_path_lines(
            file_system,
            "read",
            "Filesystem read access",
        ));
        lines.extend(permission_path_lines(
            file_system,
            "write",
            "Filesystem write access",
        ));
    }

    lines
}

fn permission_path_lines(file_system: &serde_json::Value, field: &str, label: &str) -> Vec<String> {
    file_system
        .get(field)
        .and_then(serde_json::Value::as_array)
        .map(|paths| {
            paths
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(|path| format!("- {label}: {path}"))
                .collect()
        })
        .unwrap_or_default()
}

fn json_string_field(value: &serde_json::Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

fn network_policy_amendment_action(choice: &AppServerApprovalChoice) -> Option<&str> {
    let decision = choice.available_decision.as_ref()?;
    let amendment = decision
        .get("applyNetworkPolicyAmendment")?
        .get("network_policy_amendment")
        .or_else(|| {
            decision
                .get("applyNetworkPolicyAmendment")?
                .get("networkPolicyAmendment")
        })?;
    amendment.get("action")?.as_str()
}

fn publish_agent_message(
    session_id: &SessionId,
    message: String,
    cx: &ConnectionTo<Client>,
) -> anyhow::Result<()> {
    send_session_update(
        cx,
        session_id.clone(),
        SessionUpdate::AgentMessageChunk(text_chunk(message)),
    )
}

fn warning_message(update: &AppServerWarningUpdate) -> String {
    format!("Codex warning: {}", update.message)
}

fn config_warning_message(update: &AppServerConfigWarningUpdate) -> String {
    let mut message = format!("Codex config warning: {}", update.summary);
    if let Some(details) = update.details.as_deref()
        && !details.trim().is_empty()
    {
        message.push_str("\n\n");
        message.push_str(details);
    }
    if let Some(path) = update.path.as_deref()
        && !path.trim().is_empty()
    {
        message.push_str("\n\nPath: ");
        message.push_str(path);
    }
    if let Some(range) = update.range.as_ref() {
        message.push_str("\n\nRange: ");
        message.push_str(&json_value_label(range));
    }
    message
}

fn windows_sandbox_setup_message(update: &AppServerWindowsSandboxSetupUpdate) -> String {
    let status = if update.success {
        "completed"
    } else {
        "failed"
    };
    let mut message = format!("Windows sandbox `{}` setup {status}.", update.mode);
    if let Some(error) = update.error.as_deref()
        && !error.trim().is_empty()
    {
        message.push_str("\n\n");
        message.push_str(error);
    }
    message
}

fn account_login_completed_message(update: &AppServerAccountLoginCompletedUpdate) -> String {
    let status = if update.success {
        "completed"
    } else {
        "failed"
    };
    let mut message = "Codex account login ".to_owned();
    message.push_str(status);
    if let Some(login_id) = update.login_id.as_deref()
        && !login_id.trim().is_empty()
    {
        message.push_str(" for `");
        message.push_str(login_id);
        message.push('`');
    }
    message.push('.');
    if let Some(error) = update.error.as_deref()
        && !error.trim().is_empty()
    {
        message.push_str("\n\n");
        message.push_str(error);
    }
    message
}

fn account_updated_message(update: &AppServerAccountUpdatedUpdate) -> String {
    let auth_mode = update.auth_mode.as_deref().unwrap_or("signed out");
    let mut message = format!(
        "Codex account updated: auth mode {}.",
        json_value_label(&serde_json::Value::String(auth_mode.to_owned()))
    );
    if let Some(plan_type) = update.plan_type.as_deref()
        && !plan_type.trim().is_empty()
    {
        message.push_str("\n\nPlan: ");
        message.push_str(&json_value_label(&serde_json::Value::String(
            plan_type.to_owned(),
        )));
    }
    message
}

fn account_rate_limits_updated_message(update: &AppServerAccountRateLimitsUpdatedUpdate) -> String {
    format!(
        "Codex account rate limits updated: {}.",
        json_value_label(&update.rate_limits)
    )
}

fn mcp_oauth_login_completed_message(
    update: &AppServerMcpServerOAuthLoginCompletedUpdate,
) -> String {
    let status = if update.success {
        "completed"
    } else {
        "failed"
    };
    let mut message = format!("MCP server `{}` OAuth login {status}.", update.name);
    if let Some(error) = update.error.as_deref()
        && !error.trim().is_empty()
    {
        message.push_str("\n\n");
        message.push_str(error);
    }
    message
}

fn app_list_updated_message(update: &AppServerAppListUpdate) -> String {
    let count = update.data.as_array().map_or(0, Vec::len);
    let entry_label = if count == 1 { "entry" } else { "entries" };
    format!("Codex app list updated: {count} {entry_label}.")
}

fn remote_control_status_message(update: &AppServerRemoteControlStatusUpdate) -> String {
    let mut parts = vec![format!(
        "Codex remote control status: {} on `{}`.",
        update.status, update.server_name
    )];
    if let Some(environment_id) = update.environment_id.as_deref() {
        parts.push(format!("Environment: `{environment_id}`."));
    }
    parts.join(" ")
}

fn fuzzy_file_search_message(update: &AppServerFuzzyFileSearchUpdate) -> String {
    match update {
        AppServerFuzzyFileSearchUpdate::SessionUpdated {
            session_id,
            query,
            files,
        } => {
            let result_count = files.as_array().map(Vec::len);
            let result_summary = result_count
                .map(|count| {
                    let label = if count == 1 { "result" } else { "results" };
                    format!("{count} {label}")
                })
                .unwrap_or_else(|| "unknown results".to_owned());
            format!(
                "Codex fuzzy file search `{session_id}` updated for `{query}`: {result_summary}."
            )
        }
        AppServerFuzzyFileSearchUpdate::SessionCompleted { session_id, query } => {
            format!("Codex fuzzy file search `{session_id}` completed for `{query}`.")
        }
    }
}

fn error_message(update: &AppServerErrorUpdate) -> String {
    let mut message = if update.will_retry {
        format!("Codex error (retrying): {}", update.message)
    } else {
        format!("Codex error: {}", update.message)
    };
    if let Some(details) = update.additional_details.as_deref()
        && !details.trim().is_empty()
    {
        message.push_str("\n\n");
        message.push_str(details);
    }
    if let Some(info) = update.codex_error_info.as_ref() {
        message.push_str("\n\nCode: ");
        message.push_str(&json_value_label(info));
    }
    message
}

fn guardian_approval_review_message(update: &AppServerGuardianApprovalReviewUpdate) -> String {
    let lifecycle = match update.lifecycle {
        AppServerGuardianApprovalReviewLifecycle::Started => "started",
        AppServerGuardianApprovalReviewLifecycle::Completed => "completed",
    };
    let mut parts = vec![format!(
        "Codex auto-approval review `{}` {lifecycle}.",
        update.review_id
    )];
    if let Some(target_item_id) = update.target_item_id.as_deref() {
        parts.push(format!("Target item: `{target_item_id}`."));
    }
    if let Some(action_type) = update
        .action
        .get("type")
        .and_then(serde_json::Value::as_str)
    {
        parts.push(format!("Action: {action_type}."));
    }
    if let Some(status) = update
        .review
        .get("status")
        .and_then(serde_json::Value::as_str)
    {
        parts.push(format!("Status: {status}."));
    }
    if let Some(risk_level) = update
        .review
        .get("riskLevel")
        .and_then(serde_json::Value::as_str)
    {
        parts.push(format!("Risk: {risk_level}."));
    }
    if let Some(rationale) = update
        .review
        .get("rationale")
        .and_then(serde_json::Value::as_str)
        .filter(|rationale| !rationale.trim().is_empty())
    {
        parts.push(format!("Rationale: {rationale}."));
    }
    if let Some(decision_source) = update.decision_source.as_deref() {
        parts.push(format!("Decision source: {decision_source}."));
    }
    if let (Some(started_at_ms), Some(completed_at_ms)) =
        (update.started_at_ms, update.completed_at_ms)
        && completed_at_ms >= started_at_ms
    {
        parts.push(format!("Duration: {} ms.", completed_at_ms - started_at_ms));
    }
    parts.join(" ")
}

fn server_request_resolved_message(update: &AppServerServerRequestResolvedUpdate) -> String {
    let mut parts = vec![format!(
        "Codex server request `{}` resolved.",
        update.request_id
    )];
    if let Some(turn_id) = update.turn_id.as_deref() {
        parts.push(format!("Turn: `{turn_id}`."));
    }
    parts.join(" ")
}

fn model_rerouted_message(update: &AppServerModelReroutedUpdate) -> String {
    format!(
        "Codex rerouted the model from `{}` to `{}` ({}) for this turn.",
        update.from_model,
        update.to_model,
        json_value_label(&update.reason)
    )
}

fn model_safety_buffering_message(update: &AppServerModelSafetyBufferingUpdate) -> String {
    let mut parts = vec![format!(
        "Codex safety buffering is active for model `{}`.",
        update.model
    )];
    if let Some(use_cases) = readable_label_list(update.use_cases.clone()) {
        parts.push(format!("Use cases: {use_cases}."));
    }
    if let Some(reasons) = readable_label_list(update.reasons.clone()) {
        parts.push(format!("Reasons: {reasons}."));
    }
    parts.join(" ")
}

fn model_verification_message(update: &AppServerModelVerificationUpdate) -> String {
    format!(
        "Codex requires additional verification: {}.",
        verification_summary(&update.verifications)
    )
}

fn verification_summary(verifications: &serde_json::Value) -> String {
    match verifications {
        serde_json::Value::Array(items) => {
            let labels = items.iter().map(json_value_label).collect::<Vec<_>>();
            readable_label_list(labels).unwrap_or_else(|| "unknown verification".to_owned())
        }
        other => json_value_label(other),
    }
}

fn readable_label_list(parts: Vec<String>) -> Option<String> {
    match parts.as_slice() {
        [] => None,
        [only] => Some(only.clone()),
        [first, second] => Some(format!("{first} and {second}")),
        _ => {
            let (last, rest) = parts.split_last()?;
            Some(format!("{}, and {last}", rest.join(", ")))
        }
    }
}

fn turn_moderation_metadata_message(update: &AppServerTurnModerationMetadataUpdate) -> String {
    format!(
        "Codex moderation metadata: {}.",
        json_value_label(&update.metadata)
    )
}

fn mcp_startup_status_message(update: &AppServerMcpServerStartupStatusUpdate) -> String {
    let status = json_value_label(&serde_json::Value::String(update.status.clone()));
    let mut message = format!("MCP server `{}` startup status: {status}.", update.name);
    if let Some(error) = update.error.as_deref()
        && !error.trim().is_empty()
    {
        message.push_str("\n\n");
        message.push_str(error);
    }
    message
}

fn realtime_message(update: &AppServerRealtimeUpdate) -> String {
    match update {
        AppServerRealtimeUpdate::Started {
            realtime_session_id,
            ..
        } => {
            if let Some(session_id) = realtime_session_id.as_deref()
                && !session_id.trim().is_empty()
            {
                format!("Codex realtime session started: `{session_id}`.")
            } else {
                "Codex realtime session started.".to_owned()
            }
        }
        AppServerRealtimeUpdate::Sdp { sdp, .. } => {
            format!("Codex realtime SDP answer received ({} bytes).", sdp.len())
        }
        AppServerRealtimeUpdate::ItemAdded { item, .. } => {
            format!("Codex realtime item added: {}.", json_value_label(item))
        }
        AppServerRealtimeUpdate::TranscriptDelta { role, delta, .. } => {
            format!("Codex realtime transcript delta ({role}): {delta}")
        }
        AppServerRealtimeUpdate::TranscriptDone { role, text, .. } => {
            format!("Codex realtime transcript complete ({role}): {text}")
        }
        AppServerRealtimeUpdate::OutputAudioDelta { audio, .. } => {
            format!(
                "Codex realtime output audio delta: {}.",
                realtime_audio_summary(audio)
            )
        }
        AppServerRealtimeUpdate::Error { message, .. } => {
            format!("Codex realtime error: {message}")
        }
        AppServerRealtimeUpdate::Closed { reason, .. } => {
            format!("Codex realtime session closed: {reason}.")
        }
    }
}

fn realtime_audio_summary(audio: &AppServerRealtimeAudioDelta) -> String {
    let mut parts = Vec::new();
    if let Some(data) = audio.data.as_deref() {
        parts.push(format!("{} encoded characters", data.len()));
    }
    if let Some(sample_rate) = audio.sample_rate {
        parts.push(format!("{sample_rate} Hz"));
    }
    if let Some(num_channels) = audio.num_channels {
        parts.push(format!("{num_channels} channels"));
    }
    if let Some(samples_per_channel) = audio.samples_per_channel {
        parts.push(format!("{samples_per_channel} samples per channel"));
    }
    readable_label_list(parts).unwrap_or_else(|| "unknown audio payload".to_owned())
}

fn json_value_label(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => humanize_identifier(value),
        other => other.to_string(),
    }
}

fn publish_catalog_message(
    session_id: &SessionId,
    title: &str,
    message: String,
    cx: &ConnectionTo<Client>,
) -> Result<PromptResponse, Error> {
    let message = if message.trim().is_empty() {
        format!("{title}: no entries found.")
    } else {
        message
    };
    send_session_update(
        cx,
        session_id.clone(),
        SessionUpdate::AgentMessageChunk(text_chunk(message)),
    )
    .map_err(acp_internal_error)?;
    Ok(PromptResponse::new(StopReason::EndTurn))
}

fn publish_config_options_for_command(
    session_id: &SessionId,
    config_options: Vec<SessionConfigOption>,
    message: &str,
    cx: &ConnectionTo<Client>,
) -> Result<PromptResponse, Error> {
    send_session_update(
        cx,
        session_id.clone(),
        SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(config_options)),
    )
    .map_err(acp_internal_error)?;
    send_session_update(
        cx,
        session_id.clone(),
        SessionUpdate::AgentMessageChunk(text_chunk(message.to_owned())),
    )
    .map_err(acp_internal_error)?;
    Ok(PromptResponse::new(StopReason::EndTurn))
}

fn catalog_summary(title: &str, value: &serde_json::Value) -> String {
    let entries = catalog_entries(value);
    if entries.is_empty() {
        return format!("{title}: no entries found.");
    }

    let mut lines = vec![format!("{title}: {} entries", entries.len())];
    lines.extend(
        entries
            .iter()
            .take(10)
            .map(|entry| format!("- {}", catalog_entry_label(entry))),
    );
    if entries.len() > 10 {
        lines.push(format!("- ... {} more", entries.len() - 10));
    }
    lines.join("\n")
}

fn config_summary(cwd: Option<&str>, value: &serde_json::Value) -> String {
    let config = value.get("config").unwrap_or(value);
    let mut lines = vec!["Config: effective values".to_owned()];
    if let Some(cwd) = cwd {
        lines.push(format!("- CWD: {cwd}"));
    }

    for (label, key) in [
        ("Model", "model"),
        ("Model provider", "model_provider"),
        ("Reasoning effort", "model_reasoning_effort"),
        ("Reasoning summary", "model_reasoning_summary"),
        ("Service tier", "service_tier"),
        ("Approval policy", "approval_policy"),
        ("Approvals reviewer", "approvals_reviewer"),
        ("Sandbox mode", "sandbox_mode"),
        ("Web search", "web_search"),
    ] {
        if let Some(field) = config.get(key)
            && !field.is_null()
        {
            lines.push(format!("- {label}: {}", compact_json(field)));
        }
    }

    if let Some(origins) = value.get("origins").and_then(serde_json::Value::as_object) {
        lines.push(format!("- Origins: {} entries", origins.len()));
    }
    if let Some(layers) = value.get("layers").and_then(serde_json::Value::as_array) {
        lines.push(format!("- Layers: {} entries", layers.len()));
    }

    lines.join("\n")
}

fn config_requirements_summary(value: &serde_json::Value) -> String {
    let mut lines = vec!["Config requirements: managed constraints".to_owned()];
    let Some(requirements) = value.get("requirements") else {
        if !value.is_null() {
            lines.push(format!("- Response: {}", compact_json(value)));
        }
        return lines.join("\n");
    };
    if requirements.is_null() {
        lines.push("- Requirements: none configured".to_owned());
        return lines.join("\n");
    }

    for (label, key) in [
        ("Allowed approval policies", "allowedApprovalPolicies"),
        ("Allowed approvals reviewers", "allowedApprovalsReviewers"),
        ("Allowed sandbox modes", "allowedSandboxModes"),
        (
            "Allowed Windows sandbox implementations",
            "allowedWindowsSandboxImplementations",
        ),
        ("Allowed web search modes", "allowedWebSearchModes"),
        ("Allowed permission profiles", "allowedPermissionProfiles"),
        ("Default permissions", "defaultPermissions"),
        ("Allow managed hooks only", "allowManagedHooksOnly"),
        ("Allow appshots", "allowAppshots"),
        ("Allow remote control", "allowRemoteControl"),
        ("Computer use", "computerUse"),
        ("Feature requirements", "featureRequirements"),
        ("Hooks", "hooks"),
        ("Enforce residency", "enforceResidency"),
        ("Network", "network"),
    ] {
        if let Some(field) = requirements.get(key)
            && !field.is_null()
        {
            lines.push(format!("- {label}: {}", compact_json(field)));
        }
    }

    if lines.len() == 1 {
        lines.push(format!("- Requirements: {}", compact_json(requirements)));
    }

    lines.join("\n")
}

fn account_summary(value: &serde_json::Value) -> String {
    let mut lines = vec!["Account: current status".to_owned()];

    if let Some(requires_auth) = value
        .get("requiresOpenaiAuth")
        .and_then(serde_json::Value::as_bool)
    {
        lines.push(format!("- Requires OpenAI auth: {requires_auth}"));
    }

    match value.get("account") {
        Some(serde_json::Value::Null) | None => lines.push("- Account: not signed in".to_owned()),
        Some(account) => {
            if let Some(account_type) = account.get("type").and_then(serde_json::Value::as_str) {
                lines.push(format!(
                    "- Auth mode: {}",
                    humanize_identifier(account_type)
                ));
            } else {
                lines.push(format!("- Account: {}", compact_json(account)));
            }

            for (label, key) in [
                ("Email", "email"),
                ("Plan type", "planType"),
                ("Credential source", "credentialSource"),
            ] {
                if let Some(field) = account.get(key)
                    && !field.is_null()
                {
                    lines.push(format!("- {label}: {}", compact_json(field)));
                }
            }
        }
    }

    lines.join("\n")
}

fn rate_limits_summary(value: &serde_json::Value) -> String {
    let rate_limits = value.get("rateLimits").unwrap_or(value);
    let mut lines = vec!["Rate limits: current account".to_owned()];

    for (label, key) in [
        ("Primary", "primary"),
        ("Secondary", "secondary"),
        ("Limits", "limits"),
        ("Usage", "usage"),
        ("Plan type", "planType"),
        ("Resets at", "resetsAt"),
        ("Reset at", "resetAt"),
    ] {
        if let Some(field) = rate_limits.get(key)
            && !field.is_null()
        {
            lines.push(format!("- {label}: {}", compact_json(field)));
        }
    }

    if lines.len() == 1 && !rate_limits.is_null() {
        lines.push(format!("- Response: {}", compact_json(rate_limits)));
    }

    lines.join("\n")
}

fn account_usage_summary(value: &serde_json::Value) -> String {
    let summary = value.get("summary").unwrap_or(value);
    let mut lines = vec!["Usage: account token activity".to_owned()];

    for (label, key) in [
        ("Lifetime tokens", "lifetimeTokens"),
        ("Peak daily tokens", "peakDailyTokens"),
        ("Longest running turn seconds", "longestRunningTurnSec"),
        ("Current streak days", "currentStreakDays"),
        ("Longest streak days", "longestStreakDays"),
    ] {
        if let Some(field) = summary.get(key)
            && !field.is_null()
        {
            lines.push(format!("- {label}: {}", compact_json(field)));
        }
    }

    if let Some(buckets) = value
        .get("dailyUsageBuckets")
        .and_then(serde_json::Value::as_array)
    {
        lines.push(format!("- Daily buckets: {} entries", buckets.len()));
        for bucket in buckets.iter().take(5) {
            let start_date = bucket
                .get("startDate")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unknown>");
            if let Some(tokens) = bucket.get("tokens") {
                lines.push(format!("- {start_date}: {} tokens", compact_json(tokens)));
            } else {
                lines.push(format!("- {}", compact_json(bucket)));
            }
        }
        if buckets.len() > 5 {
            lines.push(format!("- ... {} more", buckets.len() - 5));
        }
    }

    if lines.len() == 1 && !value.is_null() {
        lines.push(format!("- Response: {}", compact_json(value)));
    }

    lines.join("\n")
}

fn workspace_messages_summary(value: &serde_json::Value) -> String {
    let mut lines = vec!["Workspace messages: active messages".to_owned()];

    if let Some(enabled) = value
        .get("featureEnabled")
        .and_then(serde_json::Value::as_bool)
    {
        lines.push(format!("- Feature enabled: {enabled}"));
    }

    let Some(messages) = value.get("messages").and_then(serde_json::Value::as_array) else {
        if !value.is_null() {
            lines.push(format!("- Response: {}", compact_json(value)));
        }
        return lines.join("\n");
    };

    lines.push(format!("- Messages: {} entries", messages.len()));
    for message in messages.iter().take(5) {
        let message_type = message
            .get("messageType")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("message");
        let body = message
            .get("messageBody")
            .and_then(serde_json::Value::as_str)
            .map(|body| truncate_for_summary(body, 160))
            .unwrap_or_else(|| compact_json(message));
        lines.push(format!("- {}: {}", humanize_identifier(message_type), body));
    }
    if messages.len() > 5 {
        lines.push(format!("- ... {} more", messages.len() - 5));
    }

    lines.join("\n")
}

fn experimental_features_summary(
    response: &crate::app_server::ExperimentalFeatureListResponse,
) -> String {
    if response.data.is_empty() {
        return "Features: no entries found.".to_owned();
    }

    let mut lines = vec![format!("Features: {} entries", response.data.len())];
    lines.extend(response.data.iter().take(10).map(|feature| {
        let label = feature
            .display_name
            .as_deref()
            .unwrap_or(feature.name.as_str());
        let state = if feature.enabled {
            "enabled"
        } else {
            "disabled"
        };
        let default_state = if feature.default_enabled {
            "default enabled"
        } else {
            "default disabled"
        };
        let mut line = format!(
            "- {} (`{}`): {} ({}, {})",
            label,
            feature.name,
            state,
            humanize_identifier(&feature.stage),
            default_state
        );
        if let Some(description) = feature.description.as_deref() {
            line.push_str(&format!(" - {description}"));
        }
        line
    }));
    if response.data.len() > 10 {
        lines.push(format!("- ... {} more", response.data.len() - 10));
    }
    if let Some(next_cursor) = response.next_cursor.as_deref() {
        lines.push(format!("Next cursor: {next_cursor}"));
    }
    lines.join("\n")
}

fn experimental_feature_enablement_summary(
    name: &str,
    enabled: bool,
    response: &crate::app_server::ExperimentalFeatureEnablementSetResponse,
) -> String {
    match response.enablement.get(name) {
        Some(actual) => {
            let state = if *actual { "enabled" } else { "disabled" };
            format!("Feature `{name}` is now {state} for this Codex app-server process.")
        }
        None => {
            let requested = if enabled { "enable" } else { "disable" };
            format!(
                "Codex did not accept the request to {requested} feature `{name}`. It may be unknown or managed by policy."
            )
        }
    }
}

fn plugin_detail_summary(value: &serde_json::Value) -> String {
    let plugin_name = first_string_at_paths(
        value,
        &[
            &["displayName"][..],
            &["name"],
            &["manifest", "displayName"],
            &["manifest", "name"],
        ],
    )
    .unwrap_or("Plugin details");
    let mut lines = vec![format!("Plugin: {plugin_name}")];

    if let Some(marketplace) =
        first_string_at_paths(value, &[&["marketplacePath"][..], &["marketplaceName"]])
    {
        lines.push(format!("Marketplace: {marketplace}"));
    }
    if let Some(description) = first_string_at_paths(
        value,
        &[
            &["description"][..],
            &["manifest", "description"],
            &["interface", "description"],
            &["interface", "shortDescription"],
        ],
    ) {
        lines.push(format!("Description: {description}"));
    }

    append_plugin_entries(&mut lines, "Summary", value.get("summary"));
    append_plugin_entries(&mut lines, "Apps", value.get("apps"));
    append_plugin_entries(&mut lines, "Skills", value.get("skills"));
    append_plugin_entries(&mut lines, "Hooks", value.get("hooks"));
    append_plugin_entries(&mut lines, "MCP servers", value.get("mcpServers"));

    lines.join("\n")
}

fn plugin_install_summary(plugin_name: &str, value: &serde_json::Value) -> String {
    let mut lines = vec![format!("Installed Codex plugin `{plugin_name}`.")];
    if let Some(policy) = value.get("authPolicy") {
        lines.push(format!("- Auth policy: {}", compact_json(policy)));
    }
    append_plugin_entries(
        &mut lines,
        "Apps needing auth",
        value.get("appsNeedingAuth"),
    );
    lines.join("\n")
}

fn marketplace_add_summary(response: &crate::app_server::MarketplaceAddResponse) -> String {
    let action = if response.already_added {
        "already configured"
    } else {
        "added"
    };
    format!(
        "Marketplace `{}` {action} at `{}`.",
        response.marketplace_name, response.installed_root
    )
}

fn marketplace_remove_summary(response: &crate::app_server::MarketplaceRemoveResponse) -> String {
    match response.installed_root.as_deref() {
        Some(installed_root) => format!(
            "Removed Codex marketplace `{}` from `{installed_root}`.",
            response.marketplace_name
        ),
        None => format!("Removed Codex marketplace `{}`.", response.marketplace_name),
    }
}

fn first_string_at_paths<'a>(value: &'a serde_json::Value, paths: &[&[&str]]) -> Option<&'a str> {
    paths
        .iter()
        .find_map(|path| value_at_path(value, path).and_then(serde_json::Value::as_str))
}

fn value_at_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}

fn append_plugin_entries(lines: &mut Vec<String>, title: &str, value: Option<&serde_json::Value>) {
    let Some(entries) = value.and_then(serde_json::Value::as_array) else {
        return;
    };
    if entries.is_empty() {
        return;
    }

    lines.push(format!("{title}: {} entries", entries.len()));
    lines.extend(
        entries
            .iter()
            .take(10)
            .map(|entry| format!("- {}", catalog_entry_label(entry))),
    );
    if entries.len() > 10 {
        lines.push(format!("- ... {} more", entries.len() - 10));
    }
}

fn mcp_reload_summary(value: &serde_json::Value) -> String {
    format!(
        "Reloaded Codex MCP server configuration.\n\n{}",
        catalog_summary("MCP", value)
    )
}

fn mcp_resource_summary(server: &str, uri: &str, value: &serde_json::Value) -> String {
    let mut lines = vec![
        "MCP resource".to_owned(),
        format!("- Server: {server}"),
        format!("- URI: {uri}"),
    ];
    append_mcp_contents(&mut lines, value);
    lines.join("\n")
}

fn mcp_tool_summary(server: &str, tool: &str, value: &serde_json::Value) -> String {
    let mut lines = vec![
        "MCP tool".to_owned(),
        format!("- Server: {server}"),
        format!("- Tool: {tool}"),
    ];
    append_mcp_contents(&mut lines, value);
    lines.join("\n")
}

fn append_mcp_contents(lines: &mut Vec<String>, value: &serde_json::Value) {
    let contents = value
        .get("content")
        .or_else(|| value.get("contents"))
        .and_then(serde_json::Value::as_array);
    let Some(contents) = contents else {
        lines.push(format!("- Response: {}", compact_json(value)));
        return;
    };
    if contents.is_empty() {
        lines.push("- Contents: no entries found.".to_owned());
        return;
    }

    lines.push(format!("- Contents: {} entries", contents.len()));
    for content in contents.iter().take(3) {
        if let Some(text) = content.get("text").and_then(serde_json::Value::as_str) {
            lines.push(format!(
                "- Text: {}",
                truncate_for_summary(text.replace('\n', "\\n").as_str(), 240)
            ));
        } else if let Some(blob) = content.get("blob").and_then(serde_json::Value::as_str) {
            lines.push(format!("- Blob: {} encoded characters", blob.len()));
        } else {
            lines.push(format!("- {}", compact_json(content)));
        }
    }
    if contents.len() > 3 {
        lines.push(format!("- ... {} more", contents.len() - 3));
    }
}

fn truncate_for_summary(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let mut truncated = String::new();
    for _ in 0..max_chars {
        let Some(ch) = chars.next() else {
            return text.to_owned();
        };
        truncated.push(ch);
    }
    if chars.next().is_some() {
        truncated.push_str("...");
    }
    truncated
}

fn catalog_entries(value: &serde_json::Value) -> Vec<&serde_json::Value> {
    value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .or_else(|| value.as_array())
        .map(|entries| entries.iter().collect())
        .unwrap_or_default()
}

fn catalog_entry_label(entry: &serde_json::Value) -> String {
    for key in [
        "displayName",
        "name",
        "title",
        "id",
        "pluginId",
        "connectorId",
        "serverName",
        "cwd",
    ] {
        if let Some(value) = entry.get(key).and_then(serde_json::Value::as_str) {
            return value.to_owned();
        }
    }

    if let Some(object) = entry.as_object()
        && let Some((key, value)) = object.iter().next()
    {
        return format!("{key}: {}", compact_json(value));
    }

    compact_json(entry)
}

fn status_summary(thread_id: &str, cwd: &str, loaded_threads: &serde_json::Value) -> String {
    let loaded = catalog_entries(loaded_threads)
        .into_iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();
    let loaded_state = if loaded.contains(&thread_id) {
        "loaded"
    } else {
        "not reported as loaded"
    };

    format!(
        "Status\n- Thread: {thread_id}\n- Cwd: {cwd}\n- Loaded threads: {} ({loaded_state})",
        loaded.len()
    )
}

fn skill_roots_summary(roots: &[String]) -> String {
    let mut lines = vec![format!(
        "Skill roots updated for this app-server process: {} entries",
        roots.len()
    )];
    lines.extend(roots.iter().take(10).map(|root| format!("- {root}")));
    if roots.len() > 10 {
        lines.push(format!("- ... {} more", roots.len() - 10));
    }
    lines.join("\n")
}

fn compact_json(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unprintable>".to_owned())
}

fn text_chunk(text: String) -> ContentChunk {
    ContentChunk::new(ContentBlock::Text(TextContent::new(text)))
}

fn agent_message_chunk(message: AppServerAgentMessageDelta) -> ContentChunk {
    let chunk = text_chunk(message.delta);
    if let Some(item_id) = message.item_id {
        chunk.message_id(MessageId::new(item_id))
    } else {
        chunk
    }
}

fn text_tool_content(text: String) -> ToolCallContent {
    ToolCallContent::from(ContentBlock::Text(TextContent::new(text)))
}

fn diff_tool_content(diff: crate::app_server::AppServerFileDiff) -> ToolCallContent {
    ToolCallContent::from(Diff::new(diff.path, diff.diff))
}

fn skill_command(skill: AppServerSkill) -> AvailableCommand {
    let description = skill
        .interface
        .as_ref()
        .and_then(|interface| interface.short_description.clone())
        .or_else(|| skill.description.clone())
        .unwrap_or_else(|| format!("Invoke Codex skill {}", skill.name));

    let input_hint = skill
        .interface
        .as_ref()
        .and_then(|interface| interface.default_prompt.clone())
        .unwrap_or_else(|| format!("Instructions for {}", skill.name));

    AvailableCommand::new(format!("skill:{}", skill.name), description).input(
        AvailableCommandInput::Unstructured(UnstructuredCommandInput::new(input_hint)),
    )
}

fn builtin_commands() -> Vec<AvailableCommand> {
    BUILTIN_COMMAND_SPECS
        .iter()
        .filter(|spec| {
            matches!(
                spec.availability,
                CommandAvailability::RequiresSession | CommandAvailability::RequiresNoActiveTurn
            )
        })
        .map(|spec| {
            let command = AvailableCommand::new(spec.name, spec.description);
            if let Some(input_hint) = spec.input_hint {
                command.input(AvailableCommandInput::Unstructured(
                    UnstructuredCommandInput::new(input_hint),
                ))
            } else {
                command
            }
        })
        .collect()
}

fn available_commands(skills: Vec<AppServerSkill>) -> Vec<AvailableCommand> {
    let mut commands = builtin_commands();
    commands.extend(
        skills
            .into_iter()
            .filter(|skill| skill.enabled)
            .map(skill_command),
    );
    commands
}

fn tool_kind(kind: AppServerToolKind) -> ToolKind {
    match kind {
        AppServerToolKind::Read => ToolKind::Read,
        AppServerToolKind::Edit => ToolKind::Edit,
        AppServerToolKind::Search => ToolKind::Search,
        AppServerToolKind::Execute => ToolKind::Execute,
        AppServerToolKind::Think => ToolKind::Think,
        AppServerToolKind::Fetch => ToolKind::Fetch,
        AppServerToolKind::Other => ToolKind::Other,
    }
}

fn tool_status(status: AppServerToolStatus) -> ToolCallStatus {
    match status {
        AppServerToolStatus::Pending => ToolCallStatus::Pending,
        AppServerToolStatus::InProgress => ToolCallStatus::InProgress,
        AppServerToolStatus::Completed => ToolCallStatus::Completed,
        AppServerToolStatus::Failed => ToolCallStatus::Failed,
    }
}

fn plan_status(status: AppServerPlanStatus) -> PlanEntryStatus {
    match status {
        AppServerPlanStatus::Pending => PlanEntryStatus::Pending,
        AppServerPlanStatus::InProgress => PlanEntryStatus::InProgress,
        AppServerPlanStatus::Completed => PlanEntryStatus::Completed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_server::{
        AppServerCollaborationModeMask, AppServerReasoningEffortOption, AppServerServiceTier,
    };

    fn skill(name: &str, path: Option<&str>, enabled: bool) -> AppServerSkill {
        AppServerSkill {
            name: name.to_owned(),
            path: path.map(ToOwned::to_owned),
            description: None,
            enabled,
            interface: None,
        }
    }

    fn model(id: &str, display_name: &str, is_default: bool) -> AppServerModel {
        AppServerModel {
            id: id.to_owned(),
            model: Some(id.to_owned()),
            display_name: display_name.to_owned(),
            description: format!("{display_name} description"),
            supported_reasoning_efforts: vec![
                AppServerReasoningEffortOption {
                    reasoning_effort: "low".to_owned(),
                    description: "Fast".to_owned(),
                },
                AppServerReasoningEffortOption {
                    reasoning_effort: "high".to_owned(),
                    description: "Deep".to_owned(),
                },
            ],
            default_reasoning_effort: Some("high".to_owned()),
            service_tiers: vec![
                AppServerServiceTier {
                    id: "standard".to_owned(),
                    name: "Standard".to_owned(),
                    description: "Default speed".to_owned(),
                },
                AppServerServiceTier {
                    id: "priority".to_owned(),
                    name: "Priority".to_owned(),
                    description: "Faster".to_owned(),
                },
            ],
            default_service_tier: Some("standard".to_owned()),
            hidden: false,
            is_default,
        }
    }

    fn permission_profile(id: &str, description: Option<&str>) -> AppServerPermissionProfile {
        AppServerPermissionProfile {
            id: id.to_owned(),
            description: description.map(ToOwned::to_owned),
        }
    }

    fn collaboration_mode(id: &str, name: &str) -> AppServerCollaborationModeMask {
        AppServerCollaborationModeMask {
            name: name.to_owned(),
            mode: Some(id.to_owned()),
            model: None,
            reasoning_effort: None,
        }
    }

    #[test]
    fn prompt_input_adds_structured_skill_for_dollar_invocation() {
        let input = prompt_input_with_skills(
            "$skill-creator Make a test skill".to_owned(),
            &[skill(
                "skill-creator",
                Some("/skills/skill-creator/SKILL.md"),
                true,
            )],
        );

        assert!(matches!(
            &input[..],
            [
                AppServerTurnInput::Text { text },
                AppServerTurnInput::Skill { name, path },
            ] if text == "$skill-creator Make a test skill"
                && name == "skill-creator"
                && path == "/skills/skill-creator/SKILL.md"
        ));
    }

    #[test]
    fn prompt_input_converts_slash_skill_to_visible_dollar_invocation() {
        let input = prompt_input_with_skills(
            "/skill skill-creator Make a test skill".to_owned(),
            &[skill(
                "skill-creator",
                Some("/skills/skill-creator/SKILL.md"),
                true,
            )],
        );

        assert!(matches!(
            &input[..],
            [
                AppServerTurnInput::Text { text },
                AppServerTurnInput::Skill { name, path },
            ] if text == "$skill-creator Make a test skill"
                && name == "skill-creator"
                && path == "/skills/skill-creator/SKILL.md"
        ));
    }

    #[test]
    fn prompt_input_falls_back_to_text_without_usable_skill() {
        for skills in [
            vec![skill("other", Some("/skills/other/SKILL.md"), true)],
            vec![skill(
                "skill-creator",
                Some("/skills/skill-creator/SKILL.md"),
                false,
            )],
            vec![skill("skill-creator", None, true)],
        ] {
            let input =
                prompt_input_with_skills("$skill-creator Make a test skill".to_owned(), &skills);

            assert!(matches!(
                &input[..],
                [AppServerTurnInput::Text { text }]
                    if text == "$skill-creator Make a test skill"
            ));
        }
    }

    #[test]
    fn prompt_input_blocks_maps_images_to_app_server_input() {
        let input = prompt_input_blocks(vec![
            ContentBlock::Text(TextContent::new("describe this")),
            ContentBlock::Image(agent_client_protocol::schema::ImageContent::new(
                "iVBORw0KGgo=",
                "image/png",
            )),
        ])
        .unwrap();

        assert!(matches!(
            &input.input[..],
            [
                AppServerTurnInput::Text { text },
                AppServerTurnInput::Image { url },
            ] if text == "describe this" && url == "data:image/png;base64,iVBORw0KGgo="
        ));
        assert!(prompt_command_text(&input).is_none());
    }

    #[test]
    fn prompt_input_blocks_maps_embedded_text_resources_to_additional_context() {
        let input = prompt_input_blocks(vec![
            ContentBlock::Text(TextContent::new("summarize")),
            ContentBlock::Resource(agent_client_protocol::schema::EmbeddedResource::new(
                EmbeddedResourceResource::TextResourceContents(
                    agent_client_protocol::schema::TextResourceContents::new(
                        "Project notes",
                        "file:///notes.md",
                    )
                    .mime_type("text/markdown"),
                ),
            )),
        ])
        .unwrap();

        assert!(matches!(
            &input.input[..],
            [AppServerTurnInput::Text { text }]
                if text == "summarize\n@file:///notes.md"
        ));
        let additional_context = input
            .additional_context
            .as_ref()
            .expect("embedded resources should populate app-server additionalContext");
        assert_eq!(
            additional_context
                .get("file:///notes.md")
                .expect("resource URI should be used as context key")
                .value,
            "MIME type: text/markdown\n\nProject notes"
        );
    }

    #[test]
    fn prompt_starts_with_command_prefix_detects_multimodal_commands() {
        let input = prompt_input_blocks(vec![
            ContentBlock::Text(TextContent::new("/rename title")),
            ContentBlock::Image(agent_client_protocol::schema::ImageContent::new(
                "iVBORw0KGgo=",
                "image/png",
            )),
        ])
        .unwrap();

        assert!(prompt_starts_with_command_prefix(&input));
    }

    #[test]
    fn parse_builtin_command_recognizes_rename() {
        let command = parse_builtin_command("  /rename Current project title")
            .unwrap()
            .unwrap();

        assert!(matches!(
            command,
            BuiltinCommand::Rename { title } if title == "Current project title"
        ));
    }

    #[test]
    fn parse_builtin_command_recognizes_resume() {
        let command = parse_builtin_command("/resume Started Thread")
            .unwrap()
            .unwrap();

        assert!(matches!(
            command,
            BuiltinCommand::Resume { target } if target == "Started Thread"
        ));
    }

    #[test]
    fn parse_builtin_command_recognizes_archive() {
        let command = parse_builtin_command("/archive").unwrap().unwrap();

        assert!(matches!(command, BuiltinCommand::Archive));

        let command = parse_builtin_command("/unarchive").unwrap().unwrap();

        assert!(matches!(command, BuiltinCommand::Unarchive));
    }

    #[test]
    fn parse_builtin_command_recognizes_kill() {
        let command = parse_builtin_command("/kill 42").unwrap().unwrap();

        assert!(matches!(
            command,
            BuiltinCommand::Kill { process_id } if process_id == "42"
        ));
    }

    #[test]
    fn parse_builtin_command_recognizes_memory_variants() {
        let command = parse_builtin_command("/memory enable").unwrap().unwrap();
        assert!(matches!(
            command,
            BuiltinCommand::Memory {
                action: MemoryCommandAction::Enable
            }
        ));

        let command = parse_builtin_command("/memory disable").unwrap().unwrap();
        assert!(matches!(
            command,
            BuiltinCommand::Memory {
                action: MemoryCommandAction::Disable
            }
        ));

        let command = parse_builtin_command("/memory reset").unwrap().unwrap();
        assert!(matches!(
            command,
            BuiltinCommand::Memory {
                action: MemoryCommandAction::Reset
            }
        ));
    }

    #[test]
    fn parse_builtin_command_recognizes_rollback() {
        let command = parse_builtin_command("/rollback 2").unwrap().unwrap();

        assert!(matches!(
            command,
            BuiltinCommand::Rollback { num_turns } if num_turns == 2
        ));
    }

    #[test]
    fn parse_shell_command_recognizes_bang_command() {
        assert_eq!(
            parse_shell_command("  !echo hi").unwrap().as_deref(),
            Some("echo hi")
        );
        assert_eq!(parse_shell_command("echo hi").unwrap(), None);
    }

    #[test]
    fn parse_shell_command_rejects_empty_bang_command() {
        let error = parse_shell_command("!  ").unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("! shell command requires a command")
        );
    }

    #[test]
    fn parse_builtin_command_recognizes_turn_commands() {
        let command = parse_builtin_command("/compact").unwrap().unwrap();
        assert!(matches!(command, BuiltinCommand::Compact));

        let command = parse_builtin_command("/review").unwrap().unwrap();
        assert!(matches!(command, BuiltinCommand::Review));
    }

    #[test]
    fn parse_builtin_command_recognizes_catalog_commands() {
        for text in [
            "/account",
            "/apps",
            "/config /repo with spaces",
            "/config-requirements",
            "/delete",
            "/feature memories enable",
            "/features",
            "/fork",
            "/hooks",
            "/init",
            "/marketplace-add owner/repo main plugins,skills",
            "/marketplace-remove debug",
            "/mcp",
            "/mcp-reload",
            "/memory disable",
            "/mcp-resource filesystem file:///repo/README.md",
            r#"/mcp-tool filesystem read_file {"path":"/repo/README.md"}"#,
            "/model",
            "/new",
            "/permissions",
            "/plan",
            "/plugin github@openai",
            "/plugin-install github@openai",
            "/plugin-uninstall github@openai",
            "/plugins",
            "/ps",
            "/rate-limits",
            "/rollback 2",
            "/skill-roots /repo/.codex/skills,/shared/skills",
            "/status",
            "/stop",
            "/unarchive",
            "/usage",
            "/workspace-messages",
        ] {
            let command = parse_builtin_command(text).unwrap().unwrap();
            match text {
                "/account" => assert!(matches!(command, BuiltinCommand::Account)),
                "/apps" => assert!(matches!(command, BuiltinCommand::Apps)),
                "/config /repo with spaces" => assert!(matches!(
                    command,
                    BuiltinCommand::Config { cwd }
                        if cwd.as_deref() == Some("/repo with spaces")
                )),
                "/config-requirements" => {
                    assert!(matches!(command, BuiltinCommand::ConfigRequirements))
                }
                "/delete" => assert!(matches!(command, BuiltinCommand::Delete)),
                "/feature memories enable" => assert!(matches!(
                    command,
                    BuiltinCommand::Feature { name, enabled }
                        if name == "memories" && enabled
                )),
                "/features" => assert!(matches!(command, BuiltinCommand::Features)),
                "/fork" => assert!(matches!(command, BuiltinCommand::Fork)),
                "/hooks" => assert!(matches!(command, BuiltinCommand::Hooks)),
                "/init" => assert!(matches!(command, BuiltinCommand::Init)),
                "/marketplace-add owner/repo main plugins,skills" => assert!(matches!(
                    command,
                    BuiltinCommand::MarketplaceAdd {
                        source,
                        ref_name,
                        sparse_paths
                    } if source == "owner/repo"
                        && ref_name.as_deref() == Some("main")
                        && sparse_paths == Some(vec!["plugins".to_owned(), "skills".to_owned()])
                )),
                "/marketplace-remove debug" => assert!(matches!(
                    command,
                    BuiltinCommand::MarketplaceRemove { marketplace_name }
                        if marketplace_name == "debug"
                )),
                "/mcp" => assert!(matches!(command, BuiltinCommand::Mcp)),
                "/mcp-reload" => assert!(matches!(command, BuiltinCommand::McpReload)),
                "/memory disable" => assert!(matches!(
                    command,
                    BuiltinCommand::Memory {
                        action: MemoryCommandAction::Disable
                    }
                )),
                "/mcp-resource filesystem file:///repo/README.md" => assert!(matches!(
                    command,
                    BuiltinCommand::McpResource { server, uri }
                        if server == "filesystem" && uri == "file:///repo/README.md"
                )),
                r#"/mcp-tool filesystem read_file {"path":"/repo/README.md"}"# => {
                    assert!(matches!(
                        command,
                        BuiltinCommand::McpTool {
                            server,
                            tool,
                            arguments
                        } if server == "filesystem"
                            && tool == "read_file"
                            && arguments == serde_json::json!({"path": "/repo/README.md"})
                    ))
                }
                "/model" => assert!(matches!(command, BuiltinCommand::Model)),
                "/new" => assert!(matches!(command, BuiltinCommand::New)),
                "/permissions" => assert!(matches!(command, BuiltinCommand::Permissions)),
                "/plan" => assert!(matches!(command, BuiltinCommand::Plan)),
                "/plugin github@openai" => assert!(matches!(
                    command,
                    BuiltinCommand::Plugin {
                        plugin_name,
                        marketplace_path
                    } if plugin_name == "github" && marketplace_path == "openai"
                )),
                "/plugin-install github@openai" => assert!(matches!(
                    command,
                    BuiltinCommand::PluginInstall {
                        plugin_name,
                        marketplace_path
                    } if plugin_name == "github" && marketplace_path == "openai"
                )),
                "/plugin-uninstall github@openai" => assert!(matches!(
                    command,
                    BuiltinCommand::PluginUninstall { plugin_id } if plugin_id == "github@openai"
                )),
                "/plugins" => assert!(matches!(command, BuiltinCommand::Plugins)),
                "/ps" => assert!(matches!(command, BuiltinCommand::Ps)),
                "/rate-limits" => assert!(matches!(command, BuiltinCommand::RateLimits)),
                "/rollback 2" => assert!(matches!(
                    command,
                    BuiltinCommand::Rollback { num_turns } if num_turns == 2
                )),
                "/skill-roots /repo/.codex/skills,/shared/skills" => assert!(matches!(
                    command,
                    BuiltinCommand::SkillRoots { roots }
                        if roots == vec!["/repo/.codex/skills", "/shared/skills"]
                )),
                "/status" => assert!(matches!(command, BuiltinCommand::Status)),
                "/stop" => assert!(matches!(command, BuiltinCommand::Stop)),
                "/unarchive" => assert!(matches!(command, BuiltinCommand::Unarchive)),
                "/usage" => assert!(matches!(command, BuiltinCommand::Usage)),
                "/workspace-messages" => {
                    assert!(matches!(command, BuiltinCommand::WorkspaceMessages))
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn parse_builtin_command_recognizes_goal_variants() {
        let command = parse_builtin_command("/goal Improve ACP coverage")
            .unwrap()
            .unwrap();
        assert!(matches!(
            command,
            BuiltinCommand::GoalSet { objective } if objective == "Improve ACP coverage"
        ));

        let command = parse_builtin_command("/goal").unwrap().unwrap();
        assert!(matches!(command, BuiltinCommand::GoalGet));

        let command = parse_builtin_command("/goal get").unwrap().unwrap();
        assert!(matches!(command, BuiltinCommand::GoalGet));

        let command = parse_builtin_command("/goal clear").unwrap().unwrap();
        assert!(matches!(command, BuiltinCommand::GoalClear));
    }

    #[test]
    fn parse_builtin_command_rejects_empty_rename() {
        let error = parse_builtin_command("/rename").unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/rename requires a title")
        );
    }

    #[test]
    fn app_server_method_unavailable_maps_to_user_actionable_acp_error() {
        let error = acp_app_server_method_error(
            "thread/settings/update",
            crate::app_server::AppServerMethodUnavailable::new(
                "thread/settings/update".to_owned(),
                serde_json::json!({
                    "code": -32601,
                    "message": "Method not found",
                }),
            )
            .into(),
        );

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some(
                "Codex app-server method `thread/settings/update` is unavailable in this Codex version"
            )
        );
    }

    #[tokio::test]
    async fn cancel_active_prompts_drains_and_signals_all_prompts() {
        let active_prompts = Arc::new(Mutex::new(HashMap::new()));
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        {
            let mut active_prompts = active_prompts.lock().await;
            active_prompts.insert("thread-1".to_owned(), first_tx);
            active_prompts.insert("thread-2".to_owned(), second_tx);
        }

        let cancelled = cancel_active_prompts(&active_prompts).await;

        assert_eq!(cancelled, 2);
        assert!(active_prompts.lock().await.is_empty());
        assert!(first_rx.await.is_ok());
        assert!(second_rx.await.is_ok());
    }

    #[tokio::test]
    async fn cancel_outstanding_approvals_drains_and_signals_all_approvals() {
        let outstanding_approvals = Arc::new(Mutex::new(HashMap::new()));
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        {
            let mut outstanding_approvals = outstanding_approvals.lock().await;
            outstanding_approvals.insert("thread-1:item-1".to_owned(), first_tx);
            outstanding_approvals.insert("thread-2:item-2".to_owned(), second_tx);
        }

        let cancelled = cancel_outstanding_approvals(&outstanding_approvals).await;

        assert_eq!(cancelled, 2);
        assert!(outstanding_approvals.lock().await.is_empty());
        assert!(first_rx.await.is_ok());
        assert!(second_rx.await.is_ok());
    }

    #[tokio::test]
    async fn cancel_outstanding_approvals_for_session_only_signals_matching_session() {
        let outstanding_approvals = Arc::new(Mutex::new(HashMap::new()));
        let (matching_tx, matching_rx) = oneshot::channel();
        let (other_tx, mut other_rx) = oneshot::channel();
        {
            let mut outstanding_approvals = outstanding_approvals.lock().await;
            outstanding_approvals.insert("thread-1:item-1".to_owned(), matching_tx);
            outstanding_approvals.insert("thread-2:item-2".to_owned(), other_tx);
        }

        let cancelled = cancel_outstanding_approvals_for_session(
            &outstanding_approvals,
            &SessionId::new("thread-1"),
        )
        .await;

        assert_eq!(cancelled, 1);
        assert!(matching_rx.await.is_ok());
        assert!(other_rx.try_recv().is_err());
        let remaining = outstanding_approvals.lock().await;
        assert!(remaining.contains_key("thread-2:item-2"));
        assert!(!remaining.contains_key("thread-1:item-1"));
    }

    #[test]
    fn parse_builtin_command_rejects_empty_resume() {
        let error = parse_builtin_command("/resume").unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/resume requires a thread id or name")
        );
    }

    #[test]
    fn parse_builtin_command_rejects_empty_kill() {
        let error = parse_builtin_command("/kill").unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/kill requires a process id")
        );
    }

    #[test]
    fn parse_builtin_command_rejects_invalid_memory_action() {
        let error = parse_builtin_command("/memory").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/memory requires `enable`, `disable`, or `reset`")
        );

        let error = parse_builtin_command("/memory maybe").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/memory accepts only `enable`, `disable`, or `reset`")
        );
    }

    #[test]
    fn parse_builtin_command_rejects_invalid_feature_action() {
        let error = parse_builtin_command("/feature").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/feature requires a feature name and `enable` or `disable`")
        );

        let error = parse_builtin_command("/feature memories").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/feature requires a feature name and `enable` or `disable`")
        );

        let error = parse_builtin_command("/feature memories maybe").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/feature action must be `enable` or `disable`")
        );

        let error = parse_builtin_command("/feature memories enable extra").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/feature accepts only a feature name and one action")
        );
    }

    #[test]
    fn parse_builtin_command_rejects_invalid_marketplace_arguments() {
        let error = parse_builtin_command("/marketplace-add").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/marketplace-add requires a source, optional ref, and optional sparse paths")
        );

        let error =
            parse_builtin_command("/marketplace-add owner/repo main plugins extra").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/marketplace-add accepts only source, optional ref, and optional sparse paths")
        );

        let error = parse_builtin_command("/marketplace-add owner/repo main ,").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("comma-separated values cannot be empty")
        );

        for text in ["/marketplace-remove", "/marketplace-remove debug extra"] {
            let error = parse_builtin_command(text).unwrap_err();
            assert_eq!(
                error.data.as_ref().and_then(serde_json::Value::as_str),
                Some("/marketplace-remove requires a marketplace name")
            );
        }
    }

    #[test]
    fn parse_builtin_command_rejects_invalid_rollback_count() {
        let error = parse_builtin_command("/rollback").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/rollback requires a turn count")
        );

        let error = parse_builtin_command("/rollback 0").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("turn count must be greater than zero")
        );

        let error = parse_builtin_command("/rollback latest").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("expected a positive integer, got `latest`")
        );
    }

    #[test]
    fn parse_builtin_command_rejects_archive_arguments() {
        let error = parse_builtin_command("/archive now").unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/archive does not accept arguments")
        );

        let error = parse_builtin_command("/unarchive now").unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/unarchive does not accept arguments")
        );
    }

    #[test]
    fn parse_builtin_command_rejects_turn_command_arguments() {
        let error = parse_builtin_command("/compact now").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/compact does not accept arguments")
        );

        let error = parse_builtin_command("/review now").unwrap_err();
        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/review does not accept arguments")
        );
    }

    #[test]
    fn parse_builtin_command_rejects_catalog_command_arguments() {
        for (text, expected) in [
            ("/apps now", "/apps does not accept arguments"),
            ("/delete now", "/delete does not accept arguments"),
            ("/fork now", "/fork does not accept arguments"),
            ("/hooks now", "/hooks does not accept arguments"),
            ("/init now", "/init does not accept arguments"),
            ("/mcp now", "/mcp does not accept arguments"),
            ("/model now", "/model does not accept arguments"),
            ("/new now", "/new does not accept arguments"),
            ("/permissions now", "/permissions does not accept arguments"),
            ("/plan now", "/plan does not accept arguments"),
            ("/plugins now", "/plugins does not accept arguments"),
            ("/ps now", "/ps does not accept arguments"),
            ("/status now", "/status does not accept arguments"),
            ("/stop now", "/stop does not accept arguments"),
            ("/unarchive now", "/unarchive does not accept arguments"),
        ] {
            let error = parse_builtin_command(text).unwrap_err();
            assert_eq!(
                error.data.as_ref().and_then(serde_json::Value::as_str),
                Some(expected)
            );
        }
    }

    #[test]
    fn parse_builtin_command_rejects_invalid_plugin_reference() {
        for text in [
            "/plugin",
            "/plugin github",
            "/plugin @openai",
            "/plugin github@",
        ] {
            let error = parse_builtin_command(text).unwrap_err();
            assert_eq!(
                error.data.as_ref().and_then(serde_json::Value::as_str),
                Some("/plugin requires a plugin name and marketplace path as name@marketplacePath")
            );
        }
    }

    #[test]
    fn parse_builtin_command_rejects_invalid_plugin_install_reference() {
        for text in [
            "/plugin-install",
            "/plugin-install github",
            "/plugin-install @openai",
            "/plugin-install github@",
        ] {
            let error = parse_builtin_command(text).unwrap_err();
            assert_eq!(
                error.data.as_ref().and_then(serde_json::Value::as_str),
                Some(
                    "/plugin-install requires a plugin name and marketplace path as name@marketplacePath"
                )
            );
        }
    }

    #[test]
    fn parse_builtin_command_rejects_invalid_plugin_uninstall_reference() {
        for text in ["/plugin-uninstall", "/plugin-uninstall github extra"] {
            let error = parse_builtin_command(text).unwrap_err();
            assert_eq!(
                error.data.as_ref().and_then(serde_json::Value::as_str),
                Some("/plugin-uninstall requires a plugin id")
            );
        }
    }

    #[test]
    fn parse_builtin_command_rejects_invalid_mcp_resource_reference() {
        for text in ["/mcp-resource", "/mcp-resource filesystem"] {
            let error = parse_builtin_command(text).unwrap_err();
            assert_eq!(
                error.data.as_ref().and_then(serde_json::Value::as_str),
                Some("/mcp-resource requires a server and resource uri")
            );
        }
    }

    #[test]
    fn parse_builtin_command_rejects_invalid_mcp_tool_reference() {
        for text in ["/mcp-tool", "/mcp-tool filesystem"] {
            let error = parse_builtin_command(text).unwrap_err();
            assert_eq!(
                error.data.as_ref().and_then(serde_json::Value::as_str),
                Some("/mcp-tool requires a server and tool name")
            );
        }

        let error = parse_builtin_command("/mcp-tool filesystem read_file {not-json}").unwrap_err();
        assert!(
            error
                .data
                .as_ref()
                .and_then(serde_json::Value::as_str)
                .is_some_and(
                    |message| message.starts_with("/mcp-tool arguments must be valid JSON:")
                ),
            "unexpected error data: {:?}",
            error.data
        );
    }

    #[test]
    fn parse_builtin_command_rejects_empty_skill_roots() {
        let error = parse_builtin_command("/skill-roots").unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/skill-roots requires at least one path")
        );
    }

    #[test]
    fn parse_builtin_command_rejects_unknown_slash_command() {
        let error = parse_builtin_command("/unknown now").unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("unsupported slash command `/unknown`")
        );
    }

    #[test]
    fn parse_builtin_command_allows_skill_invocation_fallback() {
        let command = parse_builtin_command("/skill skill-creator Make a test skill").unwrap();

        assert!(command.is_none());
    }

    #[test]
    fn builtin_command_registry_carries_availability_metadata() {
        let compact = builtin_command_spec(COMPACT_COMMAND).expect("compact command is registered");
        assert_eq!(
            compact.availability,
            CommandAvailability::RequiresNoActiveTurn
        );

        let status = builtin_command_spec(STATUS_COMMAND).expect("status command is registered");
        assert_eq!(status.availability, CommandAvailability::RequiresSession);
    }

    #[test]
    fn available_commands_include_builtin_and_enabled_skills() {
        let commands = available_commands(vec![
            skill(
                "skill-creator",
                Some("/skills/skill-creator/SKILL.md"),
                true,
            ),
            skill("disabled-skill", Some("/skills/disabled/SKILL.md"), false),
        ]);

        assert_eq!(
            commands
                .iter()
                .map(|command| command.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "account",
                "archive",
                "apps",
                "compact",
                "config",
                "config-requirements",
                "delete",
                "feature",
                "features",
                "fork",
                "goal",
                "hooks",
                "kill",
                "marketplace-add",
                "marketplace-remove",
                "memory",
                "init",
                "mcp",
                "mcp-reload",
                "mcp-resource",
                "mcp-tool",
                "model",
                "new",
                "permissions",
                "plan",
                "plugin",
                "plugin-install",
                "plugin-uninstall",
                "plugins",
                "ps",
                "rate-limits",
                "rename",
                "resume",
                "review",
                "rollback",
                "skill-roots",
                "status",
                "stop",
                "unarchive",
                "usage",
                "workspace-messages",
                "skill:skill-creator"
            ]
        );
    }

    #[test]
    fn init_prompt_requests_agents_file_update() {
        let prompt = init_prompt();

        assert!(prompt.contains("Create or update AGENTS.md"));
        assert!(prompt.contains("Write repository instructions in English"));
    }

    #[test]
    fn catalog_summary_prefers_human_labels() {
        let summary = catalog_summary(
            "Apps",
            &serde_json::json!({
                "data": [
                    {"displayName": "Linear", "connectorId": "linear"},
                    {"name": "GitHub"},
                ],
            }),
        );

        assert_eq!(summary, "Apps: 2 entries\n- Linear\n- GitHub");
    }

    #[test]
    fn account_summary_lists_auth_state() {
        let summary = account_summary(&serde_json::json!({
            "account": {
                "type": "chatgpt",
                "email": "user@example.com",
                "planType": "pro"
            },
            "requiresOpenaiAuth": true
        }));

        assert_eq!(
            summary,
            "Account: current status\n- Requires OpenAI auth: true\n- Auth mode: Chatgpt\n- Email: \"user@example.com\"\n- Plan type: \"pro\""
        );
    }

    #[test]
    fn config_requirements_summary_lists_managed_constraints() {
        let summary = config_requirements_summary(&serde_json::json!({
            "requirements": {
                "allowedApprovalPolicies": ["on-request", "never"],
                "allowedSandboxModes": ["workspace-write"],
                "allowedPermissionProfiles": {
                    "trusted": true,
                    "untrusted": false
                },
                "defaultPermissions": "trusted",
                "allowRemoteControl": false,
                "featureRequirements": {
                    "remote_control": false
                },
                "network": {
                    "managedAllowedDomainsOnly": true
                }
            }
        }));

        assert_eq!(
            summary,
            "Config requirements: managed constraints\n- Allowed approval policies: [\"on-request\",\"never\"]\n- Allowed sandbox modes: [\"workspace-write\"]\n- Allowed permission profiles: {\"trusted\":true,\"untrusted\":false}\n- Default permissions: \"trusted\"\n- Allow remote control: false\n- Feature requirements: {\"remote_control\":false}\n- Network: {\"managedAllowedDomainsOnly\":true}"
        );
    }

    #[test]
    fn rate_limits_summary_lists_known_limit_buckets() {
        let summary = rate_limits_summary(&serde_json::json!({
            "rateLimits": {
                "primary": {
                    "usedPercent": 42,
                    "resetsAt": "2026-06-22T12:00:00Z"
                },
                "secondary": {
                    "usedPercent": 7
                }
            }
        }));

        assert_eq!(
            summary,
            "Rate limits: current account\n- Primary: {\"usedPercent\":42,\"resetsAt\":\"2026-06-22T12:00:00Z\"}\n- Secondary: {\"usedPercent\":7}"
        );
    }

    #[test]
    fn account_usage_summary_lists_summary_and_daily_buckets() {
        let summary = account_usage_summary(&serde_json::json!({
            "summary": {
                "lifetimeTokens": 12345,
                "peakDailyTokens": 900,
                "longestRunningTurnSec": 360,
                "currentStreakDays": 3,
                "longestStreakDays": 7
            },
            "dailyUsageBuckets": [
                {
                    "startDate": "2026-06-21",
                    "tokens": 700
                },
                {
                    "startDate": "2026-06-22",
                    "tokens": 900
                }
            ]
        }));

        assert_eq!(
            summary,
            "Usage: account token activity\n- Lifetime tokens: 12345\n- Peak daily tokens: 900\n- Longest running turn seconds: 360\n- Current streak days: 3\n- Longest streak days: 7\n- Daily buckets: 2 entries\n- 2026-06-21: 700 tokens\n- 2026-06-22: 900 tokens"
        );
    }

    #[test]
    fn workspace_messages_summary_lists_active_messages() {
        let summary = workspace_messages_summary(&serde_json::json!({
            "featureEnabled": true,
            "messages": [
                {
                    "messageId": "msg_123",
                    "messageType": "headline",
                    "messageBody": "Workspace maintenance starts at 5pm.",
                    "createdAt": 1781395200,
                    "archivedAt": null
                },
                {
                    "messageId": "msg_456",
                    "messageType": "announcement",
                    "messageBody": "New Codex limits are available.",
                    "createdAt": 1781395300,
                    "archivedAt": null
                }
            ]
        }));

        assert_eq!(
            summary,
            "Workspace messages: active messages\n- Feature enabled: true\n- Messages: 2 entries\n- Headline: Workspace maintenance starts at 5pm.\n- Announcement: New Codex limits are available."
        );
    }

    #[test]
    fn plugin_detail_summary_lists_plugin_components() {
        let summary = plugin_detail_summary(&serde_json::json!({
            "name": "github",
            "marketplacePath": "openai",
            "manifest": {
                "description": "GitHub integration"
            },
            "skills": [
                {"name": "triage"}
            ],
            "mcpServers": [
                {"serverName": "github"}
            ]
        }));

        assert_eq!(
            summary,
            "Plugin: github\nMarketplace: openai\nDescription: GitHub integration\nSkills: 1 entries\n- triage\nMCP servers: 1 entries\n- github"
        );
    }

    #[test]
    fn plugin_install_summary_lists_auth_requirements() {
        let summary = plugin_install_summary(
            "github",
            &serde_json::json!({
                "authPolicy": {
                    "type": "requireAuthenticated"
                },
                "appsNeedingAuth": [
                    {
                        "displayName": "GitHub",
                        "connectorId": "github"
                    }
                ]
            }),
        );

        assert_eq!(
            summary,
            "Installed Codex plugin `github`.\n- Auth policy: {\"type\":\"requireAuthenticated\"}\nApps needing auth: 1 entries\n- GitHub"
        );
    }

    #[test]
    fn mcp_reload_summary_includes_refreshed_catalog() {
        let summary = mcp_reload_summary(&serde_json::json!({
            "data": [
                {
                    "serverName": "filesystem",
                    "status": "running"
                }
            ]
        }));

        assert_eq!(
            summary,
            "Reloaded Codex MCP server configuration.\n\nMCP: 1 entries\n- filesystem"
        );
    }

    #[test]
    fn mcp_resource_summary_includes_text_contents() {
        let summary = mcp_resource_summary(
            "filesystem",
            "file:///repo/README.md",
            &serde_json::json!({
                "contents": [
                    {
                        "uri": "file:///repo/README.md",
                        "mimeType": "text/markdown",
                        "text": "README contents"
                    }
                ]
            }),
        );

        assert_eq!(
            summary,
            "MCP resource\n- Server: filesystem\n- URI: file:///repo/README.md\n- Contents: 1 entries\n- Text: README contents"
        );
    }

    #[test]
    fn mcp_tool_summary_includes_text_content() {
        let summary = mcp_tool_summary(
            "filesystem",
            "read_file",
            &serde_json::json!({
                "content": [
                    {
                        "type": "text",
                        "text": "README contents"
                    }
                ]
            }),
        );

        assert_eq!(
            summary,
            "MCP tool\n- Server: filesystem\n- Tool: read_file\n- Contents: 1 entries\n- Text: README contents"
        );
    }

    #[test]
    fn session_info_preserves_app_server_thread_metadata() {
        let session = session_info_from_app_server_thread(
            AppServerThread {
                id: "thread-1".to_owned(),
                cwd: Some(std::path::PathBuf::from("/repo")),
                turns: Vec::new(),
                name: None,
                preview: Some("Recent work summary".to_owned()),
                status: Some(serde_json::json!({"type": "notLoaded"})),
                model_provider: Some("openai".to_owned()),
                created_at: Some(1_700_000_000),
                updated_at: Some(1_700_000_100),
                recency_at: Some(1_700_000_200),
                agent_nickname: Some("Codex".to_owned()),
                agent_role: Some("primary".to_owned()),
                parent_thread_id: Some("thread-parent".to_owned()),
            },
            vec![std::path::PathBuf::from("/shared")],
        )
        .expect("thread with cwd should map to session info");

        assert_eq!(session.session_id.0.as_ref(), "thread-1");
        assert_eq!(session.cwd, std::path::PathBuf::from("/repo"));
        assert_eq!(
            session.additional_directories,
            vec![std::path::PathBuf::from("/shared")]
        );
        assert_eq!(session.title.as_deref(), Some("Recent work summary"));
        assert_eq!(session.updated_at.as_deref(), Some("2023-11-14T22:15:00Z"));

        let adapter_meta = session
            .meta
            .as_ref()
            .and_then(|meta| meta.get("brokk_codex_acp"))
            .and_then(serde_json::Value::as_object)
            .expect("adapter metadata should be namespaced");

        assert_eq!(adapter_meta["preview"], "Recent work summary");
        assert_eq!(adapter_meta["threadStatus"]["type"], "notLoaded");
        assert_eq!(adapter_meta["modelProvider"], "openai");
        assert_eq!(adapter_meta["updatedAt"], 1_700_000_100);
        assert_eq!(adapter_meta["parentThreadId"], "thread-parent");
    }

    #[test]
    fn unix_timestamp_to_utc_iso8601_formats_utc_seconds() {
        assert_eq!(unix_timestamp_to_utc_iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(
            unix_timestamp_to_utc_iso8601(1_700_000_100),
            "2023-11-14T22:15:00Z"
        );
        assert_eq!(unix_timestamp_to_utc_iso8601(-1), "1969-12-31T23:59:59Z");
    }

    #[test]
    fn runtime_workspace_roots_include_cwd_and_additional_directories() {
        let roots = runtime_workspace_roots_for_acp_request(
            std::path::Path::new("/repo"),
            &[
                std::path::PathBuf::from("/shared"),
                std::path::PathBuf::from("/repo"),
            ],
        )
        .expect("absolute roots should map");

        assert_eq!(
            roots,
            vec![
                std::path::PathBuf::from("/repo"),
                std::path::PathBuf::from("/shared")
            ]
        );
    }

    #[test]
    fn runtime_workspace_roots_reject_relative_additional_directories() {
        let error = runtime_workspace_roots_for_acp_request(
            std::path::Path::new("/repo"),
            &[std::path::PathBuf::from("relative")],
        )
        .unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("additionalDirectories entries must be absolute paths")
        );
    }

    #[test]
    fn additional_directories_are_extracted_from_runtime_workspace_roots() {
        let additional = additional_directories_from_runtime_roots(
            std::path::Path::new("/repo"),
            vec![
                std::path::PathBuf::from("/repo"),
                std::path::PathBuf::from("/shared"),
                std::path::PathBuf::from("/shared"),
            ],
        );

        assert_eq!(additional, vec![std::path::PathBuf::from("/shared")]);
    }

    #[test]
    fn session_additional_directories_store_path_lives_under_codex_home() {
        assert_eq!(
            session_additional_directories_store_path(std::path::Path::new("/codex-home")),
            std::path::PathBuf::from(
                "/codex-home/brokk-codex-acp/session-additional-directories.json"
            )
        );
    }

    #[test]
    fn session_additional_directories_persistence_round_trips() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let store_path = session_additional_directories_store_path(tempdir.path());
        let mut sessions = HashMap::new();
        sessions.insert(
            "thread-1".to_owned(),
            vec![
                std::path::PathBuf::from("/shared"),
                std::path::PathBuf::from("/other"),
            ],
        );

        persist_session_additional_directories(&store_path, &sessions);

        assert_eq!(load_session_additional_directories(&store_path), sessions);
    }

    #[test]
    fn permission_option_preserves_rich_available_decision_meta() {
        let available_decision = serde_json::json!({
            "acceptWithExecpolicyAmendment": {
                "execpolicy_amendment": [
                    {"type": "exact", "argv": ["cargo", "test"]}
                ]
            }
        });

        let option = permission_option(AppServerApprovalChoice {
            option: AppServerApprovalOption::AcceptWithExecpolicyAmendment,
            option_id: "acceptWithExecpolicyAmendment:1".to_owned(),
            label: "Allow similar commands".to_owned(),
            available_decision: Some(available_decision.clone()),
        });

        assert_eq!(
            option.option_id.to_string(),
            "acceptWithExecpolicyAmendment:1"
        );
        assert_eq!(option.kind, PermissionOptionKind::AllowAlways);
        assert_eq!(
            option
                .meta
                .as_ref()
                .and_then(|meta| meta.get("brokk_codex_acp"))
                .and_then(|meta| meta.get("availableDecision")),
            Some(&available_decision)
        );
    }

    #[test]
    fn permission_option_marks_network_deny_amendment_as_reject_always() {
        let option = permission_option(AppServerApprovalChoice {
            option: AppServerApprovalOption::ApplyNetworkPolicyAmendment,
            option_id: "applyNetworkPolicyAmendment:0".to_owned(),
            label: "Apply network rule".to_owned(),
            available_decision: Some(serde_json::json!({
                "applyNetworkPolicyAmendment": {
                    "network_policy_amendment": {
                        "host": "example.com",
                        "action": "deny"
                    }
                }
            })),
        });

        assert_eq!(option.kind, PermissionOptionKind::RejectAlways);
    }

    #[test]
    fn permission_request_text_summarizes_requested_access() {
        let text = permission_request_text(
            &serde_json::json!({
                "reason": "Need workspace access",
                "environmentId": "local",
                "cwd": "/repo",
            }),
            &serde_json::json!({
                "network": {"enabled": true},
                "fileSystem": {
                    "read": ["/repo"],
                    "write": ["/repo/src"],
                },
            }),
        );

        assert_eq!(
            text,
            "Requested permissions:\nReason: Need workspace access\nEnvironment: local\nWorking directory: /repo\n- Network access\n- Filesystem read access: /repo\n- Filesystem write access: /repo/src"
        );
    }

    #[test]
    fn permission_request_text_handles_empty_permission_payload() {
        let text = permission_request_text(&serde_json::json!({}), &serde_json::json!({}));

        assert_eq!(
            text,
            "Requested permissions:\n- No additional permissions requested."
        );
    }

    #[test]
    fn config_options_include_model_reasoning_service_tier_mode_and_permission_selectors() {
        let state = AcpConfigState {
            current_model: Some("gpt-5-codex".to_owned()),
            current_reasoning_effort: Some("high".to_owned()),
            current_service_tier: Some("priority".to_owned()),
            current_approval_policy: Some("on-request".to_owned()),
            current_collaboration_mode: Some("plan".to_owned()),
            current_permission_profile: Some(":workspace".to_owned()),
            models: vec![
                model("gpt-5", "GPT-5", true),
                model("gpt-5-codex", "GPT-5 Codex", false),
            ],
            collaboration_modes: vec![
                collaboration_mode("default", "Default"),
                collaboration_mode("plan", "Plan"),
            ],
            permission_profiles: vec![
                permission_profile(":read-only", None),
                permission_profile(":workspace", None),
                permission_profile("audit", Some("Inspect only.")),
            ],
        };

        let options = config_options(&state);

        assert_eq!(options.len(), 6);
        assert_eq!(options[0].id.to_string(), MODEL_CONFIG_ID);
        assert_eq!(options[1].id.to_string(), REASONING_EFFORT_CONFIG_ID);
        assert_eq!(options[2].id.to_string(), SERVICE_TIER_CONFIG_ID);
        assert_eq!(options[3].id.to_string(), APPROVAL_POLICY_CONFIG_ID);
        assert_eq!(options[4].id.to_string(), COLLABORATION_MODE_CONFIG_ID);
        assert_eq!(options[5].id.to_string(), PERMISSION_PROFILE_CONFIG_ID);
        let serialized = serde_json::to_value(&options).unwrap();
        assert_eq!(serialized[0]["currentValue"], "gpt-5-codex");
        assert_eq!(serialized[1]["currentValue"], "high");
        assert_eq!(serialized[2]["currentValue"], "priority");
        assert_eq!(serialized[3]["currentValue"], "on-request");
        assert_eq!(serialized[4]["currentValue"], "plan");
        assert_eq!(serialized[4]["options"][1]["name"], "Plan");
        assert_eq!(serialized[5]["currentValue"], ":workspace");
        assert_eq!(serialized[5]["options"][1]["name"], "Workspace");
    }

    #[test]
    fn skill_config_options_expose_enabled_state() {
        let options = skill_config_options(&[
            skill(
                "skill-creator",
                Some("/skills/skill-creator/SKILL.md"),
                true,
            ),
            skill("disabled-skill", Some("/skills/disabled/SKILL.md"), false),
            skill("skill-creator", Some("/duplicate/SKILL.md"), false),
        ]);

        assert_eq!(options.len(), 2);
        assert_eq!(options[0].id.to_string(), "skill:skill-creator");
        assert_eq!(options[1].id.to_string(), "skill:disabled-skill");
        let serialized = serde_json::to_value(&options).unwrap();
        assert_eq!(serialized[0]["currentValue"], SKILL_ENABLED_VALUE);
        assert_eq!(serialized[0]["options"][0]["value"], SKILL_ENABLED_VALUE);
        assert_eq!(serialized[0]["options"][1]["value"], SKILL_DISABLED_VALUE);
        assert_eq!(serialized[1]["currentValue"], SKILL_DISABLED_VALUE);
    }
}
