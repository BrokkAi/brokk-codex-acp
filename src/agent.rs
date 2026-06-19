use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use agent_client_protocol::schema::{
    AgentCapabilities, AvailableCommand, AvailableCommandInput, AvailableCommandsUpdate,
    CancelNotification, CloseSessionRequest, CloseSessionResponse, ConfigOptionUpdate,
    ContentBlock, ContentChunk, DeleteSessionRequest, DeleteSessionResponse, ForkSessionRequest,
    ForkSessionResponse, InitializeRequest, InitializeResponse, ListSessionsRequest,
    ListSessionsResponse, LoadSessionRequest, LoadSessionResponse, NewSessionRequest,
    NewSessionResponse, PermissionOption, PermissionOptionId, PermissionOptionKind, Plan,
    PlanEntry, PlanEntryPriority, PlanEntryStatus, PromptCapabilities, PromptRequest,
    PromptResponse, ProtocolVersion, RequestPermissionOutcome, RequestPermissionRequest,
    ResumeSessionRequest, ResumeSessionResponse, SessionCapabilities, SessionCloseCapabilities,
    SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelectOption,
    SessionDeleteCapabilities, SessionForkCapabilities, SessionId, SessionInfo, SessionInfoUpdate,
    SessionListCapabilities, SessionNotification, SessionResumeCapabilities, SessionUpdate,
    SetSessionConfigOptionRequest, SetSessionConfigOptionResponse, StopReason, TextContent,
    ToolCall, ToolCallContent, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, ToolKind,
    UnstructuredCommandInput, UsageUpdate,
};
use agent_client_protocol::{
    Agent, ByteStreams, Client, ConnectTo, ConnectionTo, Error, on_receive_notification,
    on_receive_request,
};
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, trace};

use crate::app_server::{
    AppServerActivePermissionProfile, AppServerApprovalDecision, AppServerApprovalOption,
    AppServerApprovalRequest, AppServerClient, AppServerCollaborationMode,
    AppServerCollaborationModeMask, AppServerCollaborationModeSettings, AppServerHistoryEvent,
    AppServerMessage, AppServerModel, AppServerPermissionProfile, AppServerPlanStatus,
    AppServerPromptCompletion, AppServerPromptEvent, AppServerSkill, AppServerThread,
    AppServerThreadSettingsUpdate, AppServerToolKind, AppServerToolStatus, AppServerTurnInput,
    ThreadSettingsUpdateParams, decode_thread_archived, decode_thread_goal_cleared,
    decode_thread_goal_updated, decode_thread_name_updated, decode_thread_settings_updated,
    decode_thread_status_changed, history_events, history_events_for_turns,
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
const ARCHIVE_COMMAND: &str = "archive";
const APPS_COMMAND: &str = "apps";
const COMPACT_COMMAND: &str = "compact";
const FORK_COMMAND: &str = "fork";
const GOAL_COMMAND: &str = "goal";
const HOOKS_COMMAND: &str = "hooks";
const INIT_COMMAND: &str = "init";
const MCP_COMMAND: &str = "mcp";
const MODEL_COMMAND: &str = "model";
const NEW_COMMAND: &str = "new";
const PERMISSIONS_COMMAND: &str = "permissions";
const PLUGINS_COMMAND: &str = "plugins";
const PS_COMMAND: &str = "ps";
const RENAME_COMMAND: &str = "rename";
const RESUME_COMMAND: &str = "resume";
const REVIEW_COMMAND: &str = "review";
const SKILL_COMMAND: &str = "skill";
const SKILL_ROOTS_COMMAND: &str = "skill-roots";
const STATUS_COMMAND: &str = "status";
const STOP_COMMAND: &str = "stop";
type CancelSignals = Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>;
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

impl CodexAcpAgent {
    pub fn new(app_server: AppServerClient) -> Self {
        Self {
            app_server: Arc::new(Mutex::new(app_server)),
            active_prompts: Arc::new(Mutex::new(HashMap::new())),
            outstanding_approvals: Arc::new(Mutex::new(HashMap::new())),
            skills_by_cwd: Arc::new(Mutex::new(HashMap::new())),
            session_cwds: Arc::new(Mutex::new(HashMap::new())),
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
                publish_session_archived_update(&session_id, cx).map_err(acp_internal_error)?;
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
            _ => {}
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
        let (session_id, config_options) = self.start_thread(cwd, &cx).await?;

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
        let (session_id, config_options) = self.fork_thread(source_thread_id, cwd, &cx).await?;

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

        let thread = self
            .app_server
            .lock()
            .await
            .thread_read(thread_id.clone())
            .await
            .map_err(acp_internal_error)?
            .thread;

        let resume_response = self
            .app_server
            .lock()
            .await
            .thread_resume(thread_id, cwd)
            .await
            .map_err(acp_internal_error)?;

        let mut event_state = AcpEventState::default();
        for event in history_events(&thread) {
            match event {
                AppServerHistoryEvent::UserMessage(text) => {
                    send_session_update(
                        &cx,
                        request.session_id.clone(),
                        SessionUpdate::UserMessageChunk(text_chunk(text)),
                    )
                    .map_err(acp_internal_error)?;
                }
                AppServerHistoryEvent::PromptEvent(event) => {
                    send_prompt_event(&cx, request.session_id.clone(), *event, &mut event_state)
                        .map_err(acp_internal_error)?;
                }
            }
        }

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
        let (_, config_options) = self.resume_thread(thread_id, cwd, &cx).await?;

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

        let sessions = response
            .data
            .into_iter()
            .filter_map(session_info_from_app_server_thread)
            .collect();

        Ok(ListSessionsResponse::new(sessions).next_cursor(response.next_cursor))
    }

    async fn close_session(
        &self,
        request: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, Error> {
        self.app_server
            .lock()
            .await
            .thread_unsubscribe(request.session_id.0.to_string())
            .await
            .map_err(acp_internal_error)?;

        Ok(CloseSessionResponse::new())
    }

    async fn delete_session(
        &self,
        request: DeleteSessionRequest,
    ) -> Result<DeleteSessionResponse, Error> {
        let thread_id = request.session_id.0.to_string();

        if let Some(cancel) = self.active_prompts.lock().await.remove(&thread_id) {
            let _ = cancel.send(());
        }

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
                    .map_err(acp_internal_error)?
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
                    .map_err(acp_internal_error)?
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
                    .map_err(acp_internal_error)?
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
                    .map_err(acp_internal_error)?
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
                    .map_err(acp_internal_error)?
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
                    .map_err(acp_internal_error)?
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
        let text = prompt_text(request.prompt)?;
        let session_id = request.session_id.clone();
        let thread_id = request.session_id.0.to_string();
        debug!(method = "session/prompt", thread_id, "handling ACP request");

        if let Some(command) = parse_builtin_command(&text)? {
            return self
                .run_builtin_command(command, &session_id, &thread_id, &cx)
                .await;
        }

        let input = self.prompt_input(&request.session_id, text).await;
        self.run_turn_inputs(&session_id, &thread_id, input, &cx)
            .await
    }

    async fn run_turn_inputs(
        &self,
        session_id: &SessionId,
        thread_id: &str,
        input: Vec<AppServerTurnInput>,
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
            .turn_start_until_complete(
                thread_id.to_owned(),
                input,
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
        if let Some(cancel) = self
            .active_prompts
            .lock()
            .await
            .remove(notification.session_id.0.as_ref())
        {
            let _ = cancel.send(());
        }
        Ok(())
    }

    async fn prompt_input(&self, session_id: &SessionId, text: String) -> Vec<AppServerTurnInput> {
        let Some(cwd) = self
            .session_cwds
            .lock()
            .await
            .get(session_id.0.as_ref())
            .cloned()
        else {
            return vec![AppServerTurnInput::Text { text }];
        };

        let skills = self
            .skills_by_cwd
            .lock()
            .await
            .get(&cwd)
            .cloned()
            .unwrap_or_default();

        prompt_input_with_skills(text, &skills)
    }

    async fn run_builtin_command(
        &self,
        command: BuiltinCommand,
        session_id: &SessionId,
        thread_id: &str,
        cx: &ConnectionTo<Client>,
    ) -> Result<PromptResponse, Error> {
        match command {
            BuiltinCommand::Archive => {
                self.app_server
                    .lock()
                    .await
                    .thread_archive(thread_id.to_owned())
                    .await
                    .map_err(acp_internal_error)?;
                publish_session_archived_update(session_id, cx).map_err(acp_internal_error)?;
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
            BuiltinCommand::Init => {
                let input = vec![AppServerTurnInput::Text {
                    text: init_prompt(),
                }];
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
                let (new_session_id, _) = self.start_thread(cwd, cx).await?;
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
                let (forked_session_id, _) =
                    self.fork_thread(thread_id.to_owned(), cwd, cx).await?;

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
            BuiltinCommand::Resume { target } => {
                let cwd = self
                    .session_cwd(session_id)
                    .await
                    .ok_or_else(|| Error::invalid_request().data("session cwd is not known yet"))?;
                let target_thread_id = self.resolve_resume_target(&cwd, &target).await;
                let (resumed_session_id, _) = self.resume_thread(target_thread_id, cwd, cx).await?;
                publish_catalog_message(
                    session_id,
                    "Resume",
                    format!("Resumed Codex session `{}`.", resumed_session_id.0),
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

    async fn start_thread(
        &self,
        cwd: String,
        cx: &ConnectionTo<Client>,
    ) -> Result<(SessionId, Vec<SessionConfigOption>), Error> {
        let response = self
            .app_server
            .lock()
            .await
            .thread_start(cwd)
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
            self.refresh_and_publish_skills(cwd, &session_id, cx, false)
                .await?;
            config_options = self.config_options_for_session(session_id.0.as_ref()).await;
        }

        Ok((session_id, config_options))
    }

    async fn fork_thread(
        &self,
        source_thread_id: String,
        cwd: String,
        cx: &ConnectionTo<Client>,
    ) -> Result<(SessionId, Vec<SessionConfigOption>), Error> {
        let response = self
            .app_server
            .lock()
            .await
            .thread_fork(source_thread_id, cwd)
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

    async fn resume_thread(
        &self,
        thread_id: String,
        cwd: String,
        cx: &ConnectionTo<Client>,
    ) -> Result<(SessionId, Vec<SessionConfigOption>), Error> {
        let response = self
            .app_server
            .lock()
            .await
            .thread_resume(thread_id, cwd)
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
            .prompt_capabilities(PromptCapabilities::new())
            .session_capabilities(
                // Fork is exposed by the Rust ACP crate behind its unstable
                // RFD/extension feature; it is not stable ACP v1 behavior.
                SessionCapabilities::new()
                    .list(SessionListCapabilities::new())
                    .resume(SessionResumeCapabilities::new())
                    .close(SessionCloseCapabilities::new())
                    .delete(SessionDeleteCapabilities::new())
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

fn prompt_text(prompt: Vec<ContentBlock>) -> Result<String, Error> {
    let mut text = String::new();

    for block in prompt {
        match block {
            ContentBlock::Text(text_content) => {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(&text_content.text);
            }
            ContentBlock::ResourceLink(resource) => {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(&format!("@{}", resource.uri));
            }
            _ => {
                return Err(Error::invalid_params()
                    .data("only text and resource link prompt blocks are supported so far"));
            }
        }
    }

    if text.trim().is_empty() {
        return Err(Error::invalid_params().data("prompt text cannot be empty"));
    }

    Ok(text)
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
    Archive,
    Apps,
    Compact,
    Fork,
    GoalSet { objective: String },
    GoalGet,
    GoalClear,
    Hooks,
    Init,
    Mcp,
    Model,
    New,
    Permissions,
    Plugins,
    Ps,
    Rename { title: String },
    Resume { target: String },
    Review,
    SkillRoots { roots: Vec<String> },
    Status,
    Stop,
}

#[derive(Debug, Clone, Copy)]
enum BuiltinTurnCommand {
    Compact,
    Review,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandAvailability {
    RequiresSession,
    RequiresNoActiveTurn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandHandler {
    Archive,
    Apps,
    Compact,
    Fork,
    Goal,
    Hooks,
    Init,
    Mcp,
    Model,
    New,
    Permissions,
    Plugins,
    Ps,
    Rename,
    Resume,
    Review,
    SkillRoots,
    Status,
    Stop,
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
        CommandHandler::Archive => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Archive)
        }
        CommandHandler::Apps => parse_no_argument_command(rest, spec.name, BuiltinCommand::Apps),
        CommandHandler::Compact => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Compact)
        }
        CommandHandler::Fork => parse_no_argument_command(rest, spec.name, BuiltinCommand::Fork),
        CommandHandler::Goal => parse_goal_command(rest),
        CommandHandler::Hooks => parse_no_argument_command(rest, spec.name, BuiltinCommand::Hooks),
        CommandHandler::Init => parse_no_argument_command(rest, spec.name, BuiltinCommand::Init),
        CommandHandler::Mcp => parse_no_argument_command(rest, spec.name, BuiltinCommand::Mcp),
        CommandHandler::Model => parse_no_argument_command(rest, spec.name, BuiltinCommand::Model),
        CommandHandler::New => parse_no_argument_command(rest, spec.name, BuiltinCommand::New),
        CommandHandler::Permissions => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Permissions)
        }
        CommandHandler::Plugins => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Plugins)
        }
        CommandHandler::Ps => parse_no_argument_command(rest, spec.name, BuiltinCommand::Ps),
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
        CommandHandler::SkillRoots => parse_skill_roots_command(rest),
        CommandHandler::Status => {
            parse_no_argument_command(rest, spec.name, BuiltinCommand::Status)
        }
        CommandHandler::Stop => parse_no_argument_command(rest, spec.name, BuiltinCommand::Stop),
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
    let mut words = id.split(['-', '_']);
    let Some(first) = words.next() else {
        return String::new();
    };
    let mut result = capitalize(first);
    for word in words {
        result.push(' ');
        result.push_str(word);
    }
    result
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
    cx: &ConnectionTo<Client>,
) -> anyhow::Result<()> {
    publish_session_adapter_meta_update(
        session_id,
        [("archived".to_owned(), serde_json::Value::Bool(true))],
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

fn session_info_from_app_server_thread(thread: AppServerThread) -> Option<SessionInfo> {
    let cwd = thread.cwd.clone()?;
    let title = thread.name.clone().or_else(|| thread.preview.clone());
    let meta = session_info_meta_from_app_server_thread(&thread);

    Some(
        SessionInfo::new(SessionId::new(thread.id), cwd)
            .title(title)
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
            SessionUpdate::AgentMessageChunk(text_chunk(delta)),
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
            fields.raw_output = update.raw;
            send_session_update(
                cx,
                session_id,
                SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(update.id, fields)),
            )
        }
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
    fields.title = Some(approval.title);
    fields.kind = Some(tool_kind(approval.kind));
    fields.status = Some(ToolCallStatus::Pending);
    fields.raw_input = Some(approval.raw);

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
                        approval_decision_from_option_id(option_id)
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

fn permission_option(option: AppServerApprovalOption) -> PermissionOption {
    PermissionOption::new(
        PermissionOptionId::new(option.id()),
        option.label(),
        permission_option_kind(option),
    )
}

fn permission_option_kind(option: AppServerApprovalOption) -> PermissionOptionKind {
    match option {
        AppServerApprovalOption::Accept => PermissionOptionKind::AllowOnce,
        AppServerApprovalOption::AcceptForSession => PermissionOptionKind::AllowAlways,
        AppServerApprovalOption::Decline => PermissionOptionKind::RejectOnce,
        AppServerApprovalOption::Cancel => PermissionOptionKind::RejectOnce,
    }
}

fn approval_decision_from_option_id(option_id: &str) -> AppServerApprovalDecision {
    match option_id {
        "accept" => AppServerApprovalDecision::Accept,
        "acceptForSession" => AppServerApprovalDecision::AcceptForSession,
        "decline" => AppServerApprovalDecision::Decline,
        "cancel" => AppServerApprovalDecision::Cancel,
        _ => AppServerApprovalDecision::Cancel,
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

fn text_tool_content(text: String) -> ToolCallContent {
    ToolCallContent::from(ContentBlock::Text(TextContent::new(text)))
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
            "/apps",
            "/fork",
            "/hooks",
            "/init",
            "/mcp",
            "/model",
            "/new",
            "/permissions",
            "/plugins",
            "/ps",
            "/skill-roots /repo/.codex/skills,/shared/skills",
            "/status",
            "/stop",
        ] {
            let command = parse_builtin_command(text).unwrap().unwrap();
            match text {
                "/apps" => assert!(matches!(command, BuiltinCommand::Apps)),
                "/fork" => assert!(matches!(command, BuiltinCommand::Fork)),
                "/hooks" => assert!(matches!(command, BuiltinCommand::Hooks)),
                "/init" => assert!(matches!(command, BuiltinCommand::Init)),
                "/mcp" => assert!(matches!(command, BuiltinCommand::Mcp)),
                "/model" => assert!(matches!(command, BuiltinCommand::Model)),
                "/new" => assert!(matches!(command, BuiltinCommand::New)),
                "/permissions" => assert!(matches!(command, BuiltinCommand::Permissions)),
                "/plugins" => assert!(matches!(command, BuiltinCommand::Plugins)),
                "/ps" => assert!(matches!(command, BuiltinCommand::Ps)),
                "/skill-roots /repo/.codex/skills,/shared/skills" => assert!(matches!(
                    command,
                    BuiltinCommand::SkillRoots { roots }
                        if roots == vec!["/repo/.codex/skills", "/shared/skills"]
                )),
                "/status" => assert!(matches!(command, BuiltinCommand::Status)),
                "/stop" => assert!(matches!(command, BuiltinCommand::Stop)),
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

    #[test]
    fn parse_builtin_command_rejects_empty_resume() {
        let error = parse_builtin_command("/resume").unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/resume requires a thread id or name")
        );
    }

    #[test]
    fn parse_builtin_command_rejects_archive_arguments() {
        let error = parse_builtin_command("/archive now").unwrap_err();

        assert_eq!(
            error.data.as_ref().and_then(serde_json::Value::as_str),
            Some("/archive does not accept arguments")
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
            ("/fork now", "/fork does not accept arguments"),
            ("/hooks now", "/hooks does not accept arguments"),
            ("/init now", "/init does not accept arguments"),
            ("/mcp now", "/mcp does not accept arguments"),
            ("/model now", "/model does not accept arguments"),
            ("/new now", "/new does not accept arguments"),
            ("/permissions now", "/permissions does not accept arguments"),
            ("/plugins now", "/plugins does not accept arguments"),
            ("/ps now", "/ps does not accept arguments"),
            ("/status now", "/status does not accept arguments"),
            ("/stop now", "/stop does not accept arguments"),
        ] {
            let error = parse_builtin_command(text).unwrap_err();
            assert_eq!(
                error.data.as_ref().and_then(serde_json::Value::as_str),
                Some(expected)
            );
        }
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
                "archive",
                "apps",
                "compact",
                "fork",
                "goal",
                "hooks",
                "init",
                "mcp",
                "model",
                "new",
                "permissions",
                "plugins",
                "ps",
                "rename",
                "resume",
                "review",
                "skill-roots",
                "status",
                "stop",
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
    fn session_info_preserves_app_server_thread_metadata() {
        let session = session_info_from_app_server_thread(AppServerThread {
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
        })
        .expect("thread with cwd should map to session info");

        assert_eq!(session.session_id.0.as_ref(), "thread-1");
        assert_eq!(session.cwd, std::path::PathBuf::from("/repo"));
        assert_eq!(session.title.as_deref(), Some("Recent work summary"));

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
