use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tracing::{debug, trace, warn};

const APP_SERVER_OVERLOADED_CODE: i64 = -32001;
const JSON_RPC_REQUEST_FAILED: i64 = -32000;
const JSON_RPC_METHOD_NOT_FOUND: i64 = -32601;
const APP_SERVER_OVERLOAD_MAX_RETRIES: usize = 3;
const APP_SERVER_OVERLOAD_INITIAL_RETRY_DELAY: Duration = Duration::from_millis(25);
const APP_SERVER_MESSAGE_BUFFER_CAPACITY: usize = 16 * 1024;

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
    codex_home: Option<PathBuf>,
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
        let (messages_tx, _) = broadcast::channel(APP_SERVER_MESSAGE_BUFFER_CAPACITY);
        let reader_task = spawn_reader(stdout, pending_responses.clone(), messages_tx.clone());

        Ok(Self {
            child,
            stdin,
            pending_responses,
            messages_tx,
            reader_task,
            next_id: 1,
            codex_home: None,
        })
    }

    pub fn codex_home(&self) -> Option<&Path> {
        self.codex_home.as_deref()
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
                mcp_server_openai_form_elicitation: true,
            },
        };

        let response: InitializeResponse = self.request("initialize", params).await?;
        self.codex_home = Some(PathBuf::from(&response.codex_home));
        self.notify("initialized", json!({})).await?;
        Ok(response)
    }

    pub async fn thread_start(
        &mut self,
        cwd: String,
        runtime_workspace_roots: Option<Vec<PathBuf>>,
    ) -> anyhow::Result<ThreadStartResponse> {
        self.request(
            "thread/start",
            ThreadStartParams {
                cwd: Some(cwd),
                runtime_workspace_roots,
            },
        )
        .await
    }

    pub async fn thread_fork(
        &mut self,
        thread_id: String,
        cwd: String,
        runtime_workspace_roots: Option<Vec<PathBuf>>,
    ) -> anyhow::Result<ThreadForkResponse> {
        self.thread_fork_with_options(thread_id, cwd, runtime_workspace_roots, false, false)
            .await
    }

    pub async fn thread_fork_with_options(
        &mut self,
        thread_id: String,
        cwd: String,
        runtime_workspace_roots: Option<Vec<PathBuf>>,
        ephemeral: bool,
        exclude_turns: bool,
    ) -> anyhow::Result<ThreadForkResponse> {
        self.request(
            "thread/fork",
            ThreadForkParams {
                thread_id,
                cwd: Some(cwd),
                runtime_workspace_roots,
                ephemeral,
                exclude_turns,
            },
        )
        .await
    }

    pub async fn thread_resume(
        &mut self,
        thread_id: String,
        cwd: String,
        runtime_workspace_roots: Option<Vec<PathBuf>>,
    ) -> anyhow::Result<ThreadResumeResponse> {
        self.request(
            "thread/resume",
            ThreadResumeParams {
                thread_id,
                cwd: Some(cwd),
                runtime_workspace_roots,
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

    pub async fn thread_rollback(
        &mut self,
        thread_id: String,
        num_turns: u32,
    ) -> anyhow::Result<ThreadRollbackResponse> {
        self.request(
            "thread/rollback",
            ThreadRollbackParams {
                thread_id,
                num_turns,
            },
        )
        .await
    }

    pub async fn thread_turns_list(
        &mut self,
        thread_id: String,
        cursor: Option<String>,
        limit: u32,
    ) -> anyhow::Result<ThreadTurnsListResponse> {
        self.request(
            "thread/turns/list",
            ThreadTurnsListParams {
                thread_id,
                cursor,
                limit: Some(limit),
                sort_direction: Some("asc"),
                items_view: Some("full"),
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

    pub async fn thread_unarchive(
        &mut self,
        thread_id: String,
    ) -> anyhow::Result<ThreadUnarchiveResponse> {
        self.request("thread/unarchive", ThreadUnarchiveParams { thread_id })
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

    pub async fn thread_memory_mode_set(
        &mut self,
        thread_id: String,
        mode: ThreadMemoryMode,
    ) -> anyhow::Result<ThreadMemoryModeSetResponse> {
        self.request(
            "thread/memoryMode/set",
            ThreadMemoryModeSetParams { thread_id, mode },
        )
        .await
    }

    pub async fn memory_reset(&mut self) -> anyhow::Result<MemoryResetResponse> {
        self.request("memory/reset", EmptyParams {}).await
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

    pub async fn config_read(
        &mut self,
        cwd: Option<String>,
        include_layers: bool,
    ) -> anyhow::Result<Value> {
        self.request(
            "config/read",
            ConfigReadParams {
                cwd,
                include_layers,
            },
        )
        .await
    }

    pub async fn config_requirements_read(&mut self) -> anyhow::Result<Value> {
        self.request("configRequirements/read", EmptyParams {})
            .await
    }

    pub async fn account_read(&mut self, refresh_token: bool) -> anyhow::Result<Value> {
        self.request("account/read", AccountReadParams { refresh_token })
            .await
    }

    pub async fn account_login_start(&mut self, mode: AccountLoginMode) -> anyhow::Result<Value> {
        self.request("account/login/start", AccountLoginStartParams::from(mode))
            .await
    }

    pub async fn account_login_cancel(&mut self, login_id: String) -> anyhow::Result<Value> {
        self.request(
            "account/login/cancel",
            AccountLoginCancelParams { login_id },
        )
        .await
    }

    pub async fn account_logout(&mut self) -> anyhow::Result<Value> {
        self.request("account/logout", EmptyParams {}).await
    }

    pub async fn account_rate_limits_read(&mut self) -> anyhow::Result<Value> {
        self.request("account/rateLimits/read", EmptyParams {})
            .await
    }

    pub async fn account_usage_read(&mut self) -> anyhow::Result<Value> {
        self.request("account/usage/read", EmptyParams {}).await
    }

    pub async fn account_workspace_messages_read(&mut self) -> anyhow::Result<Value> {
        self.request("account/workspaceMessages/read", EmptyParams {})
            .await
    }

    pub async fn experimental_feature_list(
        &mut self,
        thread_id: String,
    ) -> anyhow::Result<ExperimentalFeatureListResponse> {
        self.request(
            "experimentalFeature/list",
            ExperimentalFeatureListParams {
                cursor: None,
                limit: None,
                thread_id: Some(thread_id),
            },
        )
        .await
    }

    pub async fn experimental_feature_enablement_set(
        &mut self,
        name: String,
        enabled: bool,
    ) -> anyhow::Result<ExperimentalFeatureEnablementSetResponse> {
        self.request(
            "experimentalFeature/enablement/set",
            ExperimentalFeatureEnablementSetParams {
                enablement: BTreeMap::from([(name, enabled)]),
            },
        )
        .await
    }

    pub async fn plugin_list(&mut self) -> anyhow::Result<Value> {
        self.request("plugin/list", EmptyParams {}).await
    }

    pub async fn plugin_installed(&mut self) -> anyhow::Result<Value> {
        self.request("plugin/installed", EmptyParams {}).await
    }

    pub async fn plugin_read(
        &mut self,
        marketplace_path: String,
        plugin_name: String,
    ) -> anyhow::Result<Value> {
        self.request(
            "plugin/read",
            PluginReadParams {
                marketplace_path,
                plugin_name,
            },
        )
        .await
    }

    pub async fn plugin_install(
        &mut self,
        marketplace_path: String,
        plugin_name: String,
    ) -> anyhow::Result<Value> {
        self.request(
            "plugin/install",
            PluginInstallParams {
                marketplace_path: Some(marketplace_path),
                remote_marketplace_name: None,
                plugin_name,
            },
        )
        .await
    }

    pub async fn plugin_uninstall(&mut self, plugin_id: String) -> anyhow::Result<Value> {
        self.request("plugin/uninstall", PluginUninstallParams { plugin_id })
            .await
    }

    pub async fn marketplace_add(
        &mut self,
        source: String,
        ref_name: Option<String>,
        sparse_paths: Option<Vec<String>>,
    ) -> anyhow::Result<MarketplaceAddResponse> {
        self.request(
            "marketplace/add",
            MarketplaceAddParams {
                source,
                ref_name,
                sparse_paths,
            },
        )
        .await
    }

    pub async fn marketplace_remove(
        &mut self,
        marketplace_name: String,
    ) -> anyhow::Result<MarketplaceRemoveResponse> {
        self.request(
            "marketplace/remove",
            MarketplaceRemoveParams { marketplace_name },
        )
        .await
    }

    pub async fn marketplace_upgrade(
        &mut self,
        marketplace_name: Option<String>,
    ) -> anyhow::Result<MarketplaceUpgradeResponse> {
        self.request(
            "marketplace/upgrade",
            MarketplaceUpgradeParams { marketplace_name },
        )
        .await
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

    pub async fn mcp_server_reload(&mut self) -> anyhow::Result<Value> {
        self.request("config/mcpServer/reload", EmptyParams {})
            .await
    }

    pub async fn mcp_server_resource_read(
        &mut self,
        thread_id: String,
        server: String,
        uri: String,
    ) -> anyhow::Result<Value> {
        self.request(
            "mcpServer/resource/read",
            McpServerResourceReadParams {
                thread_id: Some(thread_id),
                server: Some(server),
                uri,
            },
        )
        .await
    }

    pub async fn mcp_server_tool_call(
        &mut self,
        thread_id: String,
        server: String,
        tool: String,
        arguments: Value,
    ) -> anyhow::Result<Value> {
        self.request(
            "mcpServer/tool/call",
            McpServerToolCallParams {
                thread_id,
                server,
                tool,
                arguments,
                meta: None,
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

    pub async fn thread_background_terminals_terminate(
        &mut self,
        thread_id: String,
        process_id: String,
    ) -> anyhow::Result<Value> {
        self.request(
            "thread/backgroundTerminals/terminate",
            ThreadBackgroundTerminalTerminateParams {
                thread_id,
                process_id,
            },
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

    pub async fn model_provider_capabilities_read(&mut self) -> anyhow::Result<Value> {
        self.request("modelProvider/capabilities/read", EmptyParams {})
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
        params: ThreadSettingsUpdateParams,
    ) -> anyhow::Result<ThreadSettingsUpdateResponse> {
        if !params.has_settings() {
            bail!("thread/settings/update requires at least one setting");
        }

        self.request("thread/settings/update", params).await
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
        self.turn_start_until_complete_with_context(
            thread_id,
            AppServerTurnStartInput {
                input,
                additional_context: None,
            },
            cancel_rx,
            on_event,
            on_approval,
        )
        .await
    }

    pub async fn turn_start_until_complete_with_context<OnEvent, OnApproval, ApprovalFuture>(
        &mut self,
        thread_id: String,
        turn_input: AppServerTurnStartInput,
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
                    input: turn_input.input,
                    additional_context: turn_input.additional_context,
                },
            )
            .await?;

        self.wait_for_turn_until_complete(
            messages_rx,
            thread_id,
            TurnWaitState {
                active_turn_id: Some(response.turn.id),
                cancel_rx,
            },
            on_event,
            on_approval,
            no_interactive_response,
        )
        .await
    }

    pub async fn turn_start_until_complete_with_interactive<
        OnEvent,
        OnApproval,
        ApprovalFuture,
        OnInteractive,
        InteractiveFuture,
    >(
        &mut self,
        thread_id: String,
        turn_input: AppServerTurnStartInput,
        cancel_rx: Option<oneshot::Receiver<()>>,
        on_event: OnEvent,
        on_approval: OnApproval,
        on_interactive: OnInteractive,
    ) -> anyhow::Result<AppServerPromptCompletion>
    where
        OnEvent: FnMut(AppServerPromptEvent) -> anyhow::Result<()>,
        OnApproval: FnMut(AppServerApprovalRequest) -> ApprovalFuture,
        ApprovalFuture: Future<Output = anyhow::Result<AppServerApprovalDecision>>,
        OnInteractive: FnMut(AppServerInteractiveRequest) -> InteractiveFuture,
        InteractiveFuture: Future<Output = anyhow::Result<Option<Value>>>,
    {
        let messages_rx = self.subscribe();
        let response: TurnStartResponse = self
            .request(
                "turn/start",
                TurnStartParams {
                    thread_id: thread_id.clone(),
                    input: turn_input.input,
                    additional_context: turn_input.additional_context,
                },
            )
            .await?;

        self.wait_for_turn_until_complete(
            messages_rx,
            thread_id,
            TurnWaitState {
                active_turn_id: Some(response.turn.id),
                cancel_rx,
            },
            on_event,
            on_approval,
            on_interactive,
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
            TurnWaitState {
                active_turn_id: Some(response.turn.id),
                cancel_rx,
            },
            on_event,
            on_approval,
            no_interactive_response,
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
            TurnWaitState {
                active_turn_id: None,
                cancel_rx,
            },
            on_event,
            on_approval,
            no_interactive_response,
        )
        .await
    }

    pub async fn thread_shell_command_until_complete<OnEvent, OnApproval, ApprovalFuture>(
        &mut self,
        thread_id: String,
        command: String,
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
        let _response: ThreadShellCommandResponse = self
            .request(
                "thread/shellCommand",
                ThreadShellCommandParams {
                    thread_id: thread_id.clone(),
                    command,
                },
            )
            .await?;

        self.wait_for_turn_until_complete(
            messages_rx,
            thread_id,
            TurnWaitState {
                active_turn_id: None,
                cancel_rx,
            },
            on_event,
            on_approval,
            no_interactive_response,
        )
        .await
    }

    async fn wait_for_turn_until_complete<
        OnEvent,
        OnApproval,
        ApprovalFuture,
        OnInteractive,
        InteractiveFuture,
    >(
        &mut self,
        mut messages_rx: broadcast::Receiver<AppServerMessage>,
        thread_id: String,
        state: TurnWaitState,
        mut on_event: OnEvent,
        mut on_approval: OnApproval,
        mut on_interactive: OnInteractive,
    ) -> anyhow::Result<AppServerPromptCompletion>
    where
        OnEvent: FnMut(AppServerPromptEvent) -> anyhow::Result<()>,
        OnApproval: FnMut(AppServerApprovalRequest) -> ApprovalFuture,
        ApprovalFuture: Future<Output = anyhow::Result<AppServerApprovalDecision>>,
        OnInteractive: FnMut(AppServerInteractiveRequest) -> InteractiveFuture,
        InteractiveFuture: Future<Output = anyhow::Result<Option<Value>>>,
    {
        let mut active_turn_id = state.active_turn_id;
        let mut cancel_rx = state.cancel_rx;
        let mut interrupt_requested = false;
        if let Some(active_turn_id) = active_turn_id.as_ref() {
            trace!(turn_id = %active_turn_id, "codex app-server turn started");
        }
        loop {
            let message = if let Some(cancel) = cancel_rx.as_mut() {
                tokio::select! {
                    message = messages_rx.recv() => {
                        let Some(message) = receive_app_server_message(message)? else {
                            continue;
                        };
                        message
                    },
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
                loop {
                    if let Some(message) = receive_app_server_message(messages_rx.recv().await)? {
                        break message;
                    }
                }
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
                    } else if let Some(interactive) = decode_interactive_request(
                        &method, &params, &thread_id,
                    )
                    .with_context(|| {
                        format!("failed to decode app-server interactive request `{method}`")
                    })? {
                        if let Some(response) = on_interactive(interactive).await? {
                            self.write_request_response(id, response).await?;
                        } else if let Some(response) =
                            fallback_interactive_request_response(&method, &params)
                        {
                            self.write_request_response(id, response).await?;
                        } else {
                            trace!(
                                method,
                                ?params,
                                "rejecting unsupported app-server request during turn"
                            );
                            let (code, message) = app_server_request_error(&method);
                            self.write_request_error(id, code, message).await?;
                        }
                    } else if let Some(response) =
                        fallback_interactive_request_response(&method, &params)
                    {
                        self.write_request_response(id, response).await?;
                    } else {
                        trace!(
                            method,
                            ?params,
                            "rejecting unsupported app-server request during turn"
                        );
                        let (code, message) = app_server_request_error(&method);
                        self.write_request_error(id, code, message).await?;
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
                        on_event(AppServerPromptEvent::AgentMessageDelta(
                            AppServerAgentMessageDelta {
                                item_id: notification.item_id,
                                delta: notification.delta,
                            },
                        ))?;
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
                | "item/autoApprovalReview/started"
                | "item/autoApprovalReview/completed"
                | "item/commandExecution/terminalInteraction"
                | "item/commandExecution/outputDelta"
                | "item/fileChange/patchUpdated"
                | "item/plan/delta"
                | "item/reasoning/summaryTextDelta"
                | "item/reasoning/summaryPartAdded"
                | "item/reasoning/textDelta"
                | "serverRequest/resolved"
                | "turn/diff/updated"
                | "turn/plan/updated"
                | "thread/tokenUsage/updated"
                | "warning"
                | "error"
                | "model/safetyBuffering/updated"
                | "model/rerouted"
                | "model/verification"
                | "turn/moderationMetadata"
                | "mcpServer/startupStatus/updated"
                | "configWarning"
                | "windowsSandbox/setupCompleted"
                | "account/login/completed"
                | "account/updated"
                | "account/rateLimits/updated"
                | "mcpServer/oauthLogin/completed"
                | "app/list/updated"
                | "fuzzyFileSearch/sessionUpdated"
                | "fuzzyFileSearch/sessionCompleted"
                | "remoteControl/status/changed"
                | "thread/realtime/started"
                | "thread/realtime/sdp"
                | "thread/realtime/itemAdded"
                | "thread/realtime/transcript/delta"
                | "thread/realtime/transcript/done"
                | "thread/realtime/outputAudio/delta"
                | "thread/realtime/error"
                | "thread/realtime/closed" => {
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
        let params = serde_json::to_value(params)
            .with_context(|| format!("failed to encode app-server `{method}` request params"))?;

        let mut overload_retry = 0;
        loop {
            let response_rx = self.send_request(method, &params).await?;
            let message = response_rx.await.with_context(|| {
                format!("codex app-server exited before responding to `{method}`")
            })?;

            if let Some(error) = message.get("error") {
                warn!(method, %error, "codex app-server request failed");
                if app_server_request_overloaded(error)
                    && overload_retry < APP_SERVER_OVERLOAD_MAX_RETRIES
                {
                    overload_retry += 1;
                    let retry_delay = overload_retry_delay(overload_retry);
                    warn!(
                        method,
                        overload_retry,
                        ?retry_delay,
                        "retrying overloaded codex app-server request"
                    );
                    sleep(retry_delay).await;
                    continue;
                }
                if app_server_method_unavailable(error) {
                    return Err(
                        AppServerMethodUnavailable::new(method.to_owned(), error.clone()).into(),
                    );
                }
                if app_server_request_overloaded(error) {
                    return Err(AppServerOverloaded {
                        method: method.to_owned(),
                        error: error.clone(),
                    }
                    .into());
                }
                bail!("codex app-server request `{method}` failed: {error}");
            }

            let result = message
                .get("result")
                .cloned()
                .context("app-server response did not include `result`")?;
            trace!(method, "received codex app-server response");
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

    async fn write_request_error(
        &self,
        request_id: Value,
        code: i64,
        message: String,
    ) -> anyhow::Result<()> {
        let response = json!({
            "id": request_id,
            "error": {
                "code": code,
                "message": message,
            },
        });
        self.write_message(&response).await
    }
}

#[derive(Debug)]
pub struct AppServerMethodUnavailable {
    method: String,
    error: Value,
}

impl AppServerMethodUnavailable {
    pub(crate) fn new(method: String, error: Value) -> Self {
        Self { method, error }
    }

    pub fn method(&self) -> &str {
        &self.method
    }
}

impl fmt::Display for AppServerMethodUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "codex app-server method `{}` is unavailable: {}",
            self.method, self.error
        )
    }
}

impl std::error::Error for AppServerMethodUnavailable {}

#[derive(Debug)]
pub struct AppServerOverloaded {
    method: String,
    error: Value,
}

impl AppServerOverloaded {
    pub fn method(&self) -> &str {
        &self.method
    }
}

impl fmt::Display for AppServerOverloaded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "codex app-server request `{}` was rejected as overloaded after retries: {}",
            self.method, self.error
        )
    }
}

impl std::error::Error for AppServerOverloaded {}

pub fn is_app_server_method_unavailable(error: &anyhow::Error) -> Option<&str> {
    error
        .downcast_ref::<AppServerMethodUnavailable>()
        .map(AppServerMethodUnavailable::method)
}

pub fn is_app_server_overloaded(error: &anyhow::Error) -> Option<&str> {
    error
        .downcast_ref::<AppServerOverloaded>()
        .map(AppServerOverloaded::method)
}

fn app_server_method_unavailable(error: &Value) -> bool {
    if error.get("code").and_then(Value::as_i64) == Some(-32601) {
        return true;
    }
    let Some(message) = error.get("message").and_then(Value::as_str) else {
        return false;
    };
    let message = message.to_ascii_lowercase();
    message.contains("method not found")
        || message.contains("unknown method")
        || message.contains("unsupported method")
}

fn app_server_request_overloaded(error: &Value) -> bool {
    error.get("code").and_then(Value::as_i64) == Some(APP_SERVER_OVERLOADED_CODE)
}

fn overload_retry_delay(retry: usize) -> Duration {
    APP_SERVER_OVERLOAD_INITIAL_RETRY_DELAY * (1 << (retry.saturating_sub(1) as u32))
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
) -> anyhow::Result<Option<AppServerMessage>> {
    match message {
        Ok(message) => Ok(Some(message)),
        Err(broadcast::error::RecvError::Closed) => {
            bail!("codex app-server notification stream closed")
        }
        Err(broadcast::error::RecvError::Lagged(count)) => {
            warn!(
                missed_notifications = count,
                "missed codex app-server notifications; continuing with latest buffered message"
            );
            Ok(None)
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
    mcp_server_openai_form_elicitation: bool,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_workspace_roots: Option<Vec<PathBuf>>,
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
    #[serde(default)]
    pub runtime_workspace_roots: Vec<PathBuf>,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadForkParams {
    thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_workspace_roots: Option<Vec<PathBuf>>,
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
    #[serde(default)]
    pub runtime_workspace_roots: Vec<PathBuf>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadResumeParams {
    thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_workspace_roots: Option<Vec<PathBuf>>,
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
    #[serde(default)]
    pub runtime_workspace_roots: Vec<PathBuf>,
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadRollbackParams {
    thread_id: String,
    num_turns: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadRollbackResponse {
    pub thread: AppServerThread,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadTurnsListParams {
    thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort_direction: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    items_view: Option<&'static str>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadTurnsListResponse {
    pub data: Vec<AppServerTurnHistory>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    #[serde(default)]
    pub backwards_cursor: Option<String>,
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
    pub turns: Vec<AppServerTurnHistory>,
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
struct ThreadUnarchiveParams {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadUnarchiveResponse {
    pub thread: AppServerThread,
}

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

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ThreadMemoryMode {
    Enabled,
    Disabled,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadMemoryModeSetParams {
    thread_id: String,
    mode: ThreadMemoryMode,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadMemoryModeSetResponse {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryResetResponse {}

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
struct ConfigReadParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    include_layers: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AccountReadParams {
    refresh_token: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountLoginMode {
    Chatgpt,
    ChatgptDeviceCode,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum AccountLoginStartParams {
    #[serde(rename = "chatgpt", rename_all = "camelCase")]
    Chatgpt {
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        codex_streamlined_login: bool,
    },
    #[serde(rename = "chatgptDeviceCode")]
    ChatgptDeviceCode,
}

impl From<AccountLoginMode> for AccountLoginStartParams {
    fn from(mode: AccountLoginMode) -> Self {
        match mode {
            AccountLoginMode::Chatgpt => Self::Chatgpt {
                codex_streamlined_login: false,
            },
            AccountLoginMode::ChatgptDeviceCode => Self::ChatgptDeviceCode,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AccountLoginCancelParams {
    login_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExperimentalFeatureListParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExperimentalFeatureListResponse {
    pub data: Vec<ExperimentalFeature>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExperimentalFeature {
    pub name: String,
    pub stage: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub announcement: Option<String>,
    pub enabled: bool,
    pub default_enabled: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExperimentalFeatureEnablementSetParams {
    enablement: BTreeMap<String, bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExperimentalFeatureEnablementSetResponse {
    pub enablement: BTreeMap<String, bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PluginReadParams {
    marketplace_path: String,
    plugin_name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PluginInstallParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    marketplace_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote_marketplace_name: Option<String>,
    plugin_name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PluginUninstallParams {
    plugin_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MarketplaceAddParams {
    source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ref_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sparse_paths: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketplaceAddResponse {
    pub marketplace_name: String,
    pub installed_root: String,
    pub already_added: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MarketplaceRemoveParams {
    marketplace_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketplaceRemoveResponse {
    pub marketplace_name: String,
    pub installed_root: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MarketplaceUpgradeParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    marketplace_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketplaceUpgradeResponse {
    pub selected_marketplaces: Vec<String>,
    pub upgraded_roots: Vec<String>,
    pub errors: Vec<MarketplaceUpgradeErrorInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketplaceUpgradeErrorInfo {
    pub marketplace_name: String,
    pub message: String,
}

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
struct McpServerResourceReadParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    server: Option<String>,
    uri: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct McpServerToolCallParams {
    thread_id: String,
    server: String,
    tool: String,
    arguments: Value,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    meta: Option<Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadBackgroundTerminalTerminateParams {
    thread_id: String,
    process_id: String,
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
pub struct ThreadSettingsUpdateParams {
    pub thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collaboration_mode: Option<AppServerCollaborationMode>,
}

impl ThreadSettingsUpdateParams {
    pub fn new(thread_id: String) -> Self {
        Self {
            thread_id,
            model: None,
            permissions: None,
            effort: None,
            service_tier: None,
            approval_policy: None,
            collaboration_mode: None,
        }
    }

    pub fn with_model(mut self, model: String) -> Self {
        self.model = Some(model);
        self
    }

    pub fn with_permissions(mut self, permissions: String) -> Self {
        self.permissions = Some(permissions);
        self
    }

    pub fn with_effort(mut self, effort: String) -> Self {
        self.effort = Some(effort);
        self
    }

    pub fn with_service_tier(mut self, service_tier: Option<String>) -> Self {
        self.service_tier = Some(service_tier);
        self
    }

    pub fn with_approval_policy(mut self, approval_policy: String) -> Self {
        self.approval_policy = Some(approval_policy);
        self
    }

    pub fn with_collaboration_mode(
        mut self,
        collaboration_mode: AppServerCollaborationMode,
    ) -> Self {
        self.collaboration_mode = Some(collaboration_mode);
        self
    }

    fn has_settings(&self) -> bool {
        self.model.is_some()
            || self.permissions.is_some()
            || self.effort.is_some()
            || self.service_tier.is_some()
            || self.approval_policy.is_some()
            || self.collaboration_mode.is_some()
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    additional_context: Option<AppServerAdditionalContext>,
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
struct ThreadShellCommandParams {
    thread_id: String,
    command: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadShellCommandResponse {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnInterruptParams {
    thread_id: String,
    turn_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnInterruptResponse {}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum AppServerTurnInput {
    Text { text: String },
    Image { url: String },
    Skill { name: String, path: String },
    Mention { name: String, path: String },
}

#[derive(Clone, Debug, Default)]
pub struct AppServerTurnStartInput {
    pub input: Vec<AppServerTurnInput>,
    pub additional_context: Option<AppServerAdditionalContext>,
}

pub type AppServerAdditionalContext = HashMap<String, AppServerAdditionalContextEntry>;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerAdditionalContextEntry {
    pub value: String,
    pub kind: AppServerAdditionalContextKind,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AppServerAdditionalContextKind {
    Untrusted,
    Application,
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
    #[serde(default)]
    item_id: Option<String>,
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
    AgentMessageDelta(AppServerAgentMessageDelta),
    AgentThoughtDelta(String),
    ToolCallStarted(AppServerToolCall),
    ToolCallUpdated(AppServerToolCallUpdate),
    GuardianApprovalReview(AppServerGuardianApprovalReviewUpdate),
    ServerRequestResolved(AppServerServerRequestResolvedUpdate),
    PlanUpdated(Vec<AppServerPlanEntry>),
    TurnDiffUpdated { turn_id: String, diff: String },
    UsageUpdated(AppServerUsage),
    SkillsChanged,
    ThreadSettingsUpdated(AppServerThreadSettingsUpdate),
    Warning(AppServerWarningUpdate),
    Error(AppServerErrorUpdate),
    ModelSafetyBuffering(AppServerModelSafetyBufferingUpdate),
    ModelRerouted(AppServerModelReroutedUpdate),
    ModelVerification(AppServerModelVerificationUpdate),
    TurnModerationMetadata(AppServerTurnModerationMetadataUpdate),
    McpServerStartupStatus(AppServerMcpServerStartupStatusUpdate),
    Realtime(AppServerRealtimeUpdate),
    ConfigWarning(AppServerConfigWarningUpdate),
    WindowsSandboxSetup(AppServerWindowsSandboxSetupUpdate),
    AccountLoginCompleted(AppServerAccountLoginCompletedUpdate),
    AccountUpdated(AppServerAccountUpdatedUpdate),
    AccountRateLimitsUpdated(AppServerAccountRateLimitsUpdatedUpdate),
    McpServerOAuthLoginCompleted(AppServerMcpServerOAuthLoginCompletedUpdate),
    AppListUpdated(AppServerAppListUpdate),
    FuzzyFileSearch(AppServerFuzzyFileSearchUpdate),
    RemoteControlStatus(AppServerRemoteControlStatusUpdate),
}

#[derive(Debug, Clone)]
pub struct AppServerAgentMessageDelta {
    pub item_id: Option<String>,
    pub delta: String,
}

pub enum AppServerHistoryEvent {
    UserMessage(String),
    PromptEvent(Box<AppServerPromptEvent>),
}

#[derive(Debug, Clone)]
pub struct AppServerConfigWarningUpdate {
    pub summary: String,
    pub details: Option<String>,
    pub path: Option<String>,
    pub range: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct AppServerWindowsSandboxSetupUpdate {
    pub mode: String,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppServerRemoteControlStatusUpdate {
    pub status: String,
    pub server_name: String,
    pub environment_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppServerAccountLoginCompletedUpdate {
    pub login_id: Option<String>,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppServerAccountUpdatedUpdate {
    pub auth_mode: Option<String>,
    pub plan_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppServerAccountRateLimitsUpdatedUpdate {
    pub rate_limits: Value,
}

#[derive(Debug, Clone)]
pub struct AppServerMcpServerOAuthLoginCompletedUpdate {
    pub name: String,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppServerAppListUpdate {
    pub data: Value,
}

#[derive(Debug, Clone)]
pub enum AppServerFuzzyFileSearchUpdate {
    SessionUpdated {
        session_id: String,
        query: String,
        files: Value,
    },
    SessionCompleted {
        session_id: String,
        query: String,
    },
}

pub enum AppServerPromptCompletion {
    EndTurn,
    Cancelled,
}

struct TurnWaitState {
    active_turn_id: Option<String>,
    cancel_rx: Option<oneshot::Receiver<()>>,
}

#[derive(Debug, Clone)]
pub struct AppServerApprovalRequest {
    pub item_id: String,
    pub title: String,
    pub kind: AppServerToolKind,
    pub raw: Value,
    pub options: Vec<AppServerApprovalChoice>,
    pub response_kind: AppServerApprovalResponseKind,
}

#[derive(Debug, Clone)]
pub enum AppServerInteractiveRequest {
    McpElicitation(AppServerMcpElicitationRequest),
    UserInput(AppServerUserInputRequest),
    DynamicToolCall(AppServerDynamicToolCallRequest),
}

#[derive(Debug, Clone)]
pub struct AppServerMcpElicitationRequest {
    pub raw: Value,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub server_name: String,
    pub mode: String,
    pub message: String,
    pub requested_schema: Option<Value>,
    pub url: Option<String>,
    pub elicitation_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppServerUserInputRequest {
    pub raw: Value,
    pub method: String,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub questions: Vec<AppServerUserInputQuestion>,
}

#[derive(Debug, Clone)]
pub struct AppServerDynamicToolCallRequest {
    pub raw: Value,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub call_id: String,
    pub namespace: Option<String>,
    pub tool: String,
    pub arguments: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct AppServerUserInputQuestion {
    pub id: String,
    pub header: Option<String>,
    pub question: String,
    pub options: Vec<AppServerUserInputOption>,
}

#[derive(Debug, Clone)]
pub struct AppServerUserInputOption {
    pub label: String,
    pub description: Option<String>,
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
    AcceptWithExecpolicyAmendment,
    ApplyNetworkPolicyAmendment,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppServerApprovalChoice {
    pub option: AppServerApprovalOption,
    pub option_id: String,
    pub label: String,
    pub available_decision: Option<Value>,
}

impl AppServerApprovalChoice {
    fn new(option: AppServerApprovalOption) -> Self {
        Self {
            option,
            option_id: option.id().to_owned(),
            label: option.label().to_owned(),
            available_decision: None,
        }
    }

    fn with_available_decision(
        option: AppServerApprovalOption,
        index: usize,
        available_decision: Value,
    ) -> Self {
        Self {
            option,
            option_id: format!("{}:{index}", option.id()),
            label: option.label().to_owned(),
            available_decision: Some(available_decision),
        }
    }

    fn partial_permission(
        option: AppServerApprovalOption,
        option_id: String,
        label: String,
        permissions: Value,
        scope: &'static str,
    ) -> Self {
        Self {
            option,
            option_id,
            label,
            available_decision: Some(json!({
                "permissions": permissions,
                "scope": scope,
            })),
        }
    }

    pub fn id(&self) -> &str {
        &self.option_id
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn decision(&self) -> AppServerApprovalDecision {
        match &self.available_decision {
            Some(decision) => AppServerApprovalDecision::Raw(decision.clone()),
            None => self.option.simple_decision(),
        }
    }
}

impl AppServerApprovalOption {
    pub fn id(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::AcceptForSession => "acceptForSession",
            Self::AcceptWithExecpolicyAmendment => "acceptWithExecpolicyAmendment",
            Self::ApplyNetworkPolicyAmendment => "applyNetworkPolicyAmendment",
            Self::Decline => "decline",
            Self::Cancel => "cancel",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Accept => "Allow once",
            Self::AcceptForSession => "Allow for session",
            Self::AcceptWithExecpolicyAmendment => "Allow similar commands",
            Self::ApplyNetworkPolicyAmendment => "Apply network rule",
            Self::Decline => "Reject",
            Self::Cancel => "Cancel",
        }
    }

    fn simple_decision(self) -> AppServerApprovalDecision {
        match self {
            Self::Accept => AppServerApprovalDecision::Accept,
            Self::AcceptForSession => AppServerApprovalDecision::AcceptForSession,
            Self::AcceptWithExecpolicyAmendment => AppServerApprovalDecision::Cancel,
            Self::ApplyNetworkPolicyAmendment => AppServerApprovalDecision::Cancel,
            Self::Decline => AppServerApprovalDecision::Decline,
            Self::Cancel => AppServerApprovalDecision::Cancel,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AppServerApprovalDecision {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
    Raw(Value),
}

impl AppServerApprovalDecision {
    fn into_app_server_value(self) -> Value {
        match self {
            Self::Accept => Value::String("accept".to_owned()),
            Self::AcceptForSession => Value::String("acceptForSession".to_owned()),
            Self::Decline => Value::String("decline".to_owned()),
            Self::Cancel => Value::String("cancel".to_owned()),
            Self::Raw(decision) => decision,
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
    pub diffs: Vec<AppServerFileDiff>,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct AppServerFileDiff {
    pub path: String,
    pub diff: String,
}

#[derive(Debug, Clone)]
pub struct AppServerGuardianApprovalReviewUpdate {
    pub lifecycle: AppServerGuardianApprovalReviewLifecycle,
    pub thread_id: String,
    pub turn_id: String,
    pub review_id: String,
    pub target_item_id: Option<String>,
    pub review: Value,
    pub action: Value,
    pub started_at_ms: Option<u64>,
    pub completed_at_ms: Option<u64>,
    pub decision_source: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppServerServerRequestResolvedUpdate {
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub request_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppServerGuardianApprovalReviewLifecycle {
    Started,
    Completed,
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
pub struct AppServerThreadUnarchivedUpdate {
    pub thread_id: String,
}

#[derive(Debug, Clone)]
pub struct AppServerThreadDeletedUpdate {
    pub thread_id: String,
}

#[derive(Debug, Clone)]
pub struct AppServerThreadClosedUpdate {
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

#[derive(Debug, Clone)]
pub struct AppServerWarningUpdate {
    pub thread_id: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct AppServerErrorUpdate {
    pub thread_id: String,
    pub turn_id: String,
    pub message: String,
    pub will_retry: bool,
    pub codex_error_info: Option<Value>,
    pub additional_details: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppServerModelReroutedUpdate {
    pub thread_id: String,
    pub turn_id: String,
    pub from_model: String,
    pub to_model: String,
    pub reason: Value,
}

#[derive(Debug, Clone)]
pub struct AppServerModelSafetyBufferingUpdate {
    pub thread_id: String,
    pub turn_id: String,
    pub model: String,
    pub use_cases: Vec<String>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AppServerModelVerificationUpdate {
    pub thread_id: String,
    pub turn_id: String,
    pub verifications: Value,
}

#[derive(Debug, Clone)]
pub struct AppServerTurnModerationMetadataUpdate {
    pub thread_id: String,
    pub turn_id: String,
    pub metadata: Value,
}

#[derive(Debug, Clone)]
pub struct AppServerMcpServerStartupStatusUpdate {
    pub thread_id: Option<String>,
    pub name: String,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub enum AppServerRealtimeUpdate {
    Started {
        thread_id: String,
        realtime_session_id: Option<String>,
    },
    Sdp {
        thread_id: String,
        sdp: String,
    },
    ItemAdded {
        thread_id: String,
        item: Value,
    },
    TranscriptDelta {
        thread_id: String,
        role: String,
        delta: String,
    },
    TranscriptDone {
        thread_id: String,
        role: String,
        text: String,
    },
    OutputAudioDelta {
        thread_id: String,
        audio: AppServerRealtimeAudioDelta,
    },
    Error {
        thread_id: String,
        message: String,
    },
    Closed {
        thread_id: String,
        reason: String,
    },
}

impl AppServerRealtimeUpdate {
    pub fn thread_id(&self) -> &str {
        match self {
            Self::Started { thread_id, .. }
            | Self::Sdp { thread_id, .. }
            | Self::ItemAdded { thread_id, .. }
            | Self::TranscriptDelta { thread_id, .. }
            | Self::TranscriptDone { thread_id, .. }
            | Self::OutputAudioDelta { thread_id, .. }
            | Self::Error { thread_id, .. }
            | Self::Closed { thread_id, .. } => thread_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppServerRealtimeAudioDelta {
    pub data: Option<String>,
    pub sample_rate: Option<u64>,
    pub num_channels: Option<u64>,
    pub samples_per_channel: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadArchivedNotification {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadUnarchivedNotification {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadDeletedNotification {
    thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadClosedNotification {
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
struct WarningNotification {
    #[serde(default)]
    thread_id: Option<String>,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ErrorNotification {
    error: TurnErrorNotification,
    will_retry: bool,
    thread_id: String,
    turn_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GuardianApprovalReviewStartedNotification {
    thread_id: String,
    turn_id: String,
    started_at_ms: u64,
    review_id: String,
    #[serde(default)]
    target_item_id: Option<String>,
    review: Value,
    action: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GuardianApprovalReviewCompletedNotification {
    thread_id: String,
    turn_id: String,
    started_at_ms: u64,
    completed_at_ms: u64,
    review_id: String,
    #[serde(default)]
    target_item_id: Option<String>,
    decision_source: String,
    review: Value,
    action: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerRequestResolvedNotification {
    thread_id: String,
    #[serde(default)]
    turn_id: Option<String>,
    request_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnErrorNotification {
    message: String,
    #[serde(default)]
    codex_error_info: Option<Value>,
    #[serde(default)]
    additional_details: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelReroutedNotification {
    thread_id: String,
    turn_id: String,
    from_model: String,
    to_model: String,
    reason: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelSafetyBufferingUpdatedNotification {
    thread_id: String,
    turn_id: String,
    model: String,
    use_cases: Vec<String>,
    reasons: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelVerificationNotification {
    thread_id: String,
    turn_id: String,
    verifications: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnModerationMetadataNotification {
    thread_id: String,
    turn_id: String,
    metadata: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpServerStartupStatusUpdatedNotification {
    thread_id: Option<String>,
    name: String,
    status: String,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealtimeStartedNotification {
    thread_id: String,
    realtime_session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealtimeSdpNotification {
    thread_id: String,
    sdp: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealtimeItemAddedNotification {
    thread_id: String,
    item: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealtimeTranscriptDeltaNotification {
    thread_id: String,
    role: String,
    delta: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealtimeTranscriptDoneNotification {
    thread_id: String,
    role: String,
    text: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealtimeOutputAudioDeltaNotification {
    thread_id: String,
    audio: RealtimeAudioDeltaNotification,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealtimeAudioDeltaNotification {
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    sample_rate: Option<u64>,
    #[serde(default)]
    num_channels: Option<u64>,
    #[serde(default)]
    samples_per_channel: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealtimeErrorNotification {
    thread_id: String,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RealtimeClosedNotification {
    thread_id: String,
    reason: String,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigWarningNotification {
    summary: String,
    #[serde(default)]
    details: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    range: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WindowsSandboxSetupCompletedNotification {
    mode: String,
    success: bool,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountLoginCompletedNotification {
    #[serde(default)]
    login_id: Option<String>,
    success: bool,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountUpdatedNotification {
    #[serde(default)]
    auth_mode: Option<String>,
    #[serde(default)]
    plan_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountRateLimitsUpdatedNotification {
    rate_limits: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpServerOAuthLoginCompletedNotification {
    name: String,
    success: bool,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AppListUpdatedNotification {
    data: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteControlStatusChangedNotification {
    status: String,
    server_name: String,
    #[serde(default)]
    environment_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FuzzyFileSearchSessionUpdatedNotification {
    session_id: String,
    query: String,
    files: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FuzzyFileSearchSessionCompletedNotification {
    session_id: String,
    query: String,
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
        "item/autoApprovalReview/started" | "item/autoApprovalReview/completed" => {
            decode_guardian_approval_review_update(method, params)
                .map(|update| Some(AppServerPromptEvent::GuardianApprovalReview(update)))
        }
        "serverRequest/resolved" => decode_server_request_resolved(params)
            .map(|update| Some(AppServerPromptEvent::ServerRequestResolved(update))),
        "item/commandExecution/terminalInteraction" => {
            let Some(item_id) = string_field(params, "itemId") else {
                return Ok(None);
            };
            let Some(process_id) = string_field(params, "processId") else {
                return Ok(None);
            };
            let Some(stdin) = string_field(params, "stdin") else {
                return Ok(None);
            };
            Ok(Some(AppServerPromptEvent::ToolCallUpdated(
                AppServerToolCallUpdate {
                    id: item_id,
                    title: None,
                    kind: None,
                    status: None,
                    output_delta: Some(terminal_interaction_output(&process_id, &stdin)),
                    diffs: Vec::new(),
                    raw: None,
                },
            )))
        }
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
                    diffs: Vec::new(),
                    raw: None,
                },
            )))
        }
        "item/fileChange/patchUpdated" => {
            let Some(item_id) = string_field(params, "itemId") else {
                return Ok(None);
            };
            Ok(Some(AppServerPromptEvent::ToolCallUpdated(
                AppServerToolCallUpdate {
                    id: item_id,
                    title: None,
                    kind: Some(AppServerToolKind::Edit),
                    status: None,
                    output_delta: None,
                    diffs: file_change_diffs(params),
                    raw: Some(params.clone()),
                },
            )))
        }
        "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => Ok(params
            .get("delta")
            .and_then(Value::as_str)
            .map(|delta| AppServerPromptEvent::AgentThoughtDelta(delta.to_owned()))),
        "item/reasoning/summaryPartAdded" => {
            let summary_index = params
                .get("summaryIndex")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            Ok((summary_index > 0)
                .then(|| AppServerPromptEvent::AgentThoughtDelta("\n\n".to_owned())))
        }
        "item/plan/delta" => Ok(params
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
        "warning" => {
            let update = decode_warning(params)?;
            if update
                .thread_id
                .as_deref()
                .is_some_and(|id| id != active_thread_id)
            {
                return Ok(None);
            }
            Ok(Some(AppServerPromptEvent::Warning(update)))
        }
        "error" => {
            let update = decode_error(params)?;
            if update.thread_id != active_thread_id
                || Some(update.turn_id.as_str()) != active_turn_id
            {
                return Ok(None);
            }
            Ok(Some(AppServerPromptEvent::Error(update)))
        }
        "model/rerouted" => {
            let update = decode_model_rerouted(params)?;
            if update.thread_id != active_thread_id
                || Some(update.turn_id.as_str()) != active_turn_id
            {
                return Ok(None);
            }
            Ok(Some(AppServerPromptEvent::ModelRerouted(update)))
        }
        "model/safetyBuffering/updated" => {
            let update = decode_model_safety_buffering_updated(params)?;
            if update.thread_id != active_thread_id
                || Some(update.turn_id.as_str()) != active_turn_id
            {
                return Ok(None);
            }
            Ok(Some(AppServerPromptEvent::ModelSafetyBuffering(update)))
        }
        "model/verification" => {
            let update = decode_model_verification(params)?;
            if update.thread_id != active_thread_id
                || Some(update.turn_id.as_str()) != active_turn_id
            {
                return Ok(None);
            }
            Ok(Some(AppServerPromptEvent::ModelVerification(update)))
        }
        "turn/moderationMetadata" => {
            let update = decode_turn_moderation_metadata(params)?;
            if update.thread_id != active_thread_id
                || Some(update.turn_id.as_str()) != active_turn_id
            {
                return Ok(None);
            }
            Ok(Some(AppServerPromptEvent::TurnModerationMetadata(update)))
        }
        "mcpServer/startupStatus/updated" => {
            let update = decode_mcp_server_startup_status_updated(params)?;
            if let Some(thread_id) = update.thread_id.as_deref()
                && thread_id != active_thread_id
            {
                return Ok(None);
            }
            Ok(Some(AppServerPromptEvent::McpServerStartupStatus(update)))
        }
        "configWarning" => {
            let update = decode_config_warning(params)?;
            Ok(Some(AppServerPromptEvent::ConfigWarning(update)))
        }
        "windowsSandbox/setupCompleted" => {
            let update = decode_windows_sandbox_setup_completed(params)?;
            Ok(Some(AppServerPromptEvent::WindowsSandboxSetup(update)))
        }
        "account/login/completed" => {
            let update = decode_account_login_completed(params)?;
            Ok(Some(AppServerPromptEvent::AccountLoginCompleted(update)))
        }
        "account/updated" => {
            let update = decode_account_updated(params)?;
            Ok(Some(AppServerPromptEvent::AccountUpdated(update)))
        }
        "account/rateLimits/updated" => {
            let update = decode_account_rate_limits_updated(params)?;
            Ok(Some(AppServerPromptEvent::AccountRateLimitsUpdated(update)))
        }
        "mcpServer/oauthLogin/completed" => {
            let update = decode_mcp_server_oauth_login_completed(params)?;
            Ok(Some(AppServerPromptEvent::McpServerOAuthLoginCompleted(
                update,
            )))
        }
        "app/list/updated" => {
            let update = decode_app_list_updated(params)?;
            Ok(Some(AppServerPromptEvent::AppListUpdated(update)))
        }
        "fuzzyFileSearch/sessionUpdated" | "fuzzyFileSearch/sessionCompleted" => {
            let update = decode_fuzzy_file_search_update(method, params)?;
            Ok(Some(AppServerPromptEvent::FuzzyFileSearch(update)))
        }
        "remoteControl/status/changed" => {
            let update = decode_remote_control_status_changed(params)?;
            Ok(Some(AppServerPromptEvent::RemoteControlStatus(update)))
        }
        "thread/realtime/started"
        | "thread/realtime/sdp"
        | "thread/realtime/itemAdded"
        | "thread/realtime/transcript/delta"
        | "thread/realtime/transcript/done"
        | "thread/realtime/outputAudio/delta"
        | "thread/realtime/error"
        | "thread/realtime/closed" => {
            let update = decode_realtime_update(method, params)?;
            if update.thread_id() != active_thread_id {
                return Ok(None);
            }
            Ok(Some(AppServerPromptEvent::Realtime(update)))
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
            let default_options = [
                AppServerApprovalOption::Accept,
                AppServerApprovalOption::AcceptForSession,
                AppServerApprovalOption::Decline,
                AppServerApprovalOption::Cancel,
            ];
            AppServerApprovalRequest {
                item_id,
                title: permissions_approval_title(params, &requested_permissions),
                kind: AppServerToolKind::Other,
                raw: params.clone(),
                response_kind: AppServerApprovalResponseKind::Permissions {
                    requested_permissions: requested_permissions.clone(),
                },
                options: permission_approval_options(
                    params,
                    &default_options,
                    &requested_permissions,
                ),
            }
        }
        _ => return Ok(None),
    };

    Ok(Some(request))
}

fn decode_interactive_request(
    method: &str,
    params: &Value,
    active_thread_id: &str,
) -> anyhow::Result<Option<AppServerInteractiveRequest>> {
    match method {
        "mcpServer/elicitation/request" => {
            let thread_id = required_string(params, "threadId")?;
            if thread_id != active_thread_id {
                return Ok(None);
            }
            Ok(Some(AppServerInteractiveRequest::McpElicitation(
                AppServerMcpElicitationRequest {
                    raw: params.clone(),
                    thread_id,
                    turn_id: optional_string(params, "turnId"),
                    server_name: required_string(params, "serverName")?,
                    mode: string_field(params, "mode")
                        .or_else(|| string_field(params, "type"))
                        .context("missing `mode`")?,
                    message: string_field(params, "message")
                        .unwrap_or_else(|| "Additional input is required.".to_owned()),
                    requested_schema: params.get("requestedSchema").cloned(),
                    url: optional_string(params, "url"),
                    elicitation_id: optional_string(params, "elicitationId"),
                },
            )))
        }
        "item/tool/requestUserInput" | "tool/requestUserInput" => {
            let thread_id = required_string(params, "threadId")?;
            if thread_id != active_thread_id {
                return Ok(None);
            }
            Ok(Some(AppServerInteractiveRequest::UserInput(
                AppServerUserInputRequest {
                    raw: params.clone(),
                    method: method.to_owned(),
                    thread_id,
                    turn_id: optional_string(params, "turnId"),
                    item_id: optional_string(params, "itemId"),
                    questions: decode_user_input_questions(params)?,
                },
            )))
        }
        "item/tool/call" => {
            let thread_id = required_string(params, "threadId")?;
            if thread_id != active_thread_id {
                return Ok(None);
            }
            Ok(Some(AppServerInteractiveRequest::DynamicToolCall(
                AppServerDynamicToolCallRequest {
                    raw: params.clone(),
                    thread_id,
                    turn_id: optional_string(params, "turnId"),
                    call_id: required_string(params, "callId")?,
                    namespace: optional_string(params, "namespace"),
                    tool: required_string(params, "tool")?,
                    arguments: params.get("arguments").cloned(),
                },
            )))
        }
        _ => Ok(None),
    }
}

fn decode_user_input_questions(params: &Value) -> anyhow::Result<Vec<AppServerUserInputQuestion>> {
    let questions = params
        .get("questions")
        .and_then(Value::as_array)
        .context("missing `questions`")?;
    questions
        .iter()
        .map(|question| {
            let options = question
                .get("options")
                .and_then(Value::as_array)
                .map(|options| {
                    options
                        .iter()
                        .map(|option| {
                            Ok(AppServerUserInputOption {
                                label: required_string(option, "label")?,
                                description: optional_string(option, "description"),
                            })
                        })
                        .collect::<anyhow::Result<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default();
            Ok(AppServerUserInputQuestion {
                id: required_string(question, "id")?,
                header: optional_string(question, "header"),
                question: required_string(question, "question")?,
                options,
            })
        })
        .collect()
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

pub fn decode_thread_unarchived(params: &Value) -> anyhow::Result<AppServerThreadUnarchivedUpdate> {
    let notification: ThreadUnarchivedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerThreadUnarchivedUpdate {
        thread_id: notification.thread_id,
    })
}

pub fn decode_thread_deleted(params: &Value) -> anyhow::Result<AppServerThreadDeletedUpdate> {
    let notification: ThreadDeletedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerThreadDeletedUpdate {
        thread_id: notification.thread_id,
    })
}

pub fn decode_thread_closed(params: &Value) -> anyhow::Result<AppServerThreadClosedUpdate> {
    let notification: ThreadClosedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerThreadClosedUpdate {
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

pub fn decode_warning(params: &Value) -> anyhow::Result<AppServerWarningUpdate> {
    let notification: WarningNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerWarningUpdate {
        thread_id: notification.thread_id,
        message: notification.message,
    })
}

pub fn decode_config_warning(params: &Value) -> anyhow::Result<AppServerConfigWarningUpdate> {
    let notification: ConfigWarningNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerConfigWarningUpdate {
        summary: notification.summary,
        details: notification.details,
        path: notification.path,
        range: notification.range,
    })
}

pub fn decode_windows_sandbox_setup_completed(
    params: &Value,
) -> anyhow::Result<AppServerWindowsSandboxSetupUpdate> {
    let notification: WindowsSandboxSetupCompletedNotification =
        serde_json::from_value(params.clone())?;
    Ok(AppServerWindowsSandboxSetupUpdate {
        mode: notification.mode,
        success: notification.success,
        error: notification.error,
    })
}

pub fn decode_account_login_completed(
    params: &Value,
) -> anyhow::Result<AppServerAccountLoginCompletedUpdate> {
    let notification: AccountLoginCompletedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerAccountLoginCompletedUpdate {
        login_id: notification.login_id,
        success: notification.success,
        error: notification.error,
    })
}

pub fn decode_account_updated(params: &Value) -> anyhow::Result<AppServerAccountUpdatedUpdate> {
    let notification: AccountUpdatedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerAccountUpdatedUpdate {
        auth_mode: notification.auth_mode,
        plan_type: notification.plan_type,
    })
}

pub fn decode_account_rate_limits_updated(
    params: &Value,
) -> anyhow::Result<AppServerAccountRateLimitsUpdatedUpdate> {
    let notification: AccountRateLimitsUpdatedNotification =
        serde_json::from_value(params.clone())?;
    Ok(AppServerAccountRateLimitsUpdatedUpdate {
        rate_limits: notification.rate_limits,
    })
}

pub fn decode_mcp_server_oauth_login_completed(
    params: &Value,
) -> anyhow::Result<AppServerMcpServerOAuthLoginCompletedUpdate> {
    let notification: McpServerOAuthLoginCompletedNotification =
        serde_json::from_value(params.clone())?;
    Ok(AppServerMcpServerOAuthLoginCompletedUpdate {
        name: notification.name,
        success: notification.success,
        error: notification.error,
    })
}

pub fn decode_app_list_updated(params: &Value) -> anyhow::Result<AppServerAppListUpdate> {
    let notification: AppListUpdatedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerAppListUpdate {
        data: notification.data,
    })
}

pub fn decode_remote_control_status_changed(
    params: &Value,
) -> anyhow::Result<AppServerRemoteControlStatusUpdate> {
    let notification: RemoteControlStatusChangedNotification =
        serde_json::from_value(params.clone())?;
    Ok(AppServerRemoteControlStatusUpdate {
        status: notification.status,
        server_name: notification.server_name,
        environment_id: notification.environment_id,
    })
}

pub fn decode_fuzzy_file_search_update(
    method: &str,
    params: &Value,
) -> anyhow::Result<AppServerFuzzyFileSearchUpdate> {
    match method {
        "fuzzyFileSearch/sessionUpdated" => {
            let notification: FuzzyFileSearchSessionUpdatedNotification =
                serde_json::from_value(params.clone())?;
            Ok(AppServerFuzzyFileSearchUpdate::SessionUpdated {
                session_id: notification.session_id,
                query: notification.query,
                files: notification.files,
            })
        }
        "fuzzyFileSearch/sessionCompleted" => {
            let notification: FuzzyFileSearchSessionCompletedNotification =
                serde_json::from_value(params.clone())?;
            Ok(AppServerFuzzyFileSearchUpdate::SessionCompleted {
                session_id: notification.session_id,
                query: notification.query,
            })
        }
        _ => anyhow::bail!("unsupported fuzzy file search notification `{method}`"),
    }
}

pub fn decode_error(params: &Value) -> anyhow::Result<AppServerErrorUpdate> {
    let notification: ErrorNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerErrorUpdate {
        thread_id: notification.thread_id,
        turn_id: notification.turn_id,
        message: notification.error.message,
        will_retry: notification.will_retry,
        codex_error_info: notification.error.codex_error_info,
        additional_details: notification.error.additional_details,
    })
}

pub fn decode_guardian_approval_review_update(
    method: &str,
    params: &Value,
) -> anyhow::Result<AppServerGuardianApprovalReviewUpdate> {
    match method {
        "item/autoApprovalReview/started" => {
            let notification: GuardianApprovalReviewStartedNotification =
                serde_json::from_value(params.clone())?;
            Ok(AppServerGuardianApprovalReviewUpdate {
                lifecycle: AppServerGuardianApprovalReviewLifecycle::Started,
                thread_id: notification.thread_id,
                turn_id: notification.turn_id,
                review_id: notification.review_id,
                target_item_id: notification.target_item_id,
                review: notification.review,
                action: notification.action,
                started_at_ms: Some(notification.started_at_ms),
                completed_at_ms: None,
                decision_source: None,
            })
        }
        "item/autoApprovalReview/completed" => {
            let notification: GuardianApprovalReviewCompletedNotification =
                serde_json::from_value(params.clone())?;
            Ok(AppServerGuardianApprovalReviewUpdate {
                lifecycle: AppServerGuardianApprovalReviewLifecycle::Completed,
                thread_id: notification.thread_id,
                turn_id: notification.turn_id,
                review_id: notification.review_id,
                target_item_id: notification.target_item_id,
                review: notification.review,
                action: notification.action,
                started_at_ms: Some(notification.started_at_ms),
                completed_at_ms: Some(notification.completed_at_ms),
                decision_source: Some(notification.decision_source),
            })
        }
        _ => anyhow::bail!("unsupported Guardian approval review notification `{method}`"),
    }
}

pub fn decode_server_request_resolved(
    params: &Value,
) -> anyhow::Result<AppServerServerRequestResolvedUpdate> {
    let notification: ServerRequestResolvedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerServerRequestResolvedUpdate {
        thread_id: notification.thread_id,
        turn_id: notification.turn_id,
        request_id: notification.request_id,
    })
}

pub fn decode_model_rerouted(params: &Value) -> anyhow::Result<AppServerModelReroutedUpdate> {
    let notification: ModelReroutedNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerModelReroutedUpdate {
        thread_id: notification.thread_id,
        turn_id: notification.turn_id,
        from_model: notification.from_model,
        to_model: notification.to_model,
        reason: notification.reason,
    })
}

pub fn decode_model_safety_buffering_updated(
    params: &Value,
) -> anyhow::Result<AppServerModelSafetyBufferingUpdate> {
    let notification: ModelSafetyBufferingUpdatedNotification =
        serde_json::from_value(params.clone())?;
    Ok(AppServerModelSafetyBufferingUpdate {
        thread_id: notification.thread_id,
        turn_id: notification.turn_id,
        model: notification.model,
        use_cases: notification.use_cases,
        reasons: notification.reasons,
    })
}

pub fn decode_model_verification(
    params: &Value,
) -> anyhow::Result<AppServerModelVerificationUpdate> {
    let notification: ModelVerificationNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerModelVerificationUpdate {
        thread_id: notification.thread_id,
        turn_id: notification.turn_id,
        verifications: notification.verifications,
    })
}

pub fn decode_turn_moderation_metadata(
    params: &Value,
) -> anyhow::Result<AppServerTurnModerationMetadataUpdate> {
    let notification: TurnModerationMetadataNotification = serde_json::from_value(params.clone())?;
    Ok(AppServerTurnModerationMetadataUpdate {
        thread_id: notification.thread_id,
        turn_id: notification.turn_id,
        metadata: notification.metadata,
    })
}

pub fn decode_mcp_server_startup_status_updated(
    params: &Value,
) -> anyhow::Result<AppServerMcpServerStartupStatusUpdate> {
    let notification: McpServerStartupStatusUpdatedNotification =
        serde_json::from_value(params.clone())?;
    Ok(AppServerMcpServerStartupStatusUpdate {
        thread_id: notification.thread_id,
        name: notification.name,
        status: notification.status,
        error: notification.error,
    })
}

pub fn decode_realtime_update(
    method: &str,
    params: &Value,
) -> anyhow::Result<AppServerRealtimeUpdate> {
    match method {
        "thread/realtime/started" => {
            let notification: RealtimeStartedNotification = serde_json::from_value(params.clone())?;
            Ok(AppServerRealtimeUpdate::Started {
                thread_id: notification.thread_id,
                realtime_session_id: notification.realtime_session_id,
            })
        }
        "thread/realtime/sdp" => {
            let notification: RealtimeSdpNotification = serde_json::from_value(params.clone())?;
            Ok(AppServerRealtimeUpdate::Sdp {
                thread_id: notification.thread_id,
                sdp: notification.sdp,
            })
        }
        "thread/realtime/itemAdded" => {
            let notification: RealtimeItemAddedNotification =
                serde_json::from_value(params.clone())?;
            Ok(AppServerRealtimeUpdate::ItemAdded {
                thread_id: notification.thread_id,
                item: notification.item,
            })
        }
        "thread/realtime/transcript/delta" => {
            let notification: RealtimeTranscriptDeltaNotification =
                serde_json::from_value(params.clone())?;
            Ok(AppServerRealtimeUpdate::TranscriptDelta {
                thread_id: notification.thread_id,
                role: notification.role,
                delta: notification.delta,
            })
        }
        "thread/realtime/transcript/done" => {
            let notification: RealtimeTranscriptDoneNotification =
                serde_json::from_value(params.clone())?;
            Ok(AppServerRealtimeUpdate::TranscriptDone {
                thread_id: notification.thread_id,
                role: notification.role,
                text: notification.text,
            })
        }
        "thread/realtime/outputAudio/delta" => {
            let notification: RealtimeOutputAudioDeltaNotification =
                serde_json::from_value(params.clone())?;
            Ok(AppServerRealtimeUpdate::OutputAudioDelta {
                thread_id: notification.thread_id,
                audio: AppServerRealtimeAudioDelta {
                    data: notification.audio.data,
                    sample_rate: notification.audio.sample_rate,
                    num_channels: notification.audio.num_channels,
                    samples_per_channel: notification.audio.samples_per_channel,
                },
            })
        }
        "thread/realtime/error" => {
            let notification: RealtimeErrorNotification = serde_json::from_value(params.clone())?;
            Ok(AppServerRealtimeUpdate::Error {
                thread_id: notification.thread_id,
                message: notification.message,
            })
        }
        "thread/realtime/closed" => {
            let notification: RealtimeClosedNotification = serde_json::from_value(params.clone())?;
            Ok(AppServerRealtimeUpdate::Closed {
                thread_id: notification.thread_id,
                reason: notification.reason,
            })
        }
        _ => anyhow::bail!("unsupported realtime notification `{method}`"),
    }
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
) -> Vec<AppServerApprovalChoice> {
    let parsed = params
        .get("availableDecisions")
        .and_then(Value::as_array)
        .map(|decisions| {
            decisions
                .iter()
                .enumerate()
                .filter_map(|(index, decision)| {
                    approval_choice_from_available_decision(index, decision)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if parsed.is_empty() {
        defaults
            .iter()
            .copied()
            .map(AppServerApprovalChoice::new)
            .collect()
    } else {
        parsed
    }
}

fn permission_approval_options(
    params: &Value,
    defaults: &[AppServerApprovalOption],
    requested_permissions: &Value,
) -> Vec<AppServerApprovalChoice> {
    let mut options = approval_options_from_params(params, defaults);
    let has_available_decisions = params
        .get("availableDecisions")
        .and_then(Value::as_array)
        .is_some_and(|decisions| {
            decisions
                .iter()
                .any(|decision| approval_choice_from_available_decision(0, decision).is_some())
        });

    if !has_available_decisions {
        let partial_options = partial_permission_options(requested_permissions);
        if !partial_options.is_empty() {
            let insert_at = options
                .iter()
                .position(|choice| {
                    matches!(
                        choice.option,
                        AppServerApprovalOption::Decline | AppServerApprovalOption::Cancel
                    )
                })
                .unwrap_or(options.len());
            options.splice(insert_at..insert_at, partial_options);
        }
    }

    options
}

fn partial_permission_options(requested_permissions: &Value) -> Vec<AppServerApprovalChoice> {
    let units = partial_permission_units(requested_permissions);
    if units.len() < 2 {
        return Vec::new();
    }

    units
        .into_iter()
        .flat_map(|unit| {
            let turn_option = AppServerApprovalChoice::partial_permission(
                AppServerApprovalOption::Accept,
                format!("partial:{}:turn", unit.id),
                format!("Allow {} once", unit.label),
                unit.permissions.clone(),
                "turn",
            );
            let session_option = AppServerApprovalChoice::partial_permission(
                AppServerApprovalOption::AcceptForSession,
                format!("partial:{}:session", unit.id),
                format!("Allow {} for session", unit.label),
                unit.permissions,
                "session",
            );
            [turn_option, session_option]
        })
        .collect()
}

struct PartialPermissionUnit {
    id: String,
    label: String,
    permissions: Value,
}

fn partial_permission_units(requested_permissions: &Value) -> Vec<PartialPermissionUnit> {
    let mut units = Vec::new();

    if requested_permissions
        .get("network")
        .and_then(|network| network.get("enabled"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        units.push(PartialPermissionUnit {
            id: "network".to_owned(),
            label: "network access".to_owned(),
            permissions: json!({"network": {"enabled": true}}),
        });
    }

    if let Some(file_system) = requested_permissions.get("fileSystem") {
        for (field, label) in [("read", "read access"), ("write", "write access")] {
            if let Some(paths) = file_system.get(field).and_then(Value::as_array) {
                for (index, path) in paths.iter().filter_map(Value::as_str).enumerate() {
                    units.push(PartialPermissionUnit {
                        id: format!("fileSystem-{field}-{index}"),
                        label: format!("{label} to `{path}`"),
                        permissions: json!({"fileSystem": {field: [path]}}),
                    });
                }
            }
        }
    }

    units
}

fn permissions_approval_title(params: &Value, requested_permissions: &Value) -> String {
    if let Some(reason) = string_field(params, "reason")
        && !reason.trim().is_empty()
    {
        return reason;
    }

    permissions_summary(requested_permissions)
        .map(|summary| format!("Grant {summary}"))
        .unwrap_or_else(|| "Grant additional permissions".to_owned())
}

fn permissions_summary(permissions: &Value) -> Option<String> {
    let mut parts = Vec::new();

    if permissions
        .get("network")
        .and_then(|network| network.get("enabled"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        parts.push("network access".to_owned());
    }

    if let Some(file_system) = permissions.get("fileSystem") {
        if let Some(part) = permission_path_summary(file_system, "read", "read access") {
            parts.push(part);
        }
        if let Some(part) = permission_path_summary(file_system, "write", "write access") {
            parts.push(part);
        }
    }

    readable_list(parts)
}

fn permission_path_summary(file_system: &Value, field: &str, label: &str) -> Option<String> {
    let paths = file_system.get(field)?.as_array()?;
    match paths.len() {
        0 => None,
        1 => {
            let path = paths[0].as_str()?;
            Some(format!("{label} to `{path}`"))
        }
        count => Some(format!("{label} to {count} paths")),
    }
}

fn readable_list(parts: Vec<String>) -> Option<String> {
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

fn approval_option_from_id(id: &str) -> Option<AppServerApprovalOption> {
    match id {
        "accept" => Some(AppServerApprovalOption::Accept),
        "acceptForSession" => Some(AppServerApprovalOption::AcceptForSession),
        "acceptWithExecpolicyAmendment" => {
            Some(AppServerApprovalOption::AcceptWithExecpolicyAmendment)
        }
        "applyNetworkPolicyAmendment" => Some(AppServerApprovalOption::ApplyNetworkPolicyAmendment),
        "decline" => Some(AppServerApprovalOption::Decline),
        "cancel" => Some(AppServerApprovalOption::Cancel),
        _ => None,
    }
}

fn approval_choice_from_available_decision(
    index: usize,
    decision: &Value,
) -> Option<AppServerApprovalChoice> {
    if let Some(id) = decision.as_str() {
        return approval_option_from_id(id).map(AppServerApprovalChoice::new);
    }

    let object = decision.as_object()?;
    let id = object.keys().find_map(|key| approval_option_from_id(key))?;
    Some(AppServerApprovalChoice::with_available_decision(
        id,
        index,
        decision.clone(),
    ))
}

fn approval_response_result(
    approval: AppServerApprovalRequest,
    decision: AppServerApprovalDecision,
) -> Value {
    match approval.response_kind {
        AppServerApprovalResponseKind::Decision => json!({
            "decision": decision.into_app_server_value(),
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
                AppServerApprovalDecision::Raw(raw) => permission_grant_from_raw_decision(raw),
            };
            json!({
                "permissions": permissions,
                "scope": scope,
            })
        }
    }
}

fn permission_grant_from_raw_decision(raw: Value) -> (Value, &'static str) {
    let permissions = raw.get("permissions").cloned().unwrap_or_else(|| json!({}));
    let scope = match raw.get("scope").and_then(Value::as_str) {
        Some("session") => "session",
        _ => "turn",
    };
    (permissions, scope)
}

fn fallback_interactive_request_response(method: &str, _params: &Value) -> Option<Value> {
    match method {
        "currentTime/read" => Some(json!({
            "currentTimeAt": current_unix_timestamp_seconds(),
        })),
        "mcpServer/elicitation/request" => Some(json!({
            "action": "cancel",
            "content": null,
            "_meta": null,
        })),
        "item/tool/requestUserInput" | "tool/requestUserInput" => Some(json!({
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

async fn no_interactive_response(
    _request: AppServerInteractiveRequest,
) -> anyhow::Result<Option<Value>> {
    Ok(None)
}

fn app_server_request_error(method: &str) -> (i64, String) {
    match method {
        "attestation/generate" => (
            JSON_RPC_REQUEST_FAILED,
            "attestation generation is not supported by this ACP adapter".to_owned(),
        ),
        _ => (
            JSON_RPC_METHOD_NOT_FOUND,
            format!("unsupported app-server request `{method}`"),
        ),
    }
}

fn current_unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub fn history_events(thread: &AppServerThreadHistory) -> Vec<AppServerHistoryEvent> {
    history_events_for_turns(&thread.turns)
}

pub fn history_events_for_turns(turns: &[AppServerTurnHistory]) -> Vec<AppServerHistoryEvent> {
    turns
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
        "agentMessage" => agent_message_text(item)
            .and_then(|delta| {
                Some(AppServerAgentMessageDelta {
                    item_id: Some(item_id(item)?),
                    delta,
                })
            })
            .map(AppServerPromptEvent::AgentMessageDelta)
            .map(Box::new)
            .map(AppServerHistoryEvent::PromptEvent)
            .into_iter()
            .collect(),
        "reasoning" => reasoning_text(item)
            .map(AppServerPromptEvent::AgentThoughtDelta)
            .map(Box::new)
            .map(AppServerHistoryEvent::PromptEvent)
            .into_iter()
            .collect(),
        "plan" => history_plan_entries(item)
            .map(AppServerPromptEvent::PlanUpdated)
            .map(Box::new)
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
                .map(Box::new)
                .map(AppServerHistoryEvent::PromptEvent)
                .collect()
        }
        "commandExecution" | "mcpToolCall" | "collabToolCall" | "dynamicToolCall" | "webSearch"
        | "imageView" | "sleep" | "contextCompaction" | "enteredReviewMode"
        | "exitedReviewMode" => tool_history_events(item)
            .into_iter()
            .map(Box::new)
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

fn agent_message_text(item: &Value) -> Option<String> {
    string_field(item, "text").or_else(|| {
        let mut parts = Vec::new();
        collect_text_fragments(item.get("content"), &mut parts);
        let text = parts.join("\n");
        (!text.trim().is_empty()).then_some(text)
    })
}

fn reasoning_text(item: &Value) -> Option<String> {
    let mut parts = Vec::new();
    collect_text_fragments(item.get("summary"), &mut parts);
    collect_text_fragments(item.get("content"), &mut parts);
    let text = parts.join("\n");
    (!text.trim().is_empty()).then_some(text)
}

fn history_plan_entries(item: &Value) -> Option<Vec<AppServerPlanEntry>> {
    let entries = item
        .get("entries")
        .or_else(|| item.get("steps"))
        .or_else(|| item.get("plan"))
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(decode_plan_entry)
                .collect::<Vec<_>>()
        })
        .filter(|entries| !entries.is_empty());
    entries.or_else(|| {
        string_field(item, "text").map(|text| {
            vec![AppServerPlanEntry {
                content: text,
                status: AppServerPlanStatus::Completed,
            }]
        })
    })
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
    let diff = file_change_diffs(item)
        .into_iter()
        .map(|change| format!("### {}\n{}", change.path, change.diff))
        .collect::<Vec<_>>()
        .join("\n");
    (!diff.trim().is_empty()).then_some(diff)
}

fn file_change_diffs(item: &Value) -> Vec<AppServerFileDiff> {
    let Some(changes) = item.get("changes").and_then(Value::as_array) else {
        return Vec::new();
    };
    changes
        .iter()
        .filter_map(|change| {
            let path = change.get("path").and_then(Value::as_str)?;
            let diff = change.get("diff").and_then(Value::as_str)?;
            Some(AppServerFileDiff {
                path: path.to_owned(),
                diff: diff.to_owned(),
            })
        })
        .collect()
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

    let diffs = file_change_diffs(item);
    let output_delta = if diffs.is_empty() {
        final_item_output(item)
    } else {
        None
    };
    Some(AppServerToolCallUpdate {
        id,
        title: None,
        kind: None,
        status: Some(status),
        output_delta,
        diffs,
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

fn required_string(value: &Value, field: &str) -> anyhow::Result<String> {
    string_field(value, field).with_context(|| format!("missing `{field}`"))
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    string_field(value, field)
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

fn terminal_interaction_output(process_id: &str, stdin: &str) -> String {
    format!("\n[terminal input to process `{process_id}`]\n{stdin}")
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

    #[test]
    fn decode_terminal_interaction_as_tool_output_update() {
        let event = decode_prompt_event(
            "item/commandExecution/terminalInteraction",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "cmd-1",
                "processId": "proc-1",
                "stdin": "q",
            }),
            "thread-1",
            Some("turn-1"),
        )
        .expect("terminal interaction should decode")
        .expect("terminal interaction should produce an event");

        let AppServerPromptEvent::ToolCallUpdated(update) = event else {
            panic!("terminal interaction should update the command tool call");
        };

        assert_eq!(update.id, "cmd-1");
        assert_eq!(
            update.output_delta.as_deref(),
            Some("\n[terminal input to process `proc-1`]\nq")
        );
        assert!(update.raw.is_none());
    }

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
    fn permission_approval_raw_grant_preserves_partial_permissions_and_scope() {
        let response = approval_response_result(
            permissions_approval(),
            AppServerApprovalDecision::Raw(json!({
                "permissions": {
                    "fileSystem": {
                        "write": ["/repo/src"],
                    },
                },
                "scope": "session",
            })),
        );

        assert_eq!(response["scope"], "session");
        assert_eq!(
            response["permissions"],
            json!({
                "fileSystem": {
                    "write": ["/repo/src"],
                },
            })
        );
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
    fn permissions_approval_uses_available_decisions_when_present() {
        let approval = decode_approval_request(
            "item/permissions/requestApproval",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "permissions-approval",
                "startedAtMs": 123,
                "cwd": "/repo",
                "reason": "Need network",
                "permissions": {
                    "network": {"enabled": true},
                },
                "availableDecisions": ["accept", "decline"],
            }),
            "thread-1",
            Some("turn-1"),
        )
        .unwrap()
        .expect("permissions request should decode");

        assert_eq!(
            approval.options,
            vec![
                AppServerApprovalChoice::new(AppServerApprovalOption::Accept),
                AppServerApprovalChoice::new(AppServerApprovalOption::Decline)
            ]
        );
    }

    #[test]
    fn permissions_approval_adds_partial_grant_choices_when_backend_has_no_choices() {
        let approval = decode_approval_request(
            "item/permissions/requestApproval",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "permissions-approval",
                "startedAtMs": 123,
                "cwd": "/repo",
                "permissions": {
                    "network": {"enabled": true},
                    "fileSystem": {
                        "read": ["/repo"],
                        "write": ["/repo/src"],
                    },
                },
            }),
            "thread-1",
            Some("turn-1"),
        )
        .unwrap()
        .expect("permissions request should decode");

        assert_eq!(
            approval
                .options
                .iter()
                .map(|choice| (choice.id(), choice.label()))
                .collect::<Vec<_>>(),
            vec![
                ("accept", "Allow once"),
                ("acceptForSession", "Allow for session"),
                ("partial:network:turn", "Allow network access once"),
                (
                    "partial:network:session",
                    "Allow network access for session"
                ),
                (
                    "partial:fileSystem-read-0:turn",
                    "Allow read access to `/repo` once"
                ),
                (
                    "partial:fileSystem-read-0:session",
                    "Allow read access to `/repo` for session"
                ),
                (
                    "partial:fileSystem-write-0:turn",
                    "Allow write access to `/repo/src` once"
                ),
                (
                    "partial:fileSystem-write-0:session",
                    "Allow write access to `/repo/src` for session"
                ),
                ("decline", "Reject"),
                ("cancel", "Cancel"),
            ]
        );

        let partial_write = approval
            .options
            .iter()
            .find(|choice| choice.id() == "partial:fileSystem-write-0:session")
            .expect("partial write choice should exist");
        assert_eq!(
            partial_write.decision(),
            AppServerApprovalDecision::Raw(json!({
                "permissions": {
                    "fileSystem": {
                        "write": ["/repo/src"],
                    },
                },
                "scope": "session",
            }))
        );
    }

    #[test]
    fn command_approval_preserves_rich_available_decisions_in_choices() {
        let rich_decision = json!({
            "acceptWithExecpolicyAmendment": {
                "execpolicy_amendment": [
                    {"type": "exact", "argv": ["cargo", "test"]}
                ]
            }
        });
        let approval = decode_approval_request(
            "item/commandExecution/requestApproval",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "command-approval",
                "startedAtMs": 123,
                "command": "cargo test",
                "availableDecisions": [
                    "accept",
                    rich_decision.clone(),
                    "decline"
                ],
            }),
            "thread-1",
            Some("turn-1"),
        )
        .unwrap()
        .expect("command request should decode");

        assert_eq!(
            approval.options,
            vec![
                AppServerApprovalChoice::new(AppServerApprovalOption::Accept),
                AppServerApprovalChoice::with_available_decision(
                    AppServerApprovalOption::AcceptWithExecpolicyAmendment,
                    1,
                    rich_decision.clone(),
                ),
                AppServerApprovalChoice::new(AppServerApprovalOption::Decline),
            ]
        );
    }

    #[test]
    fn command_approval_raw_decision_returns_available_decision_payload() {
        let rich_decision = json!({
            "acceptWithExecpolicyAmendment": {
                "execpolicy_amendment": [
                    {"type": "exact", "argv": ["cargo", "test"]}
                ]
            }
        });
        let response = approval_response_result(
            AppServerApprovalRequest {
                item_id: "command-approval".to_owned(),
                title: "Run `cargo test`".to_owned(),
                kind: AppServerToolKind::Execute,
                raw: json!({}),
                options: vec![],
                response_kind: AppServerApprovalResponseKind::Decision,
            },
            AppServerApprovalDecision::Raw(rich_decision.clone()),
        );

        assert_eq!(response["decision"], rich_decision);
    }

    #[test]
    fn permissions_approval_prefers_app_server_reason_as_title() {
        let approval = decode_approval_request(
            "item/permissions/requestApproval",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "permissions-approval",
                "startedAtMs": 123,
                "cwd": "/repo",
                "reason": "Need network and write access",
                "permissions": {
                    "network": {"enabled": true},
                    "fileSystem": {"write": ["/repo/src"]},
                },
            }),
            "thread-1",
            Some("turn-1"),
        )
        .unwrap()
        .expect("permissions request should decode");

        assert_eq!(approval.title, "Need network and write access");
    }

    #[test]
    fn permissions_approval_summarizes_requested_permissions_without_reason() {
        let approval = decode_approval_request(
            "item/permissions/requestApproval",
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "permissions-approval",
                "startedAtMs": 123,
                "cwd": "/repo",
                "permissions": {
                    "network": {"enabled": true},
                    "fileSystem": {
                        "read": ["/repo"],
                        "write": ["/repo/src", "/repo/tests"],
                    },
                },
            }),
            "thread-1",
            Some("turn-1"),
        )
        .unwrap()
        .expect("permissions request should decode");

        assert_eq!(
            approval.title,
            "Grant network access, read access to `/repo`, and write access to 2 paths"
        );
    }

    #[test]
    fn fallback_interactive_requests_cancel_or_fail_without_blocking_app_server() {
        let current_time_response =
            fallback_interactive_request_response("currentTime/read", &json!({})).unwrap();
        assert!(
            current_time_response["currentTimeAt"]
                .as_u64()
                .is_some_and(|timestamp| timestamp > 1_700_000_000)
        );
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
            fallback_interactive_request_response("tool/requestUserInput", &json!({})).unwrap(),
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
        assert_eq!(
            app_server_request_error("attestation/generate"),
            (
                JSON_RPC_REQUEST_FAILED,
                "attestation generation is not supported by this ACP adapter".to_owned()
            )
        );
    }

    #[test]
    fn app_server_message_receive_recovers_from_lagged_broadcasts() {
        let lagged = receive_app_server_message(Err(broadcast::error::RecvError::Lagged(42)))
            .expect("lagged notifications should not fail the receiver");
        assert!(lagged.is_none());

        let received = receive_app_server_message(Ok(AppServerMessage::Notification {
            method: "turn/completed".to_owned(),
            params: json!({}),
        }))
        .expect("valid messages should decode")
        .expect("valid messages should be present");
        assert!(matches!(
            received,
            AppServerMessage::Notification { ref method, .. } if method == "turn/completed"
        ));
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
