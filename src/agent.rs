use std::{collections::HashMap, sync::Arc};

use agent_client_protocol::schema::{
    AgentCapabilities, CancelNotification, CloseSessionRequest, CloseSessionResponse, ContentBlock,
    ContentChunk, ForkSessionRequest, ForkSessionResponse, InitializeRequest, InitializeResponse,
    ListSessionsRequest, ListSessionsResponse, NewSessionRequest, NewSessionResponse,
    PromptCapabilities, PromptRequest, PromptResponse, ProtocolVersion, ResumeSessionRequest,
    ResumeSessionResponse, SessionCapabilities, SessionCloseCapabilities, SessionForkCapabilities,
    SessionId, SessionInfo, SessionListCapabilities, SessionNotification,
    SessionResumeCapabilities, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{
    Agent, ByteStreams, Client, ConnectTo, ConnectionTo, Error, on_receive_notification,
    on_receive_request,
};
use tokio::sync::{Mutex, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::app_server::{AppServerClient, AppServerPromptCompletion, AppServerPromptEvent};

#[derive(Clone)]
pub struct CodexAcpAgent {
    app_server: Arc<Mutex<AppServerClient>>,
    active_prompts: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
}

impl CodexAcpAgent {
    pub fn new(app_server: AppServerClient) -> Self {
        Self {
            app_server: Arc::new(Mutex::new(app_server)),
            active_prompts: Arc::new(Mutex::new(HashMap::new())),
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

        Agent
            .builder()
            .name("brokk-codex-acp")
            .on_receive_request(
                {
                    let agent = agent.clone();
                    async move |request: InitializeRequest, responder, _cx| {
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
                        cx.spawn(async move {
                            responder.respond_with_result(agent.new_session(request).await)
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
                        cx.spawn(async move {
                            responder.respond_with_result(agent.fork_session(request).await)
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
                        cx.spawn(async move {
                            responder.respond_with_result(agent.resume_session(request).await)
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
            .await
    }

    async fn initialize(&self, request: InitializeRequest) -> Result<InitializeResponse, Error> {
        let _requested_version = request.protocol_version;

        Ok(InitializeResponse::new(ProtocolVersion::V1)
            .agent_capabilities(Self::capabilities())
            .auth_methods(vec![]))
    }

    async fn new_session(&self, request: NewSessionRequest) -> Result<NewSessionResponse, Error> {
        let cwd = request.cwd.to_string_lossy().into_owned();
        let response = self
            .app_server
            .lock()
            .await
            .thread_start(cwd)
            .await
            .map_err(acp_internal_error)?;

        Ok(NewSessionResponse::new(SessionId::new(response.thread.id)))
    }

    async fn fork_session(
        &self,
        request: ForkSessionRequest,
    ) -> Result<ForkSessionResponse, Error> {
        let source_thread_id = request.session_id.0.to_string();
        let cwd = request.cwd.to_string_lossy().into_owned();
        let response = self
            .app_server
            .lock()
            .await
            .thread_fork(source_thread_id, cwd)
            .await
            .map_err(acp_internal_error)?;

        Ok(ForkSessionResponse::new(SessionId::new(response.thread.id)))
    }

    async fn resume_session(
        &self,
        request: ResumeSessionRequest,
    ) -> Result<ResumeSessionResponse, Error> {
        let thread_id = request.session_id.0.to_string();
        let cwd = request.cwd.to_string_lossy().into_owned();
        self.app_server
            .lock()
            .await
            .thread_resume(thread_id, cwd)
            .await
            .map_err(acp_internal_error)?;

        Ok(ResumeSessionResponse::new())
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
            .filter_map(|thread| {
                let cwd = thread.cwd?;
                Some(SessionInfo::new(SessionId::new(thread.id), cwd).title(thread.name))
            })
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

    async fn prompt(
        &self,
        request: PromptRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<PromptResponse, Error> {
        let text = prompt_text(request.prompt)?;
        let session_id = request.session_id.clone();
        let thread_id = request.session_id.0.to_string();
        let (cancel_tx, cancel_rx) = oneshot::channel();
        if self
            .active_prompts
            .lock()
            .await
            .insert(thread_id.clone(), cancel_tx)
            .is_some()
        {
            return Err(Error::invalid_request().data("session already has an active prompt turn"));
        }

        let completion = self
            .app_server
            .lock()
            .await
            .turn_start_text_until_complete(thread_id.clone(), text, Some(cancel_rx), |event| {
                match event {
                    AppServerPromptEvent::AgentMessageDelta(delta) => {
                        cx.send_notification(SessionNotification::new(
                            session_id.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new(delta)),
                            )),
                        ))
                        .map_err(|error| {
                            anyhow::anyhow!("failed to send ACP session update: {error}")
                        })?;
                    }
                }
                Ok(())
            })
            .await
            .map_err(acp_internal_error);

        self.active_prompts.lock().await.remove(&thread_id);

        let stop_reason = match completion? {
            AppServerPromptCompletion::EndTurn => StopReason::EndTurn,
            AppServerPromptCompletion::Cancelled => StopReason::Cancelled,
        };

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

    fn capabilities() -> AgentCapabilities {
        AgentCapabilities::new()
            .load_session(false)
            .prompt_capabilities(PromptCapabilities::new())
            .session_capabilities(
                SessionCapabilities::new()
                    .list(SessionListCapabilities::new())
                    .resume(SessionResumeCapabilities::new())
                    .close(SessionCloseCapabilities::new())
                    .fork(SessionForkCapabilities::new()),
            )
    }
}

fn acp_internal_error(error: anyhow::Error) -> Error {
    Error::internal_error().data(error.to_string())
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
