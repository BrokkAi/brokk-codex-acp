# Codex ACP Server Plan

## Goal

Build a high-fidelity Agent Client Protocol server for Codex that uses Codex's
official `codex app-server` as the execution backend instead of reimplementing
Codex internals directly.

The adapter should expose Codex to ACP clients while preserving the behavior
users expect from the Codex CLI and desktop/editor integrations:

- Rich turn streaming.
- Tool call rendering and approval routing.
- Shell command output and terminal interaction.
- Slash commands where they map to real backend capabilities.
- Skills discovery, invocation, and enable/disable management.
- Session lifecycle, including load, resume, list, close, and delete. Forking is
  useful for Codex, but is not part of the stable ACP v1 surface in the local
  upstream snapshot and must be treated as an extension/RFD-backed feature.
- Models, reasoning effort, permission profiles, MCP, apps, plugins, hooks, and
  other catalog-backed surfaces where ACP can represent them.

## Current Problem

Existing Codex ACP adapters are limited because they treat Codex as a low-level
thread engine and rebuild the interactive layer beside it.

That leads to several gaps:

- Slash commands are hardcoded and incomplete.
- Skills are not first-class.
- New app-server surfaces such as apps, plugins, hooks, goals, and permission
  profiles are missing or drift quickly.
- Thread/session lifecycle behavior has to be recreated manually.
- The adapter depends on unstable Codex crates and duplicated UI logic.

The core issue is architectural: most Codex slash commands are not model
operations. They are dispatch actions implemented by the TUI or app UI and
often map to app-server APIs, local UI state, or catalog reads. A good ACP
adapter should translate those capabilities, not copy the TUI.

## Architecture

Use a thin bridge:

```text
ACP client
  <-> brokk-codex-acp
      <-> codex app-server --stdio
          <-> Codex thread manager, tools, skills, plugins, MCP, apps
```

The ACP server owns:

- ACP transport and method handling.
- App-server process lifecycle.
- JSON-RPC client for app-server.
- Mapping from ACP session IDs to Codex thread IDs.
- Event translation from app-server notifications to ACP session updates.
- Slash command routing.
- Client capability negotiation and graceful degradation.

The Codex app-server owns:

- Thread creation, resume, fork, archive, delete, and storage.
- Turn execution and event generation.
- Compaction.
- Review mode.
- Skills loading and config.
- App/plugin/MCP catalogs.
- Model and permission catalogs.
- Tool execution and approval semantics.

## App-Server Transport

Start by spawning:

```shell
codex app-server --stdio
```

Then perform the app-server handshake:

```json
{
  "method": "initialize",
  "id": 1,
  "params": {
    "clientInfo": {
      "name": "brokk_codex_acp",
      "title": "Brokk Codex ACP",
      "version": "0.1.0"
    },
    "capabilities": {
      "experimentalApi": true,
      "mcpServerOpenaiFormElicitation": true
    }
  }
}
```

Follow with the `initialized` notification.

The bridge should keep one app-server process per ACP server process. Multiple
ACP sessions can share that app-server process and map to separate app-server
threads.

## Implemented Baseline

The current repository has the first working ACP/app-server bridge in place:

- CLI commands:
  - `probe` starts `codex app-server --stdio`, initializes it, and reports the
    returned runtime metadata.
  - `serve` starts `codex app-server --stdio`, initializes it, and serves ACP
    over stdio.
- ACP protocol handling:
  - `initialize`
  - `session/new`
  - `session/load` with initial history replay through `thread/read` followed
    by `thread/resume`.
  - `session/resume`
  - `session/list`, including preservation of app-server thread preview,
    status, recency, model-provider, agent, and parent-thread metadata under
    `_meta.brokk_codex_acp`.
  - `session/close`
  - `session/fork` through the Rust crate extension, not stable ACP v1.
  - `session/delete` through the Rust crate's `unstable_session_delete` feature,
    matching the local stable ACP v1 documentation surface.
  - `session/set_config_option` for model, reasoning-effort, service-tier,
    approval-policy, collaboration-mode, and permission-profile selectors.
  - `session/prompt`
  - `session/cancel`
- App-server mappings:
  - `session/new` -> `thread/start`
  - `session/load` -> `thread/read`/`thread/resume` history replay
  - `session/resume` -> `thread/resume`
  - `session/list` -> `thread/list`
  - `session/close` -> `thread/unsubscribe`
  - `session/fork` -> `thread/fork` extension
  - `session/delete` -> `thread/delete`
  - `session/set_config_option(model)` -> `thread/settings/update.model`
  - `session/set_config_option(reasoning_effort)` ->
    `thread/settings/update.effort`
  - `session/set_config_option(service_tier)` ->
    `thread/settings/update.serviceTier`
  - `session/set_config_option(approval_policy)` ->
    `thread/settings/update.approvalPolicy`
  - `session/set_config_option(collaboration_mode)` ->
    `thread/settings/update.collaborationMode`
  - `session/set_config_option(permission_profile)` ->
    `thread/settings/update.permissions`
  - `session/prompt` -> `turn/start`
  - `session/prompt` text beginning with `!` -> `thread/shellCommand`
  - `session/prompt` `/memory enable|disable|reset` -> `thread/memoryMode/set`
    or `memory/reset`
  - `session/cancel` -> `turn/interrupt`
- Catalog/config projection:
  - `model/list` -> ACP `model`, `reasoning_effort`, and `service_tier`
    session config options.
  - `permissionProfile/list` -> ACP `permission_profile` session config
    option.
  - `collaborationMode/list` -> ACP `collaboration_mode` session config option.
  - `thread/start`, `thread/resume`, and `thread/fork` response settings seed
    current config option values where app-server provides them.
- Event translation:
  - `item/agentMessage/delta` -> ACP agent message chunks
  - `item/reasoning/summaryTextDelta`, `item/reasoning/summaryPartAdded`, and
    `item/reasoning/textDelta` -> ACP thought chunks with readable summary
    section boundaries
  - `item/started` and `item/completed` for command, file-change, MCP,
    collaboration, web-search, review, sleep, and compaction items -> ACP tool
    call lifecycle updates
  - `item/commandExecution/outputDelta` and
    `item/commandExecution/terminalInteraction` -> incremental ACP tool call
    content
  - `turn/diff/updated` -> ACP edit tool call content containing the current
    unified diff snapshot
  - `turn/plan/updated` -> ACP plan updates
  - `thread/tokenUsage/updated` -> ACP usage updates through the unstable
    `unstable_session_usage` crate feature
  - `turn/completed` -> ACP prompt response completion
  - `model/safetyBuffering/updated` and `model/verification` -> ACP
    agent-message diagnostics for safety buffering and additional account
    verification requirements.
  - `turn/moderationMetadata` -> ACP agent-message diagnostics for backend
    moderation metadata intended for client-side presentation.
  - `mcpServer/startupStatus/updated` -> ACP agent-message diagnostics for
    thread-scoped and app-scoped MCP startup state and failures.
  - `configWarning` -> ACP agent-message diagnostics for global app-server
    configuration warnings, published to every known session.
  - `windowsSandbox/setupCompleted` -> ACP agent-message diagnostics for
    app-scoped Windows sandbox setup results, published to every known session.
  - `account/login/completed`, `account/updated`,
    `account/rateLimits/updated`, and `mcpServer/oauthLogin/completed` -> ACP
    agent-message diagnostics for app-scoped auth, rate-limit, and MCP OAuth
    state changes.
  - `fuzzyFileSearch/sessionUpdated` and
    `fuzzyFileSearch/sessionCompleted` -> ACP agent-message diagnostics for
    fuzzy file search progress and completion.
  - `thread/realtime/started`, `thread/realtime/sdp`,
    `thread/realtime/itemAdded`, `thread/realtime/transcript/delta`,
    `thread/realtime/transcript/done`, `thread/realtime/outputAudio/delta`,
    `thread/realtime/error`, and `thread/realtime/closed` -> ACP
    agent-message diagnostics for realtime session lifecycle, SDP, raw item,
    transcript text, audio metadata, and transport events.
- Server-initiated request handling:
  - `currentTime/read` -> JSON-RPC response with the adapter host's current
    Unix timestamp in seconds.
  - `mcpServer/elicitation/request` -> ACP `elicitation/create` for supported
    form and URL elicitations, with explicit cancel fallback responses when the
    ACP client cannot answer.
  - `tool/requestUserInput` and `item/tool/requestUserInput` -> ACP
    `elicitation/create` form requests, with explicit empty-answer fallback
    responses when the ACP client cannot answer.
  - `item/tool/call` -> custom ACP extension request
    `_brokk_codex_acp/dynamic_tool_call`, with explicit failure fallback
    response when the ACP client cannot answer.
  - `attestation/generate` -> explicit JSON-RPC failure when app-server asks
    unexpectedly; the adapter does not advertise or provide attestation tokens.
  - Unsupported server-initiated app-server requests receive a JSON-RPC
    method-not-found error instead of being ignored, so the backend is not left
    waiting indefinitely.
- Approval routing:
  - `item/commandExecution/requestApproval` and
    `item/fileChange/requestApproval` -> ACP `session/request_permission`
    requests, then app-server JSON-RPC responses with the selected decision.
    Rich command `availableDecisions` such as exec-policy and network-policy
    amendments are exposed as ACP permission options and answered with the
    original app-server decision payload when selected.
  - `item/permissions/requestApproval` -> ACP `session/request_permission`,
    then an app-server JSON-RPC response containing `permissions` and `scope`.
    Fine-grained partial grants are exposed as ACP permission options for
    individual requested network and filesystem units when app-server has not
    supplied an exact `availableDecisions` list. The ACP tool-call content
    includes a human-readable summary of the requested reason, environment,
    working directory, and network/filesystem access.
  - `item/autoApprovalReview/started` and
    `item/autoApprovalReview/completed` -> ACP agent-message diagnostics for
    app-server automatic approval review progress and outcomes.
  - The adapter maps `accept`, `acceptForSession`, `decline`, and `cancel` to
    ACP permission options and preserves app-server's blocking request
    semantics while awaiting the ACP client.
- Slash commands:
  - built-in `account`, `archive`, `apps`, `compact`, `config`,
    `config-requirements`, `delete`, `feature`, `features`, `fork`, `goal`,
    `hooks`, `init`, `kill`, `login`, `login-cancel`, `logout`,
    `marketplace-add`, `marketplace-remove`, `memory`, `mcp`, `mcp-reload`,
    `model`, `model-provider`, `new`, `permissions`, `plan`, `plugins`, `ps`,
    `rate-limits`, `rename`, `resume`, `review`, `rollback`, `skill-roots`,
    `status`, `stop`, `unarchive`, `usage`, and `workspace-messages` commands
    are published through ACP
    `available_commands_update` alongside enabled skills.
  - `/archive` is intercepted by the adapter, mapped to `thread/archive`, and
    reflected to ACP clients through `session_info_update._meta`.
  - `/unarchive` is intercepted by the adapter, mapped to `thread/unarchive`,
    and reflected to ACP clients through `session_info_update._meta`.
  - `/compact` is intercepted by the adapter, mapped to
    `thread/compact/start`, and streamed through the normal ACP turn update
    projection. Because app-server returns `{}` for the start request, the
    adapter waits for `turn/started` to learn the active turn id.
  - `/config [cwd]` is intercepted by the adapter, mapped to `config/read`,
    and reflected as a short ACP agent-message summary of effective settings.
  - `/config-requirements` is intercepted by the adapter, mapped to
    `configRequirements/read`, and reflected as a short ACP agent-message
    summary of managed constraints.
  - `/fork` is intercepted by the adapter, mapped to `thread/fork`, initializes
    adapter state for the returned thread/session id, and reports the new
    session id as an ACP agent-message summary.
  - `/new` is intercepted by the adapter, mapped to `thread/start` in the
    current session cwd, initializes adapter state for the returned
    thread/session id, and reports the new session id as an ACP agent-message
    summary.
  - `/features` is intercepted by the adapter, mapped to
    `experimentalFeature/list` for the current thread, and reflected as a short
    ACP agent-message summary.
  - `/account` is intercepted by the adapter, mapped to `account/read`, and
    reflected as a short ACP agent-message summary.
  - `/login [chatgpt|device]`, `/login-cancel <loginId>`, and `/logout` are
    intercepted by the adapter, mapped to `account/login/start`,
    `account/login/cancel`, and `account/logout`, and reflected as short ACP
    agent-message summaries. API-key login is intentionally not accepted as a
    slash command because ACP prompt text can be retained in visible history.
  - `/rate-limits` is intercepted by the adapter, mapped to
    `account/rateLimits/read`, and reflected as a short ACP agent-message
    summary.
  - `/usage` is intercepted by the adapter, mapped to `account/usage/read`, and
    reflected as a short ACP agent-message summary.
  - `/workspace-messages` is intercepted by the adapter, mapped to
    `account/workspaceMessages/read`, and reflected as a short ACP
    agent-message summary.
  - `/mcp-reload` is intercepted by the adapter, mapped to
    `config/mcpServer/reload`, then followed by `mcpServerStatus/list` and
    reflected as a short ACP agent-message summary.
  - `/model-provider` is intercepted by the adapter, mapped to
    `modelProvider/capabilities/read`, and reflected as a short ACP
    agent-message summary.
  - `/feature <name> enable|disable` is intercepted by the adapter, mapped to
    `experimentalFeature/enablement/set`, and reflected as a short ACP
    agent-message summary.
  - `/marketplace-add <source> [ref] [sparsePathsCsv]` and
    `/marketplace-remove <name>` are intercepted by the adapter, mapped to
    `marketplace/add` and `marketplace/remove`, and reflected as short ACP
    agent-message summaries.
  - `/goal`, `/goal get`, `/goal clear`, and `/goal <objective>` are
    intercepted by the adapter, mapped to `thread/goal/get`,
    `thread/goal/clear`, and `thread/goal/set`, then reflected through
    `session_info_update._meta`.
  - `/init` is intercepted by the adapter, transformed into the AGENTS.md
    generation prompt, and streamed through the normal ACP turn update
    projection.
  - `/rename <title>` is intercepted by the adapter, mapped to
    `thread/name/set`, and reflected to ACP clients through
    `session_info_update`.
  - `/resume <id-or-name>` is intercepted by the adapter, resolves an exact
    thread id/name/preview match from `thread/list` for the current cwd when
    possible, maps to `thread/resume`, initializes adapter state for the resumed
    thread/session id, and reports the resumed session id as an ACP
    agent-message summary.
  - `/review` is intercepted by the adapter, mapped to `review/start`, and
    streamed through the normal ACP turn update projection.
  - `/apps`, `/plugins`, `/mcp`, `/hooks`, and `/status` are intercepted by the
    adapter, mapped to app-server catalog/status endpoints, and reflected as
    short ACP agent-message summaries.
  - `/ps` and `/stop` are intercepted by the adapter, mapped to
    `thread/backgroundTerminals/list` and `thread/backgroundTerminals/clean`,
    and reflected as short ACP agent-message summaries.
  - `/kill <process-id>` is intercepted by the adapter, mapped to
    `thread/backgroundTerminals/terminate`, and reflected as a short ACP
    agent-message summary.
  - `/memory enable`, `/memory disable`, and `/memory reset` are intercepted by
    the adapter, mapped to `thread/memoryMode/set` or `memory/reset`, and
    reflected as short ACP agent-message summaries.
  - `/model` and `/permissions` are intercepted by the adapter, refresh
    app-server-backed config catalogs, publish ACP `config_option_update`, and
    send a short ACP agent-message summary.
  - `!<command>` input is intercepted by the adapter, mapped to
    `thread/shellCommand`, and streamed through the normal ACP turn update
    projection.
  - Unknown leading slash commands return an explicit ACP error instead of
    being forwarded to the model; `/skill ...` remains a supported skill
    invocation fallback.
  - `thread/archived` app-server notifications are projected to ACP
    `session_info_update._meta`.
  - `thread/name/updated` app-server notifications are projected to ACP
    `session_info_update`.
  - `thread/status/changed` app-server notifications are projected to ACP
    `session_info_update._meta`, preserving the full app-server status payload.
  - `thread/deleted` and `thread/closed` app-server notifications are projected
    to ACP `session_info_update._meta` as adapter lifecycle metadata.
  - `warning`, `error`, `model/rerouted`, and
    `model/safetyBuffering/updated` app-server notifications are projected to
    ACP agent-message chunks for user-visible diagnostics.
  - `thread/goal/updated` and `thread/goal/cleared` app-server notifications are
    projected to ACP `session_info_update._meta`.

This baseline supports text, resource-link, embedded resource, and image prompt
blocks as input, and advertises stable ACP v1 `sessionCapabilities.list`,
`.resume`, `.close`, and `.delete`. Embedded resource prompt blocks are passed
through app-server `additionalContext`. It also advertises the Rust crate's
unstable session fork and elicitation extensions. Session config options are
populated from app-server models, collaboration modes, permission profiles, and
thread settings. Dynamic tool callbacks route through a custom ACP extension
request with app-server fallback semantics, and file-change items emit
structured ACP diff content when app-server provides per-file paths, including
live file-change patch updates. Text prompts that invoke apps or installed
plugins with `$app-slug` or `@plugin-name` are enriched with app-server
`mention` input items when the app/plugin catalogs can resolve the target, with
plain-text fallback otherwise. Native realtime audio playback, audio prompt
blocks, and the remaining history
fidelity edges remain planned work. ACP terminal content is projected through
app-server command output and terminal interaction events today; true
`terminal/create` embedding should only be added if app-server exposes a
client-terminal handoff for commands it does not execute itself.

## Immediate Roadmap

The next work should stay focused on making normal Codex turns feel real in an
ACP client before expanding into catalogs and slash commands.

## Release Strategy

Match Anvil's desktop/server platform coverage while excluding Android until
Codex can compile there.

Required release artifacts:

| Target | Runner | Artifact |
| --- | --- | --- |
| `x86_64-unknown-linux-gnu` | `ubuntu-latest` | `brokk-codex-acp-<tag>-x86_64-unknown-linux-gnu.zip` |
| `aarch64-unknown-linux-gnu` | `ubuntu-24.04-arm` | `brokk-codex-acp-<tag>-aarch64-unknown-linux-gnu.zip` |
| `x86_64-pc-windows-msvc` | `windows-latest` | `brokk-codex-acp-<tag>-x86_64-pc-windows-msvc.zip` |
| `universal-apple-darwin` | `macos-latest` | `brokk-codex-acp-<tag>-universal-apple-darwin.zip` |

Release rules:

- Trigger GitHub releases from `v*.*.*` tags.
- Verify the tag version matches `Cargo.toml`.
- Build with `cargo build --release --locked`.
- Include `README.md` and `LICENSE.md` in each archive.
- Publish `.sha256` files next to every archive.
- Generate GitHub release notes automatically.
- Keep `workflow_dispatch` available for dry artifact builds without creating a
  release.

Crates.io publishing:

- Publish `brokk-codex-acp` from the separate `Publish crate` workflow.
- Use crates.io trusted publishing via GitHub OIDC instead of a long-lived
  registry token.
- Require the `release` GitHub environment for the publish job.
- On tag pushes, verify the tag version matches `Cargo.toml`, run
  `cargo publish --dry-run -p brokk-codex-acp --locked`, then publish.
- On manual workflow runs, dry-run by default and only publish when the
  `publish` input is explicitly set.
- Configure the crates.io trusted publisher before the first real publish.

Android:

- Do not make Android a required release target yet.
- Revisit only if Codex gains Android support or the adapter stops depending on
  an installed Codex runtime.

### Milestone A: Complete Turn Streaming

Goal: a normal prompt should render the same major events an app-server client
would see.

Tasks:

- [x] Add a typed app-server notification dispatcher instead of handling only
  prompt-local `item/agentMessage/delta` and `turn/completed`.
- [x] Track the active prompt turn by `threadId` and `turnId`.
- [x] Track active tool output by app-server `itemId` during a prompt.
- [x] Map command execution, tool calls, reasoning, file changes, and usage updates
  into ACP updates.
- [x] Add tests that feed fake app-server notifications and assert adapter
  events.
- [x] Decode `skills/changed` and `thread/settings/updated` notifications
  observed during an active prompt.
- [x] Dispatch app-server stdout in a background reader so responses and
  notifications are routed independently of active requests.
- [x] Add end-to-end ACP client tests that assert serialized `session/update`
  notifications.
- [x] Route Codex bang shell commands through app-server `thread/shellCommand`
  instead of model prompts.
- [x] Add approval request bridging for command and file-change approvals.
- [x] Add approval request bridging for permission-profile requests, granting
  the full requested profile for a turn/session or rejecting it.
- [x] Add non-blocking fallback responses for current-time reads, MCP
  elicitation, dynamic tool requests, and `request_user_input` when no
  ACP-compatible UI is available, plus explicit JSON-RPC errors for unsupported
  app-server requests.
- [x] Add rich ACP UI bridging for MCP elicitation.
- [x] Add rich ACP UI bridging for request-user-input requests.
- [x] Add rich ACP UI bridging for dynamic tool requests through a custom ACP
  extension request with fallback.
- [x] Keep ACP terminal embedding gated on a future app-server client-terminal
  handoff; project the current app-server-owned command output and terminal
  interaction events through ACP tool updates.

Acceptance criteria:

- [x] Agent text streams as it does today.
- [x] Reasoning deltas appear as ACP thought chunks when supported.
- [x] Shell commands appear as ACP tool calls.
- [x] Shell output streams incrementally.
- [x] File changes appear as tool call updates or diff content.
- [x] Prompt completion returns the correct `StopReason`.
- [x] Command and file-change approval requests route through ACP
  `session/request_permission` instead of blocking or being ignored.
- [x] Permission-profile approvals route through ACP `session/request_permission`
  and respond to app-server with `permissions`/`scope`.
- [x] Current-time reads, MCP elicitation, dynamic tool, and
  `request_user_input` requests receive explicit responses instead of blocking
  app-server.
- [x] Unknown server-initiated app-server requests receive explicit JSON-RPC
  errors instead of blocking app-server.
- [x] MCP elicitation routes through ACP `elicitation/create` when the client
  can answer it.
- [x] Request-user-input requests route through ACP `elicitation/create` when
  the client can answer them.
- [x] Dynamic tool requests route through a rich ACP request surface when one
  is available.
- [x] Terminal work is documented as waiting on app-server handoff events;
  current command execution and terminal interactions continue to stream
  through app-server-owned tool events.

### Milestone B: Skills Catalog and Invocation

Goal: skills should be discoverable and invokable, not just passed through as
unstructured text.

Tasks:

- [x] Add app-server `skills/list` request support in the adapter.
- [x] Refresh skills on `session/new`, `session/load`, `session/resume`, and
  extension `session/fork`.
- [x] Refresh skills on `skills/changed` notifications.
- [x] Cache skills by cwd.
- [x] Invalidate the skill cache on `skills/changed` notifications.
- [x] Publish enabled skills through ACP available commands.
- [x] Confirm ACP v1 has no skill mention publication surface; keep skills
  exposed through available commands and config options until ACP adds one.
- [x] Convert `$skill-name` and `/skill skill-name` input into app-server
  `UserInput::Skill` when the skill path is known.
- [x] Fall back to plain text when a skill cannot be resolved.
- [x] Add app-server `skills/config/write` request support.
- [x] Expose skill enable/disable through ACP session config options.
- [x] Support process-local extra skill roots through `skills/extraRoots/set`.

Acceptance criteria:

- [x] A client can discover available skills for a session cwd through an
  ACP-supported projection surface, initially `available_commands_update`.
- [x] `$skill-name do work` reaches Codex with structured skill metadata when
  possible.
- [x] Disabled skills disappear from the published list after refresh.
- [x] Unknown skills produce a clear error or clean text fallback.

### Milestone C: Slash Command Router

Goal: supported slash commands should route to real app-server APIs or explicit
client behavior, not model prompts.

Tasks:

- [x] Add an initial parser for leading slash commands, currently `/account`,
  `/archive`, `/apps`, `/compact`, `/config`, `/config-requirements`,
  `/delete`, `/feature`, `/features`, `/fork`, `/goal`, `/hooks`, `/init`,
  `/kill`, `/login`, `/login-cancel`, `/logout`, `/marketplace-add`,
  `/marketplace-remove`, `/memory`, `/mcp`, `/mcp-reload`, `/model`,
  `/model-provider`, `/new`, `/permissions`, `/plan`, `/plugins`, `/ps`,
  `/rate-limits`, `/rename`, `/resume`, `/review`, `/rollback`,
  `/skill-roots`, `/status`, `/stop`, `/unarchive`, `/usage`, and
  `/workspace-messages`.
- [x] Build the full command registry with aliases, availability, required
  active turn state, and handler metadata.
- [x] Publish adapter-owned ACP available commands plus skills.
- Implement backend commands first: `/new`, `/resume`, `/review`,
  `/compact`, `/rename`, `/model`, `/permissions`, `/mcp`, `/apps`,
  `/plugins`, `/hooks`, and `/status`. Implemented so far: `/account`,
  `/archive`, `/apps`, `/compact`, `/config`, `/config-requirements`,
  `/delete`, `/feature`, `/features`, `/fork`, `/goal`, `/hooks`, `/init`,
  `/kill`, `/login`, `/login-cancel`, `/logout`, `/marketplace-add`,
  `/marketplace-remove`, `/memory`, `/mcp`, `/mcp-reload`, `/model`,
  `/model-provider`, `/new`, `/permissions`, `/plan`, `/plugins`, `/ps`,
  `/rate-limits`, `/rename`, `/resume`, `/review`, `/rollback`,
  `/skill-roots`, `/status`, `/stop`, `/unarchive`, `/usage`, and
  `/workspace-messages`. `/fork` is implemented only as an extension command
  backed by Codex `thread/fork`, not as required ACP v1 behavior.
- [x] Return explicit unsupported-command responses for slash commands that are
  not currently handled by the adapter. `/skill ...` remains a supported
  non-builtin fallback.
- [x] Add fake app-server coverage for `thread/archive`, `thread/unarchive`,
  `thread/compact/start`, `thread/goal/*`, `thread/name/set`, and
  `review/start` plus unit coverage for `/archive`, `/compact`, `/goal`,
  `/rename`, `/review`, and `/unarchive` parsing/advertisement.
- [x] Add fake app-server coverage for `app/list`, `plugin/list`,
  `plugin/installed`, `mcpServerStatus/list`, `hooks/list`, and
  `thread/loaded/list` plus unit coverage for `/apps`, `/plugins`, `/mcp`,
  `/hooks`, and `/status` parsing/advertisement.
- [x] Add unit coverage for `/model` and `/permissions` parsing/advertisement;
  both reuse the existing fake app-server catalog coverage for model and
  permission-profile config option refresh.
- [x] Add serialized fake app-server coverage for `/plan`, mapped to
  `thread/settings/update.collaborationMode`, plus unit coverage for parsing and
  advertisement.
- [x] Add unit coverage for `/fork` parsing/advertisement; it reuses the
  existing fake app-server `thread/fork` coverage for the backend call.
- [x] Add unit coverage for `/new` parsing/advertisement; it reuses the
  existing fake app-server `thread/start` coverage for the backend call.
- [x] Add fake app-server coverage for `thread/resume` plus unit coverage for
  `/resume` parsing/advertisement.
- [x] Add unit coverage for `/init` parsing/advertisement and the generated
  AGENTS.md prompt.
- [x] Add unit coverage proving unknown slash commands return explicit errors
  while `/skill ...` remains available for skill invocation fallback.
- [x] Add fake app-server coverage for `thread/backgroundTerminals/list` and
  `thread/backgroundTerminals/clean` plus unit coverage for `/ps` and `/stop`
  parsing/advertisement.
- [x] Add fake app-server coverage for `thread/rollback` plus unit coverage for
  `/rollback` parsing/advertisement. ACP v1 has no transcript deletion update,
  so the command updates app-server state and publishes a status message instead
  of trying to visually remove prior history.
- [x] Add fake app-server coverage for
  `thread/backgroundTerminals/terminate` plus unit coverage for `/kill`
  parsing/advertisement.
- [x] Add serialized ACP coverage proving `/rename` emits
  `session_info_update` and does not call `turn/start`.
- [x] Add serialized ACP coverage proving `/archive` emits
  `session_info_update._meta` and does not call `turn/start`.
- [x] Add serialized ACP coverage proving `/goal` emits
  `session_info_update._meta` and does not call `turn/start`.
- [x] Add fake app-server tests for each remaining backend command mapping.

Acceptance criteria:

- [x] `/fork`, when the extension is enabled, creates a new session via
  `thread/fork`.
- [x] `/review` calls `review/start`.
- [x] `/compact` calls the app-server compaction API when available.
- [x] `/model` and `/permissions` expose pickers/config updates rather than
  sending text to the model.
- [x] `/apps`, `/plugins`, `/mcp`, `/hooks`, and `/status` call app-server
  catalog/status endpoints instead of becoming model prompts.
- [x] Unknown commands never silently become prompts unless explicitly
  configured.

### Milestone D: Session History and Replay

Goal: resume, load, and fork should be useful in clients that need transcript
hydration.

Tasks:

- [x] Add `thread/read` support.
- [x] Implement `session/load` as the stable ACP v1 history-replay path.
- [x] Keep `session/resume` as a no-replay reconnect path, as required by ACP
  v1.
- [x] Convert stored user messages, agent messages, reasoning, command executions,
  MCP tool calls, and file changes into ACP updates.
- [x] Add pagination and size limits for large histories through
  `thread/turns/list`, with `thread/read` fallback for older Codex versions.
- [x] Add fake app-server tests for replay ordering.
- [x] Add ACP client tests for replay notification ordering before the
  `session/load` response and partial history behavior.

Acceptance criteria:

- [x] `session/list` plus `session/load` can reopen a useful prior conversation
  with replayed transcript state.
- [x] Large histories do not require loading all turns into memory when
  app-server supports paginated `thread/turns/list`.
- [x] Fork replay behavior is explicit and tested for the extension path.

## Core Session Mapping

ACP sessions should map directly to app-server threads.

```text
ACP SessionId == app-server thread.id
```

That keeps load, resume, list, close, delete, and Codex extension forking simple
and avoids a second identifier namespace.

If ACP requires an opaque session ID that cannot be the app-server thread ID,
store a local mapping:

```text
SessionId -> ThreadId
ThreadId -> SessionId
```

The first implementation should avoid that unless required by the ACP crate.

## ACP Surface and Capabilities

### initialize

Return capabilities based on:

- Built-in support in this adapter.
- App-server feature availability.
- ACP client capabilities.

Stable ACP v1 methods and capabilities:

- `session/new` is baseline and does not have a capability flag.
- `session/prompt` is baseline and uses `promptCapabilities` for optional
  content block types. All agents must support text and resource links.
- `session/cancel` is a notification, not a request-response method.
- `session/load` is enabled by `agentCapabilities.loadSession`.
- `session/resume` is enabled by `agentCapabilities.sessionCapabilities.resume`.
- `session/list` is enabled by `agentCapabilities.sessionCapabilities.list`.
- `session/close` is enabled by `agentCapabilities.sessionCapabilities.close`.
- `session/delete` is enabled by `agentCapabilities.sessionCapabilities.delete`.
- `additionalDirectories` is enabled by
  `agentCapabilities.sessionCapabilities.additionalDirectories`.
- session config options are returned in session lifecycle responses and updated
  through `session/set_config_option`.
- slash commands are advertised through `available_commands_update` and invoked
  as regular `session/prompt` text.

Do not advertise `session/fork` as stable ACP v1. Keep Codex forking behind a
clearly identified extension path until the local upstream docs include it in
`protocol/v1/schema.md`.

### session/new

Map to:

```text
thread/start
```

Pass:

- `cwd`
- sandbox or permission profile selection
- runtime workspace roots where available
- selected capability roots where available
- MCP servers when representable

Store the returned thread ID as the ACP session ID.

After creation:

- Send initial `skills/list` for the cwd.
- Send available commands.
- Send config options from model, collaboration-mode, and permission catalogs.
  The current implementation publishes `model`, `reasoning_effort`,
  `service_tier`, `approval_policy`, `collaboration_mode`, and
  `permission_profile`.

### session/load

Map to:

```text
thread/read
thread/resume
```

Implemented baseline:

- Call `thread/read` with `includeTurns: true`.
- Call `thread/resume` with `excludeTurns: true` so the loaded session is ready
  for new prompts.
- Replay known stored `ThreadItem` variants as ACP `session/update`
  notifications before returning `session/load`.
- Page large histories through `thread/turns/list`, with `thread/read` fallback
  for older app-server versions.
- Replay structured plan entries, MCP tool calls, command/file tool lifecycles,
  command output, file diffs, reasoning, and agent/user message text fragments
  where the app-server history exposes them.
- Preserve app-server agent-message item ids as ACP `messageId` values during
  live streaming and history replay when the backend exposes them.

Remaining work:

- Convert newly added or unknown app-server item variants to high-fidelity ACP
  updates instead of generic raw JSON fallbacks as those variants appear.

### session/resume

Map to:

```text
thread/resume
```

ACP v1 requires this to reconnect without replaying prior messages. Use it for
active session attachment, not transcript hydration.

After resume:

- Reconnect notification subscriptions.
- Refresh skills.
- Refresh config options. The current implementation refreshes `model`,
  `reasoning_effort`, `service_tier`, `approval_policy`,
  `collaboration_mode`, and `permission_profile`.
- Refresh available commands.

### session/list

Map to:

```text
thread/list
```

Apply the ACP `cwd` filter to app-server's `cwd` filter when present.

Return:

- session ID
- cwd
- additional directories when `sessionCapabilities.additionalDirectories` is
  supported and app-server provides lifecycle `runtimeWorkspaceRoots`, or when
  the adapter has persisted previously observed roots under `codexHome`
- title/name if available
- ACP `updatedAt` converted from app-server Unix seconds to an ISO 8601 UTC
  timestamp when app-server provides `updatedAt`
- app-server preview, status, model-provider, timestamp, recency, agent, and
  parent-thread fields under `_meta.brokk_codex_acp`
- adapter-specific archived/deleted metadata only under `_meta`; stable
  `SessionInfo` has no first-class archive field.

Known limitation:

- reporting additional directories for `session/list` entries that have never
  been started, loaded, resumed, or forked through this adapter; app-server
  `thread/list` does not currently include runtime workspace roots

### session/close

Preferred mapping:

```text
thread/unsubscribe
```

ACP v1 says close applies to an active session: cancel ongoing work as if
`session/cancel` were called, then free resources. If `thread/unsubscribe` does
not cancel active work by itself, interrupt the active turn before unsubscribing.
The current implementation cancels the adapter's active prompt and outstanding
permission requests for the session before calling `thread/unsubscribe`, which
causes the active turn loop to issue `turn/interrupt` when a turn is running.

Do not use close for archive or delete.

### session/delete

Map to:

```text
thread/delete
```

Only advertise `sessionCapabilities.delete` when app-server can remove the
session from future `session/list` results. ACP v1 allows soft or hard delete
and says deleting an unknown or already-deleted session should succeed silently
where practical.

### session/fork

Keep as an extension/RFD-backed feature, not a required stable ACP v1 method.
The local upstream docs include `rfds/session-fork.md`, but
`protocol/v1/schema.md` does not define `session/fork`.

Map to:

```text
thread/fork
```

Fork request shape in the adapter should include:

- source session ID
- optional cwd override
- optional ephemeral flag
- optional exclude-turns flag
- optional runtime workspace roots
- optional selected capability roots
- optional permission profile or sandbox override
- optional model/settings overrides

Suggested internal type:

```rust
pub struct ForkSessionRequest {
    pub session_id: SessionId,
    pub cwd: Option<PathBuf>,
    pub ephemeral: bool,
    pub exclude_turns: bool,
}
```

App-server request:

```json
{
  "method": "thread/fork",
  "params": {
    "threadId": "<source-thread-id>",
    "cwd": "<optional-cwd>",
    "ephemeral": false,
    "excludeTurns": false
  }
}
```

Response handling:

- Use the returned `thread.id` as the new ACP session ID.
- Register the new session in the session map.
- Auto-subscribe is handled by app-server, but the adapter must route future
  notifications for the new thread to the new ACP session.
- Return the same mode/config payload shape as `session/new` and
  `session/resume`.
- If `excludeTurns` is false, replay returned fork history as ACP updates.
- If `excludeTurns` is true, skip replay and rely on future turn events.

Slash command alias:

```text
/fork
```

Map `/fork` to the extension handler for the current session. If the command
includes text, create an ephemeral side fork and immediately start a turn with
that text only if the client UX wants Codex TUI-like `/side` behavior.

### session/prompt

For normal user input, map to:

```text
turn/start
```

For in-flight steering, optionally map to:

```text
turn/steer
```

The adapter should choose `turn/steer` only when:

- the target app-server thread has an active steerable turn
- the ACP client supports steering semantics
- the prompt is explicitly a follow-up to an active turn

Otherwise start a new turn.

### session/cancel

Map to:

```text
turn/interrupt
```

Track the active turn ID for each ACP session so cancellation is precise.

## Event Translation

The bridge should consume app-server notifications and produce ACP session
updates.

Primary notification families:

- `thread/started`
- `thread/status/changed`
- `thread/settings/updated`
- `turn/started`
- `turn/completed`
- `item/started`
- `item/completed`
- `item/agentMessage/delta`
- `item/plan/delta`
- `item/reasoning/summaryTextDelta`
- `item/reasoning/textDelta`
- `item/commandExecution/outputDelta`
- `item/commandExecution/terminalInteraction`
- `item/commandExecution/requestApproval`
- `item/fileChange/requestApproval`
- `item/permissions/requestApproval`
- `item/tool/requestUserInput`
- `serverRequest/resolved`
- `remoteControl/status/changed`
- `skills/changed`
- `app/list/updated`

Mapping rules:

- Agent message deltas -> `session/update` with `agent_message_chunk`.
- Reasoning deltas and summary section boundaries -> `session/update` with
  `agent_thought_chunk`.
- Plan item text deltas -> `session/update` with `agent_thought_chunk`.
- Command execution begin/end -> `session/update` with `tool_call` and
  `tool_call_update`, using `ToolKind::execute` where appropriate.
- Command output deltas and terminal interaction notifications ->
  `tool_call_update` content. Use ACP terminal methods only when Codex
  delegates execution to the client terminal capability.
- File edits -> `tool_call`/`tool_call_update` with diff content and locations.
- Plan changes -> `session/update` with `plan`; each update must include the
  complete plan entry list because ACP clients replace the plan wholesale.
- Approval requests -> client `session/request_permission` requests.
- Tool user-input requests -> ACP `elicitation/create` form requests with
  empty-answer fallback when the ACP client cannot answer.
- Dynamic tool callbacks -> explicit failure fallback until ACP exposes a
  compatible dynamic tool execution surface.
- Server request resolution notifications -> compact `agent_message_chunk`
  diagnostics because ACP v1 has no generic request-cleanup notification.
- Turn completion -> ACP prompt response stop reason.

The adapter should keep a per-session active item map:

```text
app-server item id -> ACP tool call id / message stream id
```

### Notification Mapping Matrix

| App-server notification | ACP output | Notes |
| --- | --- | --- |
| `turn/started` | internal active-turn state | Store `turn.id`; do not need a visible update by default. |
| `turn/completed` | `PromptResponse.stopReason` | Already handled for the active prompt path. |
| `item/agentMessage/delta` | `agent_message_chunk` | Already handled for the active prompt path. |
| `item/reasoning/summaryTextDelta` / `item/reasoning/summaryPartAdded` / `item/reasoning/textDelta` | `agent_thought_chunk` | Stable ACP v1 supports thought chunks; summary part boundaries become paragraph separators. |
| `item/plan/delta` | `agent_thought_chunk` | Implemented for experimental plan-mode text streaming. Structured `turn/plan/updated` remains the source for ACP `plan` updates. |
| `item/started` | `tool_call` or internal item state | Depends on item subtype. |
| `item/completed` | `tool_call_update` | Mark final status and attach final content. File-change items attach ACP `diff` content for each `{path, diff}` entry. |
| `item/commandExecution/outputDelta` | `tool_call_update` content | Preserve stdout/stderr boundaries if present. |
| `item/commandExecution/terminalInteraction` | `tool_call_update` content | Implemented as an input marker on the existing command tool call; this does not call ACP `terminal/create` because app-server still owns execution. |
| `item/fileChange/patchUpdated` | `tool_call_update` with `diff` content | Stream live per-file patch updates as structured ACP diff content. |
| `turn/diff/updated` | `tool_call_update` with text diff content | Useful for file edit previews. This remains text because the app-server event carries a turn-level unified diff without per-file old/new text. |
| `turn/plan/updated` | `plan` | Send the full plan every time. |
| `item/commandExecution/requestApproval`, `item/fileChange/requestApproval` | `session/request_permission` | Implemented for simple decisions; rich command `availableDecisions` such as exec-policy and network-policy amendments keep their original app-server payload under ACP option metadata and are returned unchanged when selected. App-server remains blocked until the ACP client answers. |
| `item/autoApprovalReview/started` / `item/autoApprovalReview/completed` | `agent_message_chunk` | Implemented as user-visible diagnostics summarizing automatic approval review lifecycle, target item, action type, risk, rationale, decision source, and duration. |
| `item/permissions/requestApproval` | `session/request_permission` | Implemented for full requested-profile grants, rejection, generated partial-grant options for individual requested network/filesystem units scoped to turn/session, and readable request content. |
| `mcpServer/elicitation/request` | `elicitation/create` with cancel fallback | Implemented for form, `openai/form`, and URL elicitations; falls back to app-server cancel semantics when the ACP client cannot answer. |
| `tool/requestUserInput`, `item/tool/requestUserInput` | `elicitation/create` with empty-answer fallback | Implemented as form elicitations from app-server questions; falls back to empty answers when the ACP client cannot answer. |
| `item/tool/call` | custom ACP extension request with fallback response | Implemented through `_brokk_codex_acp/dynamic_tool_call`; valid client responses are forwarded to app-server, and explicit failure fallback keeps app-server from blocking when clients cannot answer. |
| `serverRequest/resolved` | `agent_message_chunk` | Implemented as compact diagnostics for app-server request cleanup because ACP v1 has no generic cancellation/completion update for in-flight server-to-client requests. |
| `attestation/generate` | JSON-RPC error | Implemented as an explicit request failure because the adapter does not advertise or provide native attestation tokens. |
| `skills/changed` | `available_commands_update` | Implemented through the background app-server notification dispatcher; re-runs app-server `skills/list` with `forceReload`. |
| `thread/settings/updated` | `config_option_update` | Implemented through the background app-server notification dispatcher; refreshes model, collaboration-mode, and permission catalogs before publishing current options. |
| `thread/status/changed` | `session_info_update._meta` | Implemented through the background app-server notification dispatcher; preserves the full app-server status payload under adapter metadata. |
| `thread/deleted` / `thread/closed` | `session_info_update._meta` | Implemented through adapter lifecycle metadata so ACP clients can react to app-server lifecycle events. |
| `model/rerouted` | `agent_message_chunk` | Implemented as a non-invasive user-visible diagnostic chunk. |
| `model/safetyBuffering/updated` | `agent_message_chunk` | Implemented as a user-visible diagnostic chunk summarizing active buffering model, use cases, and reasons. |
| `model/verification` | `agent_message_chunk` | Implemented as a user-visible diagnostic chunk summarizing additional verification requirements. |
| `turn/moderationMetadata` | `agent_message_chunk` | Implemented as a user-visible diagnostic chunk preserving the metadata payload as compact JSON. |
| `mcpServer/startupStatus/updated` | `agent_message_chunk` | Implemented for thread-scoped MCP startup diagnostics and app-scoped updates published to known sessions. |
| `configWarning` | `agent_message_chunk` | Implemented for known sessions because app-server emits this warning without a thread id. |
| `windowsSandbox/setupCompleted` | `agent_message_chunk` | Implemented for known sessions because app-server emits this event without a thread id. |
| `account/login/completed` / `account/updated` / `account/rateLimits/updated` / `mcpServer/oauthLogin/completed` | `agent_message_chunk` | Implemented for known sessions because app-server emits these account/OAuth events without a thread id. |
| `app/list/updated` | `agent_message_chunk` | Implemented for known sessions so clients can observe app catalog refreshes. |
| `remoteControl/status/changed` | `agent_message_chunk` | Implemented for known sessions because app-server emits remote-control status without a thread id. |
| `fuzzyFileSearch/sessionUpdated` / `fuzzyFileSearch/sessionCompleted` | `agent_message_chunk` | Implemented as compact progress/completion diagnostics for known sessions. |
| `thread/realtime/started` / `thread/realtime/sdp` / `thread/realtime/itemAdded` / `thread/realtime/transcript/delta` / `thread/realtime/transcript/done` / `thread/realtime/outputAudio/delta` / `thread/realtime/error` / `thread/realtime/closed` | `agent_message_chunk` | Implemented as user-visible diagnostics for realtime lifecycle, SDP, raw item, text transcript, audio metadata, and transport events. Native audio playback remains pending because ACP v1 has no realtime audio stream. |
| `warning` / `error` | `agent_message_chunk` | Implemented with retry/details/error-code text when app-server provides it. |

## Slash Commands

ACP v1 has no separate slash-command execution method. Commands are advertised
with `available_commands_update`, then invoked as regular `session/prompt` text
whose first text block starts with `/`.

Commands should be divided into three categories.

### Backend Commands

These map cleanly to app-server APIs and should be supported early:

| Slash command | App-server mapping |
| --- | --- |
| `/review` | `review/start` `[implemented]` |
| `/compact` | `thread/compact/start` `[implemented]` |
| `/init` | transform into the AGENTS.md generation prompt `[implemented]` |
| `/rename <name>` | `thread/name/set` `[implemented]` |
| `/new` | `thread/start` `[implemented]` |
| `/resume <id-or-name>` | `thread/resume` after exact id/name/preview lookup `[implemented]` |
| `/fork` | `thread/fork` extension only `[implemented]` |
| `/archive` | `thread/archive` `[implemented]` |
| `/rollback <num-turns>` | `thread/rollback` `[implemented as status message; ACP v1 cannot delete visible transcript entries]` |
| `/delete` | `thread/delete` `[implemented]` |
| `/goal ...` | `thread/goal/*` `[implemented for get, clear, and objective updates]` |
| `/plan` | `thread/settings/update` with collaboration mode `[implemented]` |
| `/config [cwd]` | `config/read` `[implemented as summary]` |
| `/config-requirements` | `configRequirements/read` `[implemented as summary]` |
| `/account` | `account/read` `[implemented as summary]` |
| `/login [chatgpt\|device]` | `account/login/start` `[implemented as summary; API-key login intentionally not accepted in prompt text]` |
| `/login-cancel <loginId>` | `account/login/cancel` `[implemented as summary]` |
| `/logout` | `account/logout` `[implemented as summary]` |
| `/model` | `model/list` plus ACP config-option refresh `[implemented]` |
| `/model-provider` | `modelProvider/capabilities/read` `[implemented as summary]` |
| `/permissions` | `permissionProfile/list` plus ACP config-option refresh `[implemented]` |
| `/memory enable` / `/memory disable` / `/memory reset` | `thread/memoryMode/set` or `memory/reset` `[implemented as summary]` |
| `/features` | `experimentalFeature/list` `[implemented as summary]` |
| `/feature <name> enable|disable` | `experimentalFeature/enablement/set` `[implemented as summary]` |
| `/rate-limits` | `account/rateLimits/read` `[implemented as summary]` |
| `/usage` | `account/usage/read` `[implemented as summary]` |
| `/workspace-messages` | `account/workspaceMessages/read` `[implemented as summary]` |
| `/mcp` | `mcpServerStatus/list` `[implemented as summary]` |
| `/mcp-reload` | `config/mcpServer/reload` plus `mcpServerStatus/list` `[implemented as summary]` |
| `/mcp-resource <server> <uri>` | `mcpServer/resource/read` `[implemented as summary]` |
| `/mcp-tool <server> <tool> [json-arguments]` | `mcpServer/tool/call` `[implemented as summary]` |
| `/apps` | `app/list` `[implemented as summary]` |
| `/plugins` | `plugin/list` and `plugin/installed` `[implemented as summary]` |
| `/plugin <pluginName@marketplacePath>` | `plugin/read` `[implemented as summary]` |
| `/plugin-install <pluginName@marketplacePath>` | `plugin/install` `[implemented as summary]` |
| `/plugin-uninstall <pluginId>` | `plugin/uninstall` `[implemented as summary]` |
| `/marketplace-add <source> [ref] [sparsePathsCsv]` | `marketplace/add` `[implemented as summary]` |
| `/marketplace-remove <name>` | `marketplace/remove` `[implemented as summary]` |
| `/hooks` | `hooks/list` `[implemented as summary]` |
| `/skill-roots <paths...>` | `skills/extraRoots/set` plus `skills/list(forceReload)` `[implemented as process-local summary]` |
| `/ps` | `thread/backgroundTerminals/list` `[implemented as summary]` |
| `/stop` | `thread/backgroundTerminals/clean` `[implemented as summary]` |
| `/kill <process-id>` | `thread/backgroundTerminals/terminate` `[implemented as summary]` |
| `/status` | local summary plus app-server status/config reads `[implemented as initial loaded-thread summary]` |

### Command Parser Rules

- Only parse slash commands when the first non-whitespace character is `/`.
- Treat escaped `\/command` as plain text.
- Preserve the raw original input for error messages and logging.
- Split the command name from the rest of the line with shell-like quoting only
  where a command needs it; most commands can treat the remainder as a raw
  string.
- Do not parse slash commands inside code blocks or multi-line text unless the
  first logical user input line is the command.
- Prefer exact command names over aliases.
- Keep aliases explicit in the registry.
- Reject ambiguous or partial commands with suggestions.

### Command Registry Shape

The registry should be data-driven enough to publish ACP available commands and
route user input through the same source of truth.

```rust
struct CommandSpec {
    name: &'static str,
    aliases: &'static [&'static str],
    description: &'static str,
    availability: CommandAvailability,
    handler: CommandHandler,
}

enum CommandAvailability {
    Always,
    RequiresSession,
    RequiresActiveTurn,
    RequiresNoActiveTurn,
    RequiresAppServerMethod(&'static str),
}
```

Do not hardcode a separate available-commands list; derive it from the registry
plus the current session state and app-server capability probes.

Published command names should omit the leading `/`, matching ACP
`AvailableCommand.name` examples.

### Client/UI Commands

These should be advertised only when the ACP client can represent them:

| Slash command | Notes |
| --- | --- |
| `/copy` | Client clipboard action, not a backend action. |
| `/raw` | Client rendering preference. |
| `/theme` | Client UI preference. |
| `/keymap` | Client UI preference. |
| `/vim` | Client composer mode. |
| `/quit` | Client/server lifecycle action. |
| `/exit` | Client/server lifecycle action. |

### Unsupported Or Deferred Commands

These should be hidden or return a clear unsupported response until the adapter
has a real mapping:

| Slash command | Reason |
| --- | --- |
| `/feedback` | Requires product-specific upload UX. |
| `/app` | Opens Codex Desktop; not generally meaningful from ACP. |
| `/ide` | Requires client-specific editor context contract. |
| `/pets` | TUI-only. |
| debug commands | Not part of stable user-facing ACP behavior. |

## Skills Support

Skills are a first-class requirement.

ACP v1 does not define `skills/list` as an ACP method. `skills/list` below is an
app-server API used by the adapter, and the ACP-facing projection should be
available commands and config options. The local stable ACP v1 schema has no
mention-completion publication surface for agents, so skill mentions are an
app-server prompt-input detail rather than an ACP catalog projection for now.

### Discovery

On `session/new`, `session/load`, `session/resume`, extension `session/fork`,
and `skills/changed`, call:

```text
skills/list
```

Use the current session cwd. Cache the returned skills per cwd. The current
implementation refreshes lifecycle paths and `skills/changed` notifications,
then publishes enabled skills as `skill:<name>` available commands.

Expose skills to ACP clients as:

- [x] available commands if ACP only supports slash commands
- [x] no ACP v1 mention completions are available to publish
- [x] config options using select controls for enable/disable toggles

The app-server shape to use is:

```text
skills/list
skills/config/write
skills/extraRoots/set
plugin/skill/read
```

`skills/list` accepts `forceReload`; use it after config writes and
`skills/changed`, not on every prompt.

### Invocation

Support both forms:

```text
$skill-name Do the task.
/skill skill-name Do the task.
```

Preferred transport to app-server:

- [x] preserve the visible `$skill-name` text
- [x] include a structured skill item pointing at the skill path when app-server
  accepts that shape

Fallback transport:

- [x] send the text as-is and rely on Codex's skill mention parser

Structured app-server input should use `UserInput::Skill`:

```json
{
  "type": "skill",
  "name": "skill-name",
  "path": "/absolute/path/to/SKILL.md"
}
```

When the user writes `$skill-name extra instructions`, the `turn/start.input`
list includes both the visible text item and the structured skill item when
`skills/list` provided a path. Preserve the visible text in the ACP transcript
so the client still shows what the user typed.

### Enable/Disable

Map to:

```text
skills/config/write
```

Implemented baseline:

- App-server request/response mapping for `skills/config/write`.
- Fake app-server coverage that writes by name and verifies a forced
  `skills/list` refresh reflects the new enabled state.

Inputs:

- by absolute skill path when available
- by name only when path is not available

After write:

- [x] call `skills/list` with `forceReload: true`
- [x] publish updated available commands/config options from an ACP
  `session/set_config_option` handler

### Extra Roots

Expose a config path for:

```text
skills/extraRoots/set
```

This is process-local and should be documented as non-persistent. The adapter
currently exposes it as `/skill-roots <paths...>`, calls
`skills/extraRoots/set`, then refreshes the current cwd skills with
`forceReload: true`.

## Apps, Plugins, and MCP

The adapter should avoid implementing app/plugin/MCP discovery itself.

Use app-server endpoints:

- `app/list`
- `plugin/list`
- `plugin/installed`
- `plugin/read`
- `plugin/install`
- `plugin/uninstall`
- `mcpServerStatus/list`
- `mcpServer/resource/read`
- `mcpServer/tool/call`
- `config/mcpServer/reload`

Invocation should follow app-server mention semantics:

- apps use `app://<connector-id>`
- plugins use `plugin://<plugin-name>@<marketplace-name>`
- MCP tools are invoked by the model through Codex after config refresh

The adapter should expose these as command/catalog surfaces first, not as direct
model prompts:

- [x] `/apps` calls `app/list` and returns an ACP agent-message summary.
- [x] `/config [cwd]` calls `config/read` and returns an ACP agent-message
  summary.
- [x] `/config-requirements` calls `configRequirements/read` and returns an ACP
  agent-message summary.
- [x] `/account` calls `account/read` and returns an ACP agent-message summary.
- [x] `/login [chatgpt|device]` calls `account/login/start` and returns an ACP
  agent-message summary.
- [x] `/login-cancel <loginId>` calls `account/login/cancel` and returns an ACP
  agent-message summary.
- [x] `/logout` calls `account/logout` and returns an ACP agent-message summary.
- [x] `/plugins` calls `plugin/list` and `plugin/installed` and returns an ACP
  agent-message summary.
- [x] `/plugin <pluginName@marketplacePath>` calls `plugin/read` and returns an
  ACP agent-message summary.
- [x] `/plugin-install <pluginName@marketplacePath>` calls `plugin/install` and
  returns an ACP agent-message summary.
- [x] `/plugin-uninstall <pluginId>` calls `plugin/uninstall` and returns an ACP
  agent-message summary.
- [x] `/marketplace-add <source> [ref] [sparsePathsCsv]` calls
  `marketplace/add` and returns an ACP agent-message summary.
- [x] `/marketplace-remove <name>` calls `marketplace/remove` and returns an
  ACP agent-message summary.
- [x] `/rate-limits` calls `account/rateLimits/read` and returns an ACP
  agent-message summary.
- [x] `/usage` calls `account/usage/read` and returns an ACP agent-message
  summary.
- [x] `/workspace-messages` calls `account/workspaceMessages/read` and returns
  an ACP agent-message summary.
- [x] `/model-provider` calls `modelProvider/capabilities/read` and returns an
  ACP agent-message summary.
- [x] `/mcp` calls `mcpServerStatus/list` and returns an ACP agent-message
  summary.
- [x] `/mcp-reload` calls `config/mcpServer/reload`, refreshes
  `mcpServerStatus/list`, and returns an ACP agent-message summary.
- [x] `/mcp-resource <server> <uri>` calls `mcpServer/resource/read` for the
  current thread and returns an ACP agent-message summary.
- [x] `/mcp-tool <server> <tool> [json-arguments]` calls
  `mcpServer/tool/call` for the current thread and returns an ACP agent-message
  summary.
- [x] `/hooks` calls `hooks/list` and returns an ACP agent-message summary.
- [x] `/status` calls `thread/loaded/list` and returns an ACP agent-message
  summary.
- [x] `$app-slug` text prompts add a structured `mention` input item with the
  resolved `app://<connector-id>` path when `app/list` can identify an
  accessible app.
- [x] `@plugin-name` text prompts add a structured `mention` input item with
  the resolved `plugin://<plugin-name>@<marketplace-name>` path when
  `plugin/installed` can identify the installed plugin.
- resource reads and direct tool calls should only be exposed when an ACP client
  has a clear UI affordance for them.

## Config Options

Expose session config options from app-server catalogs.

Recommended options:

- model
- reasoning effort
- permission profile
- approval policy if ACP supports it
- sandbox mode if ACP supports it
- collaboration mode
- enabled skills

ACP v1 config options currently support `select` controls. Use semantic
categories `model`, `mode`, and `thought_level` where they fit, but do not rely
on categories for correctness. Prefer config options over dedicated session
modes; ACP marks `session/set_mode` and `modes` as compatibility surfaces that
will be removed in a future protocol version.

Mappings:

- `model/list` -> model, reasoning effort, and service tier pickers
  `[implemented]`
- `permissionProfile/list` -> permissions picker `[implemented for profile id selection]`
- `collaborationMode/list` -> mode picker `[implemented]`
- `thread/settings/update` -> persisted next-turn setting changes
  `[implemented for model, effort, service tier, approval policy, collaboration mode, and permissions]`

ACP config option IDs should be stable and adapter-owned:

| ACP option id | App-server source | App-server write |
| --- | --- | --- |
| `model` | `model/list` | `thread/settings/update.model` |
| `reasoning_effort` | `model/list` selected model metadata | `thread/settings/update.effort` |
| `service_tier` | `model/list` selected model metadata | `thread/settings/update.serviceTier` |
| `permission_profile` | `permissionProfile/list` | `thread/settings/update.permissions` |
| `approval_policy` | config/read or thread settings | `thread/settings/update.approvalPolicy` |
| `collaboration_mode` | `collaborationMode/list` | `thread/settings/update.collaborationMode` |
| `skill:<name>` | `skills/list` | `skills/config/write` |

Implemented config option baseline:

- `model` is populated from `model/list`, seeded from app-server
  `thread/start`, `thread/resume`, or `thread/fork` response model when present,
  and written with `thread/settings/update.model`.
- `reasoning_effort` is populated from the selected model's catalog metadata,
  seeded from app-server thread lifecycle responses when present, and written
  with `thread/settings/update.effort`.
- `service_tier` is populated from the selected model's catalog metadata, seeded
  from app-server thread lifecycle responses when present, and written with
  `thread/settings/update.serviceTier`; the adapter's synthetic `Automatic`
  option clears the app-server override with `null`.
- `approval_policy` is seeded from app-server thread lifecycle responses when
  present, defaults to app-server's normal `on-request` behavior otherwise, and
  is written with `thread/settings/update.approvalPolicy`.
- `collaboration_mode` is populated from `collaborationMode/list`, seeded from
  app-server thread lifecycle responses when present, and written with
  `thread/settings/update.collaborationMode`.
- `permission_profile` is populated from `permissionProfile/list`, seeded from
  app-server `activePermissionProfile` when present, and written with
  `thread/settings/update.permissions`.
- `skill:<name>` options are populated from `skills/list`, exposed as
  enable/disable select controls, and written with `skills/config/write`.
- `session/set_config_option` returns the complete current config option list
  and sends a `config_option_update` notification after successful writes.

## Approval Flow

Approval routing should preserve app-server semantics.

When app-server emits a command or file-change approval request, the adapter now:

- translates it to an ACP permission request
- includes the app-server request payload as raw tool input, with command/file
  kind and a user-facing title
- preserves the standard "approve for session" option by
  mapping them to ACP permission options with stable `optionId`s and the closest
  `kind` (`allow_once`, `allow_always`, `reject_once`, or `reject_always`)
- sends the user's decision back to app-server through the matching response
  method
- preserves rich command `availableDecisions` such as exec-policy and
  network-policy amendments by exposing them as ACP permission options with the
  original app-server decision payload under `_meta.brokk_codex_acp`, then
  returning that original payload if selected

Remaining approval notes:

- Dynamic tool calls now use `_brokk_codex_acp/dynamic_tool_call` with the
  existing explicit failure fallback when the ACP client cannot answer.
- Do not route app-server-owned command execution through ACP `terminal/create`
  without an app-server handoff event; the adapter would otherwise duplicate
  execution outside Codex's approval and policy owner.

Do not invent approval policies in the adapter. Policies should come from
Codex config, app-server thread settings, or explicit ACP session options.

### Approval Implementation Notes

- Treat app-server approval notifications as blocking requests.
- The current implementation answers the app-server request inline after the
  ACP permission response returns.
- Include command argv, cwd, sandbox profile, affected paths, and any app-server
  rationale in the raw tool input until ACP exposes richer first-class fields.
- Map ACP `selected` outcomes by `optionId` to the app-server approval response
  shape.
- Map ACP `cancelled` distinctly.
- When the ACP client disconnects, reject outstanding approval requests with a
  cancellation outcome.

## History and Replay

For session load/resume/fork, replay history according to the stable ACP method
semantics first, then extension semantics.

Rules:

- `session/load` must replay stored conversation entries as `session/update`
  notifications before responding.
- `session/resume` must return an active session without replaying prior
  messages.
- `session/fork` extension behavior should replay returned fork history unless
  `excludeTurns` is set.
- Large histories should use app-server pagination when available instead of
  loading all items into memory.

## Error Handling

Use explicit, user-actionable errors:

- app-server unavailable
- app-server handshake failed
- session not found
- unsupported command
- command unavailable during active turn
- feature unavailable in current Codex version
- auth required
- invalid permission profile
- fork source session not loaded or not found when the fork extension is enabled

Prefer graceful degradation:

- hide commands that cannot work
- return a concise error for commands that become unavailable at runtime
- keep normal prompting available even if catalogs fail

## Testing Plan

### Unit Tests

- ACP request to app-server request mapping.
- App-server notification to ACP update mapping.
- Slash command parser and router.
- Skills list cache invalidation.
- `session/delete` request and response mapping.
- `session/fork` extension request and response mapping.
- Approval option mapping for command/file-change requests.
- Prompt cancellation state cleanup.
- Active item mapping for command execution and MCP calls.

### Integration Tests

Use a fake app-server JSON-RPC process first.

Scenarios:

- initialize `[done]`
- new session `[done]`
- prompt and stream final answer `[done]`
- command tool call output
- approval request and approval response `[done at app-server client level for command approvals]`
- skills list and changed notification
- enable/disable skill
- delete listed session
- fork session and prompt in fork extension `[partial: fork mapping covered]`
- cancel active turn `[done at app-server client level]`

Then add smoke tests against a real local `codex app-server --stdio` when the
Codex source checkout is available.

### Manual Tests

Use an ACP client such as Zed or a small JSON-RPC harness.

Manual flows:

- create session in a repo
- run normal prompt
- run `/init`
- run `/review`
- run `/compact`
- invoke `$skill-name`
- disable and re-enable a skill
- delete a listed session
- fork session and continue independently when the extension is enabled
- list and resume sessions
- trigger a command approval

## Implementation Phases

### Phase 1: Transport and Thread Basics

- [x] Add dependencies for ACP, async runtime, serde, and JSON-RPC transport.
- [x] Spawn `codex app-server --stdio`.
- [x] Implement app-server client with request IDs and notification dispatch.
- [x] Implement ACP initialize.
- [x] Implement `session/new`, `session/resume`, `session/list`, and
  `session/close`.
- [x] Implement `session/fork` via `thread/fork` extension.
- [x] Implement basic text `prompt` via `turn/start`.
- [x] Add fake app-server integration tests for thread and prompt mappings.
- [x] Implement cancellation via `turn/interrupt`.
- [x] Implement `session/load` and advertise `loadSession`.
- [x] Implement `session/delete` and advertise `sessionCapabilities.delete`.

### Phase 2: Event Translation

- [x] Map agent message deltas for the active prompt.
- [x] Map turn completion for the active prompt.
- [x] Add active turn tracking for cancellation.
- [x] Move prompt notification handling into a typed app-server event dispatcher.
- [x] Map reasoning deltas.
- [x] Map command execution lifecycle and output.
- [x] Map file diffs/changes.
- [x] Map MCP tool calls at the lifecycle level.
- [x] Add active item/tool-call output tracking for prompt-local updates.
- [x] Add buffered output fallback for clients without terminal streaming.
- [x] Add serialized ACP client tests for each notification family.

### Phase 3: Slash Commands

- [x] Add initial command parser.
- [x] Add command registry.
- [x] Publish initial adapter-owned command through ACP available commands.
- [x] Implement `/new`.
- [x] Implement `/resume`.
- [x] Implement `/fork`.
- [x] Implement `/review`.
- [x] Implement `/compact`.
- [x] Implement `/init`.
- [x] Implement `/rename`.
- [x] Implement `/config-requirements`.
- [x] Implement `/archive`.
- [x] Implement `/rollback`.
- [x] Implement `/goal`.
- [x] Implement `/model`.
- [x] Implement `/permissions`.
- [x] Implement `/plan`.
- [x] Implement `/mcp`.
- [x] Implement `/mcp-reload`.
- [x] Implement `/apps`.
- [x] Implement `/plugins`.
- [x] Implement `/hooks`.
- [x] Implement `/status`.
- [x] Implement `/ps`.
- [x] Implement `/stop`.
- [x] Implement `/account`.
- [x] Implement `/login`.
- [x] Implement `/login-cancel`.
- [x] Implement `/logout`.
- [x] Implement `/rate-limits`.
- [x] Implement `/usage`.
- [x] Implement `/workspace-messages`.
- [x] Implement `/model-provider`.

### Phase 4: Skills

- [x] Implement `skills/list` request/response types.
- [x] Implement skill cache by cwd.
- [x] Refresh skills on session lifecycle.
- [x] Refresh skills on `skills/changed` notifications.
- [x] Publish skills as ACP commands.
- [x] Document that stable ACP v1 has no skill mention publication surface.
- [x] Support `$skill-name` invocation.
- [x] Support `/skill skill-name` invocation.
- [x] Implement app-server `skills/config/write` mapping.
- [x] Expose enable/disable through ACP session config options.
- [x] Support `skills/extraRoots/set`.
- [x] Add fake app-server tests for discovery.
- [x] Add fake app-server tests for invocation.
- [x] Add fake app-server tests for app-server config writes.
- [x] Add fake app-server tests for invalidation notification decoding and
  background app-server message dispatch.

### Phase 5: Session Delete and Fork Extension

- [x] Add ACP `session/delete` handler through the crate's
  `unstable_session_delete` feature.
- [x] Map `session/delete` to app-server session removal.
- [x] Hide `sessionCapabilities.delete` until the mapping removes sessions from
  future `session/list` results.

- [x] Add `session/fork` extension handler exposed by the current Rust crate.
- [x] Map to app-server `thread/fork`.
- [x] Return the returned thread as a new ACP session.
- [x] Mark `session/fork` as extension/RFD behavior in code and docs.
- [x] Replay fork history when app-server returns copied turns.
- [x] Route `/fork` through the same extension code path.
- [x] Add tests for persistent and ephemeral forks.

### Phase 6: Catalogs and Advanced Surfaces

- [x] Add model, reasoning effort, and service tier config options.
- [x] Add permission profile config options.
- [x] Add approval policy config options.
- [x] Add collaboration mode config options.
- [x] Add apps/plugins/MCP commands.
- [x] Add MCP config reload command.
- [x] Add effective config display.
- [x] Add config requirements display.
- [x] Add marketplace add/remove commands.
- [x] Add account status display.
- [x] Add account login/logout commands.
- [x] Add experimental feature flag display.
- [x] Add experimental feature enable/disable command.
- [x] Add initial hooks display.
- [x] Add background terminal list/clean.
- [x] Add account rate limit display.
- [x] Add account usage display.
- [x] Add workspace messages display.
- [x] Add model provider capabilities display.

### Phase 7: Hardening

- [x] Version-gate app-server methods through method-unavailable detection for
  experimental adapter surfaces.
- [x] Add compatibility handling for older Codex versions.
- [x] Add structured logging around app-server requests and ACP dispatch.
- [x] Add retry handling for app-server overload backpressure.
- [x] Add backpressure handling for notification bursts.
- [x] Add shutdown cleanup for app-server child process.
- [x] Add real app-server smoke tests.
- [x] Add connection-disconnect cleanup for active prompts.
- [x] Add connection-disconnect cleanup for outstanding approvals.
- [x] Cancel active prompt turns and outstanding approvals during
  `session/close` before unsubscribing.
- [x] Add error mapping tests.

## Open Questions

- Should `/fork <prompt>` create a persistent fork, or should that behavior be
  reserved for `/side <prompt>` as an ephemeral fork?
- Which ACP clients can represent app/plugin icons, descriptions, and install
  states?
- Should the adapter use the installed `codex` binary or link app-server crates
  directly in-process?

## Next Concrete PRs

Keep PRs small enough to review against fake app-server tests.

1. ACP terminal handoff, only after app-server exposes a command event that asks
   the adapter/client to own execution:
   - call `terminal/create` when the ACP client advertises terminal support
   - embed the returned terminal id in the related tool call before releasing it
   - keep app-server-owned command execution on the existing command-event path
