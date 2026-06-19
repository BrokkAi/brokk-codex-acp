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
      "experimentalApi": true
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
  - `session/load` is not implemented yet because `loadSession` is currently
    advertised as false.
  - `session/resume`
  - `session/list`
  - `session/close`
  - `session/fork` through the Rust crate extension, not stable ACP v1.
  - `session/delete` is not implemented yet.
  - `session/prompt`
  - `session/cancel`
- App-server mappings:
  - `session/new` -> `thread/start`
  - `session/load` -> planned `thread/read`/`thread/resume` history replay
  - `session/resume` -> `thread/resume`
  - `session/list` -> `thread/list`
  - `session/close` -> `thread/unsubscribe`
  - `session/fork` -> `thread/fork` extension
  - `session/delete` -> planned `thread/delete`
  - `session/prompt` -> `turn/start`
  - `session/cancel` -> `turn/interrupt`
- Event translation:
  - `item/agentMessage/delta` -> ACP agent message chunks
  - `turn/completed` -> ACP prompt response completion

This baseline intentionally supports only text and resource-link prompt blocks,
and advertises stable ACP v1 `sessionCapabilities.list`, `.resume`, and `.close`.
Tool calls, command output, approval requests, reasoning chunks, `session/load`
history replay, `session/delete`, skills catalogs, and slash command routing
remain planned work.

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

- Add a typed app-server notification dispatcher instead of handling only
  prompt-local `item/agentMessage/delta` and `turn/completed`.
- Track active turns by `threadId` and `turnId`.
- Track active items by app-server `itemId`.
- Map command execution, tool calls, reasoning, file changes, and usage updates
  into ACP updates.
- Add tests that feed fake app-server notifications and assert ACP output.

Acceptance criteria:

- Agent text streams as it does today.
- Reasoning deltas appear as ACP thought chunks when supported.
- Shell commands appear as ACP tool calls.
- Shell output streams incrementally.
- File changes appear as tool call updates or diff content.
- Prompt completion returns the correct `StopReason`.

### Milestone B: Skills Catalog and Invocation

Goal: skills should be discoverable and invokable, not just passed through as
unstructured text.

Tasks:

- Add app-server `skills/list` request support in the adapter.
- Refresh skills on `session/new`, `session/load`, `session/resume`, and
  `skills/changed`. Also refresh after `session/fork` only when the fork
  extension is enabled.
- Cache skills by cwd and invalidate on `skills/changed`.
- Publish skills through ACP available commands and, where supported, mention
  metadata.
- Convert `$skill-name` and `/skill skill-name` input into app-server
  `UserInput::Skill` when the skill path is known.
- Fall back to plain text when a skill cannot be resolved.
- Add `skills/config/write` support for enable/disable.

Acceptance criteria:

- A client can discover available skills for a session cwd through ACP-supported
  projection surfaces, initially `available_commands_update` and config
  options.
- `$skill-name do work` reaches Codex with structured skill metadata when
  possible.
- Disabled skills disappear from the published list after refresh.
- Unknown skills produce a clear error or clean text fallback.

### Milestone C: Slash Command Router

Goal: supported slash commands should route to real app-server APIs or explicit
client behavior, not model prompts.

Tasks:

- Add a parser that only treats a leading slash at the start of the user message
  as a command.
- Build a command registry with name, aliases, availability, required active
  turn state, and handler.
- Publish ACP available commands from that registry plus skills.
- Implement backend commands first: `/new`, `/resume`, `/review`,
  `/compact`, `/rename`, `/model`, `/permissions`, `/mcp`, `/apps`,
  `/plugins`, `/hooks`, and `/status`. Implement `/fork` only as an extension
  command backed by Codex `thread/fork`, not as required ACP v1 behavior.
- Return explicit unsupported-command responses for known UI-only commands that
  ACP cannot represent.
- Add fake app-server tests for each backend command mapping.

Acceptance criteria:

- `/fork`, when the extension is enabled, creates a new session via
  `thread/fork`.
- `/review` calls `review/start`.
- `/compact` calls the app-server compaction API when available.
- `/model` and `/permissions` expose pickers/config updates rather than sending
  text to the model.
- Unknown commands never silently become prompts unless explicitly configured.

### Milestone D: Session History and Replay

Goal: resume, load, and fork should be useful in clients that need transcript
hydration.

Tasks:

- Add `thread/read` support.
- Implement `session/load` as the stable ACP v1 history-replay path.
- Keep `session/resume` as a no-replay reconnect path, as required by ACP v1.
- Convert stored user messages, agent messages, reasoning, command executions,
  MCP tool calls, and file changes into ACP updates.
- Add pagination and size limits for large histories.
- Add tests for replay ordering and partial history.

Acceptance criteria:

- `session/list` plus `session/resume` can reopen a useful prior conversation.
- Large histories do not require loading all turns into memory.
- Fork replay behavior is explicit and tested for the extension path.

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
- Send config options from model and permission catalogs.

### session/load

Map to:

```text
thread/read
thread/resume
```

Only implement and advertise this when `loadSession` can be true. ACP v1
requires `session/load` to replay the entire conversation history as
`session/update` notifications before sending the `session/load` response. After
the response, the session must be ready for new prompts.

Use `thread/read` or `thread/resume` with history included, depending on the
app-server API that provides ordered history without losing live subscriptions.

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
- Refresh config options.
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
  supported and app-server provides them
- title/name if available
- updated time if available
- adapter-specific archived/deleted metadata only under `_meta`; stable
  `SessionInfo` has no first-class archive field.

### session/close

Preferred mapping:

```text
thread/unsubscribe
```

ACP v1 says close applies to an active session: cancel ongoing work as if
`session/cancel` were called, then free resources. If `thread/unsubscribe` does
not cancel active work by itself, interrupt the active turn before unsubscribing.

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
- `item/reasoning/delta`
- `item/commandExecution/outputDelta`
- `item/commandExecution/requestApproval`
- `item/tool/requestUserInput`
- `skills/changed`
- `app/list/updated`

Mapping rules:

- Agent message deltas -> `session/update` with `agent_message_chunk`.
- Reasoning deltas -> `session/update` with `agent_thought_chunk`.
- Command execution begin/end -> `session/update` with `tool_call` and
  `tool_call_update`, using `ToolKind::execute` where appropriate.
- Command output deltas -> `tool_call_update` content. Use ACP terminal methods
  only when Codex delegates execution to the client terminal capability.
- File edits -> `tool_call`/`tool_call_update` with diff content and locations.
- Plan changes -> `session/update` with `plan`; each update must include the
  complete plan entry list because ACP clients replace the plan wholesale.
- Approval requests -> client `session/request_permission` requests.
- Tool user-input requests -> defer until ACP has a stable elicitation surface,
  or expose a clear error message.
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
| `item/reasoning/delta` | `agent_thought_chunk` | Stable ACP v1 supports thought chunks. |
| `item/started` | `tool_call` or internal item state | Depends on item subtype. |
| `item/completed` | `tool_call_update` | Mark final status and attach final content. |
| `item/commandExecution/outputDelta` | `tool_call_update` content | Preserve stdout/stderr boundaries if present. |
| `turn/diff/updated` | `tool_call_update` with diff content | Useful for file edit previews. |
| `turn/plan/updated` | `plan` | Send the full plan every time. |
| `permissions/requestApproval` | `session/request_permission` | Must block app-server until the ACP client answers. |
| `skills/changed` | `available_commands_update` and `config_option_update` | Re-run app-server `skills/list` first. |
| `model/rerouted` | `session_info_update` or warning chunk | Prefer non-invasive visibility. |
| `warning` / `error` | agent message chunk or tool-call error | Keep user-actionable text. |

## Slash Commands

ACP v1 has no separate slash-command execution method. Commands are advertised
with `available_commands_update`, then invoked as regular `session/prompt` text
whose first text block starts with `/`.

Commands should be divided into three categories.

### Backend Commands

These map cleanly to app-server APIs and should be supported early:

| Slash command | App-server mapping |
| --- | --- |
| `/review` | `review/start` |
| `/compact` | `thread/compact/start` |
| `/init` | transform into the AGENTS.md generation prompt |
| `/rename <name>` | `thread/name/set` |
| `/new` | `thread/start` |
| `/resume <id-or-name>` | `thread/resume` after lookup |
| `/fork` | `thread/fork` extension only |
| `/archive` | `thread/archive` |
| `/delete` | `thread/delete` |
| `/goal ...` | `thread/goal/*` |
| `/plan` | `thread/settings/update` with collaboration mode |
| `/model` | `model/list` plus `thread/settings/update` |
| `/permissions` | `permissionProfile/list` plus settings update |
| `/mcp` | `mcpServerStatus/list` |
| `/apps` | `app/list` |
| `/plugins` | `plugin/list` and `plugin/installed` |
| `/hooks` | `hooks/list` |
| `/ps` | `thread/backgroundTerminals/list` |
| `/stop` | `thread/backgroundTerminals/clean` |
| `/status` | local summary plus app-server status/config reads |

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
available commands, config options, or future mention support depending on what
clients can render.

### Discovery

On `session/new`, `session/load`, `session/resume`, extension `session/fork`,
and `skills/changed`, call:

```text
skills/list
```

Use the current session cwd. Cache the returned skills per cwd.

Expose skills to ACP clients as:

- available commands if ACP only supports slash commands
- mention completions if ACP supports mentions
- config options if ACP supports enable/disable toggles

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

- preserve the visible `$skill-name` text
- include a structured mention item pointing at the skill path when app-server
  accepts that shape

Fallback transport:

- send the text as-is and rely on Codex's skill mention parser

Structured app-server input should use `UserInput::Skill`:

```json
{
  "type": "skill",
  "name": "skill-name",
  "path": "/absolute/path/to/SKILL.md"
}
```

When the user writes `$skill-name extra instructions`, the `turn/start.input`
list should include both the skill item and a text item for the remaining user
text. Preserve the visible text in the ACP transcript so the client still shows
what the user typed.

### Enable/Disable

Map to:

```text
skills/config/write
```

Inputs:

- by absolute skill path when available
- by name only when path is not available

After write:

- call `skills/list` with `forceReload: true`
- publish updated available commands/config options

### Extra Roots

Expose a config path for:

```text
skills/extraRoots/set
```

This is process-local and should be documented as non-persistent.

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

- `/apps` should call `app/list`.
- `/plugins` should call `plugin/list` and `plugin/installed`.
- `/mcp` should call `mcpServerStatus/list`.
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

- `model/list` -> model picker
- `permissionProfile/list` -> permissions picker
- `collaborationMode/list` -> mode picker
- `thread/settings/update` -> persisted next-turn setting changes

ACP config option IDs should be stable and adapter-owned:

| ACP option id | App-server source | App-server write |
| --- | --- | --- |
| `model` | `model/list` | `thread/settings/update.model` |
| `reasoning_effort` | `model/list` selected model metadata | `thread/settings/update.effort` |
| `permission_profile` | `permissionProfile/list` | `thread/settings/update.permissions` |
| `approval_policy` | config/read or thread settings | `thread/settings/update.approvalPolicy` |
| `collaboration_mode` | `collaborationMode/list` | `thread/settings/update.collaborationMode` |
| `skills.enabled` | `skills/list` | `skills/config/write` |

## Approval Flow

Approval routing should preserve app-server semantics.

When app-server emits a command approval request:

- translate it to an ACP permission request
- include command, cwd, reason, affected paths, and suggested actions
- preserve any "approve for session" or "remember this pattern" options by
  mapping them to ACP permission options with stable `optionId`s and the closest
  `kind` (`allow_once`, `allow_always`, `reject_once`, or `reject_always`)
- send the user's decision back to app-server through the matching response
  method

Do not invent approval policies in the adapter. Policies should come from
Codex config, app-server thread settings, or explicit ACP session options.

### Approval Implementation Notes

- Treat app-server approval notifications as blocking requests.
- Store pending app-server request IDs by ACP permission request ID.
- Include command argv, cwd, sandbox profile, affected paths, and any app-server
  rationale.
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
- Approval option mapping.
- Prompt cancellation state cleanup.
- Active item mapping for command execution and MCP calls.

### Integration Tests

Use a fake app-server JSON-RPC process first.

Scenarios:

- initialize `[done]`
- new session `[done]`
- prompt and stream final answer `[done]`
- command tool call output
- approval request and approval response
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
- [ ] Implement stable `session/load` and advertise `loadSession`.
- [ ] Implement stable `session/delete` and advertise `sessionCapabilities.delete`.

### Phase 2: Event Translation

- [x] Map agent message deltas for the active prompt.
- [x] Map turn completion for the active prompt.
- [x] Add active turn tracking for cancellation.
- [ ] Move notification handling into a typed app-server event dispatcher.
- [ ] Map reasoning deltas.
- [ ] Map command execution lifecycle and output.
- [ ] Map file diffs/changes.
- [ ] Map MCP tool calls.
- [ ] Add active item/tool-call tracking.
- [ ] Add buffered output fallback for clients without terminal streaming.
- [ ] Add fake app-server tests for each notification family.

### Phase 3: Slash Commands

- [ ] Add command parser.
- [ ] Add command registry.
- [ ] Publish command registry through ACP available commands.
- [ ] Implement `/new`.
- [ ] Implement `/resume`.
- [ ] Implement `/fork`.
- [ ] Implement `/review`.
- [ ] Implement `/compact`.
- [ ] Implement `/init`.
- [ ] Implement `/rename`.
- [ ] Implement `/goal`.
- [ ] Implement `/model`.
- [ ] Implement `/permissions`.
- [ ] Implement `/mcp`.
- [ ] Implement `/apps`.
- [ ] Implement `/plugins`.
- [ ] Implement `/hooks`.
- [ ] Implement `/status`.

### Phase 4: Skills

- [ ] Implement `skills/list` request/response types.
- [ ] Implement skill cache by cwd.
- [ ] Refresh skills on session lifecycle and `skills/changed`.
- [ ] Publish skills as ACP commands or mentions.
- [ ] Support `$skill-name` invocation.
- [ ] Support `/skill skill-name` invocation.
- [ ] Implement enable/disable with `skills/config/write`.
- [ ] Support `skills/extraRoots/set`.
- [ ] Add fake app-server tests for discovery, invocation, and invalidation.

### Phase 5: Session Delete and Fork Extension

- [ ] Add stable ACP `session/delete` handler.
- [ ] Map `session/delete` to app-server session removal.
- [ ] Hide `sessionCapabilities.delete` until the mapping removes sessions from
  future `session/list` results.

- [x] Add `session/fork` extension handler exposed by the current Rust crate.
- [x] Map to app-server `thread/fork`.
- [x] Return the returned thread as a new ACP session.
- [ ] Mark `session/fork` as extension/RFD behavior in code and docs.
- [ ] Replay fork history when requested.
- [ ] Route `/fork` through the same extension code path.
- [ ] Add tests for persistent and ephemeral forks.

### Phase 6: Catalogs and Advanced Surfaces

- [ ] Add model and reasoning effort config options.
- [ ] Add permission profile config options.
- [ ] Add approval policy config options.
- [ ] Add collaboration mode config options.
- [ ] Add apps/plugins/MCP commands.
- [ ] Add hooks display.
- [ ] Add background terminal list/clean.

### Phase 7: Hardening

- [ ] Version-gate app-server methods.
- [ ] Add compatibility handling for older Codex versions.
- [ ] Add structured logging around app-server requests and ACP dispatch.
- [ ] Add backpressure for notification bursts.
- [x] Add shutdown cleanup for app-server child process.
- [ ] Add real app-server smoke tests.
- [ ] Add connection-disconnect cleanup for active prompts and approvals.
- [ ] Add error mapping tests.

## Open Questions

- Does `thread/unsubscribe` fully satisfy ACP `session/close`, including
  cancelling ongoing work and freeing active resources, or must the adapter
  explicitly interrupt first?
- Should `/fork <prompt>` create a persistent fork, or should that behavior be
  reserved for `/side <prompt>` as an ephemeral fork?
- How should skills appear in ACP clients that do not support mention
  completions?
- Which ACP clients can represent app/plugin icons, descriptions, and install
  states?
- Should the adapter use the installed `codex` binary or link app-server crates
  directly in-process?

## Next Concrete PRs

Keep PRs small enough to review against fake app-server tests.

1. App-server event dispatcher:
   - add typed notification enum
   - move active prompt handling onto the dispatcher
   - keep existing behavior unchanged

2. Command execution streaming:
   - decode command execution start/output/completion notifications
   - map them to ACP `tool_call` and `tool_call_update` session updates
   - add fake app-server tests

3. Skills discovery:
   - add `skills/list`
   - publish available commands for skills
   - refresh on session lifecycle

4. Slash command parser and `/fork`:
   - add parser and registry
   - route `/fork` through existing fork extension logic
   - publish `/fork` as an ACP available command

5. Config options:
   - add `model/list` and `permissionProfile/list`
   - publish `model` and `permission_profile` ACP config options
   - write changes with `thread/settings/update`
