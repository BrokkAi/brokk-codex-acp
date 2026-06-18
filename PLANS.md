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
- Session lifecycle, including resume, list, close, and fork.
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
  - `session/resume`
  - `session/list`
  - `session/close`
  - `session/fork`
  - `session/prompt`
- App-server mappings:
  - `session/new` -> `thread/start`
  - `session/resume` -> `thread/resume`
  - `session/list` -> `thread/list`
  - `session/close` -> `thread/unsubscribe`
  - `session/fork` -> `thread/fork`
  - `session/prompt` -> `turn/start`
- Event translation:
  - `item/agentMessage/delta` -> ACP agent message chunks
  - `turn/completed` -> ACP prompt response completion

This baseline intentionally supports only text and resource-link prompt blocks.
Tool calls, command output, approval requests, reasoning chunks, cancellation,
history replay, skills catalogs, and slash command routing remain planned work.

## Core Session Mapping

ACP sessions should map directly to app-server threads.

```text
ACP SessionId == app-server thread.id
```

That keeps resume, fork, list, archive, and delete behavior simple and avoids a
second identifier namespace.

If ACP requires an opaque session ID that cannot be the app-server thread ID,
store a local mapping:

```text
SessionId -> ThreadId
ThreadId -> SessionId
```

The first implementation should avoid that unless required by the ACP crate.

## Required ACP Methods

### initialize

Return capabilities based on:

- Built-in support in this adapter.
- App-server feature availability.
- ACP client capabilities.

Advertise at least:

- `session/new`
- `session/load`
- `session/resume`
- `session/list`
- `session/close`
- `session/fork`
- `prompt`
- `cancel`
- session config options
- available commands

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

Use `thread/read` for history replay when the ACP client wants a passive load.
Use `thread/resume` when the ACP client wants an active thread that can accept
turns immediately.

The adapter should replay stored thread items into ACP updates only when the
ACP client expects history replay.

### session/resume

Map to:

```text
thread/resume
```

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
- title/name if available
- updated time if ACP supports it
- archived state if ACP supports it

### session/close

Preferred mapping:

```text
thread/unsubscribe
```

If the ACP request means "archive" or "delete", expose those as explicit slash
commands or future ACP methods, not as close.

### session/fork

Add first-class support.

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

Map `/fork` to `session/fork` for the current session. If the command includes
text, create an ephemeral side fork and immediately start a turn with that text
only if the client UX wants Codex TUI-like `/side` behavior.

### prompt

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

### cancel

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

- Agent message deltas -> ACP agent message chunks.
- Reasoning deltas -> ACP thought chunks when the client supports them.
- Command execution begin/end -> ACP tool call and tool call update.
- Command output deltas -> terminal output if supported, otherwise buffered
  tool call output.
- File edits -> ACP edit/tool call updates.
- Approval requests -> ACP permission requests.
- Tool user-input requests -> ACP user-input requests if supported, otherwise
  a clear error message.
- Turn completion -> ACP prompt response stop reason.

The adapter should keep a per-session active item map:

```text
app-server item id -> ACP tool call id / message stream id
```

## Slash Commands

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
| `/fork` | `thread/fork` |
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

### Discovery

On session start, resume, fork, and `skills/changed`, call:

```text
skills/list
```

Use the current session cwd. Cache the returned skills per cwd.

Expose skills to ACP clients as:

- available commands if ACP only supports slash commands
- mention completions if ACP supports mentions
- config options if ACP supports enable/disable toggles

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

Mappings:

- `model/list` -> model picker
- `permissionProfile/list` -> permissions picker
- `collaborationMode/list` -> mode picker
- `thread/settings/update` -> persisted next-turn setting changes

## Approval Flow

Approval routing should preserve app-server semantics.

When app-server emits a command approval request:

- translate it to an ACP permission request
- include command, cwd, reason, affected paths, and suggested actions
- preserve any "approve for session" or "remember this pattern" options when
  ACP can represent them
- send the user's decision back to app-server through the matching response
  method

Do not invent approval policies in the adapter. Policies should come from
Codex config, app-server thread settings, or explicit ACP session options.

## History and Replay

For session load/resume/fork, decide whether to replay history based on the ACP
method and client capabilities.

Rules:

- `session/resume` should usually return an active session without replaying
  every event unless the client requests transcript hydration.
- `session/load` may replay stored items for clients that need to render an
  existing transcript.
- `session/fork` should replay returned fork history unless `excludeTurns` is
  set.
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
- fork source session not loaded or not found

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
- `session/fork` request and response mapping.
- Approval option mapping.

### Integration Tests

Use a fake app-server JSON-RPC process first.

Scenarios:

- initialize
- new session
- prompt and stream final answer
- command tool call output
- approval request and approval response
- skills list and changed notification
- enable/disable skill
- fork session and prompt in fork
- cancel active turn

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
- fork session and continue independently
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
- [x] Implement `session/fork` via `thread/fork`.
- [x] Implement basic text `prompt` via `turn/start`.
- [ ] Add fake app-server integration tests.
- Implement cancellation via `turn/interrupt`.

### Phase 2: Event Translation

- Map agent messages, thoughts, command tool calls, file edits, and turn
  completion.
- Add active turn tracking.
- Add active item/tool-call tracking.
- Add buffered output fallback for clients without terminal streaming.

### Phase 3: Slash Commands

- Add command catalog generation.
- Implement backend commands:
  - `/review`
  - `/compact`
  - `/init`
  - `/rename`
  - `/new`
  - `/resume`
  - `/fork`
  - `/goal`
  - `/model`
  - `/permissions`
  - `/mcp`
  - `/apps`
  - `/plugins`
  - `/hooks`
  - `/status`

### Phase 4: Skills

- Implement `skills/list` refresh.
- Publish skills as ACP commands or mentions.
- Support `$skill-name` invocation.
- Implement enable/disable with `skills/config/write`.
- Handle `skills/changed`.
- Support `skills/extraRoots/set`.

### Phase 5: session/fork

- [x] Add ACP `session/fork` handler.
- [x] Map to app-server `thread/fork`.
- [x] Return the returned thread as a new ACP session.
- Replay fork history when requested.
- Route `/fork` through the same code path.
- Add tests for persistent and ephemeral forks.

### Phase 6: Catalogs and Advanced Surfaces

- Add model and reasoning effort config options.
- Add permission profile config options.
- Add apps/plugins/MCP commands.
- Add hooks display.
- Add background terminal list/clean.

### Phase 7: Hardening

- Version-gate app-server methods.
- Add compatibility handling for older Codex versions.
- Add structured logging.
- Add backpressure for notification bursts.
- Add shutdown cleanup for app-server child process.
- Add real app-server smoke tests.

## Open Questions

- Should `session/close` mean unsubscribe, archive, or no-op for Codex?
- Should `/fork <prompt>` create a persistent fork, or should that behavior be
  reserved for `/side <prompt>` as an ephemeral fork?
- How should skills appear in ACP clients that do not support mention
  completions?
- Which ACP clients can represent app/plugin icons, descriptions, and install
  states?
- Should the adapter use the installed `codex` binary or link app-server crates
  directly in-process?

## Recommended First Commit

The first implementation commit should be intentionally small:

- replace the hello-world binary with a real CLI entrypoint
- add an app-server JSON-RPC client module
- add a session manager module
- implement initialize and app-server handshake
- add a fake app-server integration test

Do not start by porting `codex-acp` wholesale. Use it as a reference for ACP
event shapes and client compatibility, but keep the new adapter centered on
`codex app-server`.
