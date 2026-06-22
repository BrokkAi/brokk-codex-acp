# Brokk Codex ACP

> ⚠️ **Alpha — work in progress, expect a hot mess.** This is early,
> incomplete, and unstable. Core features are missing, APIs and behavior will
> change without notice, and things will break. It is **not** ready for
> production use. Use at your own risk, pin a specific version, and don't be
> surprised by sharp edges. Feedback and issues are welcome.

Brokk Codex ACP is an Agent Client Protocol server for Codex.

The project is intentionally built around `codex app-server` instead of linking
directly against unstable Codex internals. The ACP server should stay thin:
translate ACP requests and notifications to the app-server protocol, then let
Codex own threads, turns, skills, tools, apps, plugins, MCP, models, and
permissions.

See [PLANS.md](PLANS.md) for the implementation plan.

## Status

**Alpha / pre-0.1 in spirit.** This is an early, rough adapter under active
construction. Large parts of the ACP surface are stubbed or unimplemented, error
handling is thin, there is little test coverage, and breaking changes should be
expected on every release until this stabilizes.

The project currently includes:

- Cargo package metadata.
- GPL-3.0-or-later licensing metadata.
- A CLI with `probe` and `serve` commands.
- A JSON-RPC client that can spawn and initialize `codex app-server --stdio`.
- An ACP serving loop backed by `agent-client-protocol`.
- ACP handlers for `initialize`, `session/new`, `session/load`,
  `session/resume`, `session/list`, `session/close`, `session/delete`,
  `session/fork`, `session/prompt`, and `session/cancel`.
- `session/fork` is exposed through the ACP Rust crate's unstable RFD/extension
  feature, not as stable ACP v1 behavior.
- `session/list` preserves app-server thread preview, status, recency, model
  provider, agent, and parent-thread metadata under `_meta.brokk_codex_acp`.
- Initial prompt streaming from Codex `item/agentMessage/delta` notifications to
  ACP agent message chunks.
- Initial turn event projection for reasoning chunks, command/file/tool item
  lifecycles, command output, plan updates, turn diffs, and usage updates.
- Initial skills discovery through app-server `skills/list`, projected as ACP
  available commands for enabled skills alongside adapter-owned commands.
- Structured `$skill-name` and `/skill skill-name` invocation when app-server
  returns a skill path, with plain-text fallback otherwise.
- App-server `skills/config/write` mapping for skill enable/disable state,
  exposed through ACP session config options named `skill:<name>`.
- App-server `skills/extraRoots/set` mapping through `/skill-roots <paths...>`;
  roots are process-local and not persisted by Codex app-server.
- Initial slash-command routing for `/rename <title>`, `/archive`, `/goal`,
  `/compact`, `/review`, `/init`, `/new`, `/resume`, `/fork`, `/apps`,
  `/plugins`, `/mcp`, `/hooks`, `/model`, `/permissions`, `/ps`,
  `/skill-roots`, `/stop`, and `/status`, mapped to app-server thread, review,
  catalog, and config endpoints with ACP session update, turn-stream,
  config-option, or
  agent-message summary projection as appropriate. Unsupported leading slash
  commands return an explicit ACP error instead of being forwarded to the model.
- Initial command, file-change, and permission-profile approval routing from
  app-server approval requests to ACP `session/request_permission`, with the
  selected decision sent back to app-server in the response shape each
  app-server request expects. Rich command approval decisions such as
  exec-policy and network-policy amendments keep their original app-server
  payload and are returned unchanged when selected.
- Server-initiated `currentTime/read` requests are answered with the adapter
  host's current Unix timestamp so external-clock reminders do not block turns.
- Unsupported server-initiated app-server requests receive explicit JSON-RPC
  method-not-found errors instead of being silently ignored.
- Thread-scoped MCP server startup status notifications are projected as
  user-visible ACP diagnostic messages.
- Global app-server configuration warnings are projected as user-visible ACP
  diagnostic messages for every known session.
- Global Windows sandbox setup completion notifications are projected as
  user-visible ACP diagnostic messages for every known session.
- Global account, rate-limit, and MCP OAuth login notifications are projected
  as user-visible ACP diagnostic messages for every known session.
- Fuzzy file search session progress and completion notifications are
  projected as compact user-visible ACP diagnostic messages.
- Model verification notifications are projected as user-visible ACP
  diagnostic messages.
- Turn moderation metadata notifications are projected as user-visible ACP
  diagnostic messages.
- Thread realtime lifecycle, SDP, raw item, transcript text, and output-audio
  notifications are projected as user-visible ACP diagnostic messages. Audio
  payloads are summarized because ACP v1 has no native realtime audio stream.
- Initial ACP session config options for `model`, `reasoning_effort`,
  `service_tier`, `approval_policy`, `collaboration_mode`, and
  `permission_profile`, populated from app-server `model/list`,
  `collaborationMode/list`, and `permissionProfile/list`, with writes routed
  through `thread/settings/update`.
- Background app-server response/notification dispatch, including refresh of
  skill commands, session titles, and session config options when
  `skills/changed`, `thread/name/updated`, `thread/archived`,
  `thread/status/changed`, `thread/goal/updated`, `thread/goal/cleared`, or
  `thread/settings/updated` notifications are observed.

The adapter is not complete yet. Rich ACP UI for MCP elicitations, dynamic tool
callbacks, and user-input requests, exact terminal embedding, native realtime
audio playback, remaining slash command routing, plugin install/read actions,
direct MCP resource/tool UI, fine-grained partial permission grants, and
paginated/full-fidelity history replay are still planned work.

## Usage

Verify that Codex app-server can start:

```shell
cargo run -- probe
```

Use a custom Codex binary:

```shell
cargo run -- probe --codex-bin /path/to/codex
```

Start the ACP server:

```shell
cargo run -- serve
```

## Development

```shell
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

Run the opt-in smoke test against a real local Codex app-server:

```shell
BROKK_CODEX_ACP_SMOKE_CODEX_BIN=/path/to/codex \
  cargo test --test real_app_server_smoke -- --ignored
```

## Release Strategy

Releases are cut from `v*.*.*` tags. The GitHub release workflow verifies that
the tag version matches `Cargo.toml`, builds archives, and uploads SHA-256
checksums for:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `x86_64-pc-windows-msvc`
- `universal-apple-darwin`

Android is intentionally not part of the required release matrix for now
because Codex itself does not currently compile for Android.

The crate publishing workflow follows the same tag discipline. On tag pushes it
verifies the `Cargo.toml` version, runs `cargo publish --dry-run`, authenticates
with crates.io trusted publishing, and publishes `brokk-codex-acp`. Manual
workflow runs default to dry-run mode unless `publish` is explicitly enabled.

Before the first publish, configure crates.io trusted publishing for this
repository, the `.github/workflows/publish-crate.yml` workflow, and the
`release` GitHub environment.

## License

This project is licensed under GPL-3.0-or-later.
