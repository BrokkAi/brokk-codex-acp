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
- App-server `skills/config/write` mapping for skill enable/disable state.
- Initial slash-command routing for `/rename <title>`, `/archive`, `/goal`,
  `/compact`, `/review`, `/new`, `/resume`, `/fork`, `/apps`, `/plugins`,
  `/mcp`, `/hooks`, `/model`, `/permissions`, and `/status`, mapped to
  app-server thread, review, catalog, and config endpoints with ACP session
  update, turn-stream, config-option, or agent-message summary projection as
  appropriate.
- Initial command, file-change, and permission-profile approval routing from
  app-server approval requests to ACP `session/request_permission`, with the
  selected decision sent back to app-server in the response shape each
  app-server request expects.
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
callbacks, and user-input requests, exact terminal embedding, remaining slash
command routing, plugin install/read actions, direct MCP resource/tool UI,
fine-grained partial permission grants, skill enable/disable config options,
and paginated/full-fidelity history replay are still planned work.

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
cargo check
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
