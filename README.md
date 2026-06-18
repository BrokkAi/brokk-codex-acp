# Brokk Codex ACP

Brokk Codex ACP is an Agent Client Protocol server for Codex.

The project is intentionally built around `codex app-server` instead of linking
directly against unstable Codex internals. The ACP server should stay thin:
translate ACP requests and notifications to the app-server protocol, then let
Codex own threads, turns, skills, tools, apps, plugins, MCP, models, and
permissions.

See [PLANS.md](PLANS.md) for the implementation plan.

## Status

Early adapter implementation. The project currently includes:

- Cargo package metadata.
- GPL-3.0-or-later licensing metadata.
- A CLI with `probe` and `serve` commands.
- A JSON-RPC client that can spawn and initialize `codex app-server --stdio`.
- An ACP serving loop backed by `agent-client-protocol`.
- ACP handlers for `initialize`, `session/new`, `session/resume`,
  `session/list`, `session/close`, `session/fork`, `session/prompt`, and
  `session/cancel`.
- Initial prompt streaming from Codex `item/agentMessage/delta` notifications to
  ACP agent message chunks.

The adapter is not complete yet. Tool calls, approvals, command output,
reasoning chunks, slash command routing, skills catalogs, and history replay are
still planned work.

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
