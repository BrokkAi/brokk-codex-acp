# Brokk Codex ACP

Brokk Codex ACP is an Agent Client Protocol server for Codex.

The project is intentionally built around `codex app-server` instead of linking
directly against unstable Codex internals. The ACP server should stay thin:
translate ACP requests and notifications to the app-server protocol, then let
Codex own threads, turns, skills, tools, apps, plugins, MCP, models, and
permissions.

See [PLANS.md](PLANS.md) for the implementation plan.

## Status

Early bootstrap. The project currently includes:

- Cargo package metadata.
- GPL-3.0-or-later licensing metadata.
- A CLI skeleton.
- A minimal JSON-RPC client that can spawn and initialize `codex app-server --stdio`.

The ACP serving loop is not implemented yet.

## Usage

Verify that Codex app-server can start:

```shell
cargo run -- probe
```

Use a custom Codex binary:

```shell
cargo run -- probe --codex-bin /path/to/codex
```

Start the placeholder serve command:

```shell
cargo run -- serve
```

## Development

```shell
cargo fmt
cargo check
```

## License

This project is licensed under GPL-3.0-or-later.
