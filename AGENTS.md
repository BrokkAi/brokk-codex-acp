# Repository Instructions

This repository implements an Agent Client Protocol server for Codex.

## Language

- Write repository files, code, comments, commit messages, and documentation in English.
- User-facing chat outside the repository may be in the user's preferred language.

## Architecture

- Keep the adapter centered on `codex app-server`.
- Do not copy Codex TUI logic wholesale.
- Treat Codex app-server as the owner of threads, turns, skills, tools, approvals, plugins, apps, MCP, models, and permission profiles.
- Keep ACP-specific code in the adapter layer.
- Prefer small modules with explicit protocol boundary types.

## Development

- Run `cargo fmt` before finishing Rust changes.
- Run `cargo check` when dependencies are available.
- Keep new behavior covered by focused unit or integration tests once the core transport is in place.
- Do not add broad abstractions until a second real call site needs them.

## Licensing

- New files must be compatible with GPL-3.0-or-later.
- Keep SPDX identifiers on source files if license headers are added later.
