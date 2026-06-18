use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct Cli {
    #[arg(long, env = "RUST_LOG", default_value = "brokk_codex_acp=info")]
    pub log_filter: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start the ACP server.
    Serve(ServeArgs),

    /// Verify that the configured Codex app-server can start and initialize.
    Probe(ProbeArgs),
}

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Path to the Codex executable.
    #[arg(long, env = "CODEX_BIN", default_value = "codex")]
    pub codex_bin: PathBuf,
}

#[derive(Debug, Args)]
pub struct ProbeArgs {
    /// Path to the Codex executable.
    #[arg(long, env = "CODEX_BIN", default_value = "codex")]
    pub codex_bin: PathBuf,
}
