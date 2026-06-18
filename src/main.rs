use anyhow::Context;
use brokk_codex_acp::agent::CodexAcpAgent;
use brokk_codex_acp::app_server::{AppServerClient, AppServerCommand};
use brokk_codex_acp::cli::{Cli, Command};
use clap::Parser;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_filter)?;

    match cli.command {
        Command::Probe(args) => {
            let command = AppServerCommand::new(args.codex_bin);
            let mut client = AppServerClient::spawn(command)
                .await
                .context("failed to spawn codex app-server")?;

            let response = client
                .initialize(
                    "brokk_codex_acp",
                    "Brokk Codex ACP",
                    env!("CARGO_PKG_VERSION"),
                )
                .await
                .context("failed to initialize codex app-server")?;

            info!(
                user_agent = response.user_agent,
                codex_home = response.codex_home,
                platform_family = response.platform_family,
                platform_os = response.platform_os,
                "codex app-server probe succeeded"
            );
        }
        Command::Serve(args) => {
            let command = AppServerCommand::new(args.codex_bin);
            let mut client = AppServerClient::spawn(command)
                .await
                .context("failed to spawn codex app-server")?;

            client
                .initialize(
                    "brokk_codex_acp",
                    "Brokk Codex ACP",
                    env!("CARGO_PKG_VERSION"),
                )
                .await
                .context("failed to initialize codex app-server")?;

            CodexAcpAgent::new(client)
                .serve_stdio()
                .await
                .map_err(|error| anyhow::anyhow!("ACP server failed: {error}"))?;
        }
    }

    Ok(())
}

fn init_tracing(filter: &str) -> anyhow::Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_new(filter)
        .with_context(|| format!("invalid log filter `{filter}`"))?;

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    Ok(())
}
