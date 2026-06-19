use std::{env, path::PathBuf};

use brokk_codex_acp::app_server::{AppServerClient, AppServerCommand};

#[tokio::test]
#[ignore = "requires a real Codex binary configured for app-server smoke testing"]
async fn real_codex_app_server_initializes_and_serves_basic_catalogs() -> anyhow::Result<()> {
    let codex_bin = env::var_os("BROKK_CODEX_ACP_SMOKE_CODEX_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("codex"));
    let mut client = AppServerClient::spawn(AppServerCommand::new(codex_bin)).await?;

    let initialize = client
        .initialize(
            "brokk_codex_acp_smoke",
            "Brokk Codex ACP Smoke",
            env!("CARGO_PKG_VERSION"),
        )
        .await?;
    assert!(!initialize.user_agent.trim().is_empty());
    assert!(!initialize.codex_home.trim().is_empty());
    assert!(!initialize.platform_family.trim().is_empty());
    assert!(!initialize.platform_os.trim().is_empty());

    let models = client.model_list().await?;
    assert!(
        !models.data.is_empty(),
        "real codex app-server returned no models"
    );

    let collaboration_modes = client.collaboration_mode_list().await?;
    assert!(
        !collaboration_modes.data.is_empty(),
        "real codex app-server returned no collaboration modes"
    );

    Ok(())
}
