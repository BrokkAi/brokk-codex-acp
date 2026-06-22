use std::{env, path::PathBuf};

use brokk_codex_acp::app_server::{AppServerClient, AppServerCommand};

async fn real_app_server_client() -> anyhow::Result<AppServerClient> {
    let codex_bin = env::var_os("BROKK_CODEX_ACP_SMOKE_CODEX_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("codex"));
    let mut client = AppServerClient::spawn(AppServerCommand::new(codex_bin)).await?;
    client
        .initialize(
            "brokk_codex_acp_smoke",
            "Brokk Codex ACP Smoke",
            env!("CARGO_PKG_VERSION"),
        )
        .await?;
    Ok(client)
}

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

#[tokio::test]
#[ignore = "requires a real Codex binary configured for app-server smoke testing"]
async fn real_codex_app_server_supports_thread_lifecycle() -> anyhow::Result<()> {
    let mut client = real_app_server_client().await?;
    let temp_dir = tempfile::tempdir()?;
    let cwd = temp_dir.path().canonicalize()?;
    let cwd_string = cwd.to_string_lossy().into_owned();

    let started = client.thread_start(cwd_string.clone(), None).await?;
    let thread_id = started.thread.id.clone();
    assert!(
        !thread_id.trim().is_empty(),
        "real codex app-server returned an empty thread id"
    );
    assert_eq!(started.thread.cwd.as_deref(), Some(cwd.as_path()));

    let read = client.thread_read(thread_id.clone()).await?;
    assert_eq!(read.thread.id, thread_id);
    assert_eq!(read.thread.cwd.as_deref(), Some(cwd.as_path()));

    let page = client
        .thread_turns_list(thread_id.clone(), None, 10)
        .await?;
    assert!(
        page.data.is_empty(),
        "fresh real codex app-server thread should not have turns"
    );

    let listed = client.thread_list(Some(cwd_string.clone()), None).await?;
    assert!(
        listed.data.iter().any(|thread| thread.id == thread_id),
        "real codex app-server did not list the started thread for its cwd"
    );

    let resumed = client
        .thread_resume(thread_id.clone(), cwd_string, None)
        .await?;
    assert_eq!(resumed.thread.id, thread_id);
    assert!(
        resumed.thread.turns.is_empty(),
        "adapter resume path requests excludeTurns and should not receive turns"
    );

    let unsubscribe = client.thread_unsubscribe(thread_id.clone()).await?;
    assert!(
        matches!(
            unsubscribe.status.as_str(),
            "unsubscribed" | "notSubscribed"
        ),
        "unexpected thread/unsubscribe status: {}",
        unsubscribe.status
    );

    client.thread_delete(thread_id).await?;

    Ok(())
}
