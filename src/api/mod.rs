// HTTP API server subcommand. Phase 2 will flesh this out with routes, auth,
// refresh orchestration, etc. This file gives the binary something to link
// against so Phase 1's refactor is verifiable in isolation.

use std::path::PathBuf;

use clap::Args;

pub mod auth;
pub mod embed_assets;
pub mod handlers;
pub mod routes;
pub mod state;

#[derive(Args, Debug, Clone)]
pub struct ServeArgs {
    /// Address to bind (e.g. 127.0.0.1:8080 or 0.0.0.0:8080)
    #[arg(long, env = "CONTABO_BIND", default_value = "127.0.0.1:8080")]
    pub bind: String,

    /// Directory where snapshot JSON/CSV live; watched for hot-reload
    #[arg(long, env = "CONTABO_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Bearer token required for mutating endpoints (POST /refresh). When absent
    /// these endpoints are disabled rather than open.
    #[arg(long, env = "CONTABO_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Path to a file containing the bearer token (alternative to --auth-token)
    #[arg(long, env = "CONTABO_AUTH_TOKEN_FILE")]
    pub auth_token_file: Option<PathBuf>,

    /// CORS allow-origin (repeatable; defaults to none = same-origin only)
    #[arg(long = "cors-origin")]
    pub cors_origins: Vec<String>,

    /// Cron expression for periodic refresh (6 fields; sec min hour day mon dow).
    /// Disabled by default; suggested production value: "0 0 */6 * * *" (every 6h).
    #[arg(long, env = "CONTABO_REFRESH_CRON")]
    pub refresh_cron: Option<String>,

    /// Override the scraper command used by /refresh. Default = self (Rust).
    /// Example: --scraper-cmd "node /app/scripts/contabo_scraper.js"
    #[arg(long, env = "CONTABO_SCRAPER_CMD")]
    pub scraper_cmd: Option<String>,
}

pub async fn run_serve(args: ServeArgs) -> i32 {
    match serve_inner(args).await {
        Ok(()) => 0,
        Err(e) => {
            tracing::error!("server error: {e:#}");
            1
        }
    }
}

async fn serve_inner(args: ServeArgs) -> anyhow::Result<()> {
    let app_state = state::AppState::new(&args).await?;
    let router = routes::build_router(app_state.clone(), &args);

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    tracing::info!("listening on http://{}", args.bind);
    tracing::info!("data_dir = {}", app_state.data_dir.display());

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        sig.recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
    tracing::info!("shutdown signal received");
}
