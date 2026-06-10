// Test harness for spinning up the contabo-scraper HTTP API on an ephemeral
// port. Each test gets a fresh server bound to 127.0.0.1:0 so concurrent
// `cargo test` jobs never clash. Shutdown is driven by a oneshot channel so
// there's no `sleep`-based teardown.

#![allow(dead_code)]

pub mod golden;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::api::state::AppState;
use crate::api::{routes, ServeArgs};

pub struct TestServer {
    pub addr: SocketAddr,
    pub data_dir: PathBuf,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl TestServer {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(j) = self.join.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), j).await;
        }
    }

    pub fn base(&self) -> String {
        format!("http://{}", self.addr)
    }
}

pub fn data_dir() -> PathBuf {
    // Workspace root / data / output. CARGO_MANIFEST_DIR points at the crate.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join("output")
}

pub fn fixture_present() -> bool {
    data_dir().join("contabo_view_model.json").exists()
        && data_dir().join("contabo_base_plans.json").exists()
}

/// Spawn a server on an ephemeral port, optionally with a bearer token wired
/// into `--auth-token`. Returns once the server has answered a /health probe.
pub async fn spawn_server(auth_token: Option<&str>) -> TestServer {
    let data = data_dir();
    let args = ServeArgs {
        bind: "127.0.0.1:0".into(), // unused — we bind ourselves below
        data_dir: Some(data.clone()),
        auth_token: auth_token.map(|s| s.to_string()),
        auth_token_file: None,
        cors_origins: vec![],
    };

    let state = AppState::new(&args).await.expect("AppState::new failed");
    let router = routes::build_router(state.clone(), &args);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    // Poll /health for up to 5s. Deterministic — no sleep loops without bound.
    let health_url = format!("http://{}/api/v1/health", addr);
    let client = reqwest::Client::new();
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client.get(&health_url).send().await {
                Ok(r) if r.status().is_success() => break true,
                _ => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await;
    assert!(
        ready.is_ok(),
        "server didn't become ready within 5s on {addr}"
    );

    TestServer {
        addr,
        data_dir: data,
        shutdown_tx: Some(shutdown_tx),
        join: Some(join),
    }
}
