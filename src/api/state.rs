// In-memory snapshot held behind an RwLock; refresh swaps it atomically.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::RwLock;

use super::auth::AuthConfig;
use super::ServeArgs;

#[derive(Clone)]
pub struct AppState {
    pub data_dir: PathBuf,
    pub snapshot: Arc<RwLock<Snapshot>>,
    pub refresh_lock: Arc<tokio::sync::Mutex<RefreshState>>,
    pub auth: Arc<AuthConfig>,
    pub http_client: reqwest::Client,
    pub fx_cache: Arc<RwLock<FxCacheState>>,
    pub proposal_jobs: Arc<RwLock<HashMap<String, Value>>>,
    pub proposal_local_only: bool,
}

#[derive(Default)]
pub struct FxCacheState {
    pub data: Option<serde_json::Value>,
    pub fetched_at: Option<std::time::Instant>,
}

#[derive(Default, Clone)]
pub struct Snapshot {
    pub meta: Value,
    pub view_model: Value,
    pub plans: Value,
    pub option_catalog: Value,
    #[allow(dead_code)]
    pub quick_reference: Value,
    pub configs: Value,
}

#[derive(Default)]
pub struct RefreshState {
    pub current_job: Option<RefreshJob>,
}

#[derive(Clone)]
pub struct RefreshJob {
    pub id: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub status: JobStatus,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    /// Some plans succeeded, others failed (EXIT_PARTIAL).
    PartialSuccess,
    Failed,
}

impl AppState {
    pub async fn new(args: &ServeArgs) -> anyhow::Result<Self> {
        let data_dir = args
            .data_dir
            .clone()
            .or_else(|| {
                std::env::current_dir()
                    .ok()
                    .map(|d| d.join("data").join("output"))
            })
            .unwrap_or_else(|| PathBuf::from("data/output"));

        let snapshot = load_snapshot_from_disk(&data_dir).unwrap_or_default();
        let auth = Arc::new(AuthConfig::from_args(args)?);
        Ok(Self {
            data_dir,
            snapshot: Arc::new(RwLock::new(snapshot)),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(RefreshState::default())),
            auth,
            http_client: reqwest::Client::new(),
            fx_cache: Arc::new(RwLock::new(FxCacheState::default())),
            proposal_jobs: Arc::new(RwLock::new(HashMap::new())),
            proposal_local_only: is_loopback_bind(&args.bind),
        })
    }
}

fn is_loopback_bind(bind: &str) -> bool {
    let host = bind.rsplit_once(':').map(|(host, _)| host).unwrap_or(bind);
    let host = host.trim_matches(['[', ']']);
    host == "localhost" || host == "::1" || host == "127.0.0.1" || host.starts_with("127.")
}

pub(super) fn load_snapshot_from_disk(dir: &std::path::Path) -> anyhow::Result<Snapshot> {
    fn read(p: std::path::PathBuf) -> Value {
        std::fs::read_to_string(&p)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(Value::Null)
    }
    let view_model = read(dir.join("contabo_view_model.json"));
    let plans = read(dir.join("contabo_base_plans.json"));
    let option_catalog = read(dir.join("contabo_pricing_dataset.json"));
    let quick_reference = read(dir.join("contabo_quick_reference.json"));
    let configs = read(dir.join("contabo_configs.json"));
    let meta = view_model.get("meta").cloned().unwrap_or(Value::Null);
    Ok(Snapshot {
        meta,
        view_model,
        plans,
        option_catalog,
        quick_reference,
        configs,
    })
}
