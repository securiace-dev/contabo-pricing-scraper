// Integration test that locks down every HTTP endpoint exposed by
// `contabo-scraper serve`. The api module is pulled in via #[path] so we
// don't need a `lib.rs` in the bin crate; the stubs below stand in for the
// few `crate::*` items the api module references at runtime.

#![allow(dead_code)]

use std::path::PathBuf;
use std::time::Duration;

// ── Stubs the api module expects at the crate root ──────────────────────────
pub const VERSION: &str = "test-2.3.0";
pub const SCHEMA_VERSION: &str = "1.1";

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum FetchMode {
    Reqwest,
    Auto,
    Cloak,
}

#[derive(Clone)]
pub struct Opts {
    pub output: PathBuf,
    pub concurrency: usize,
    pub retries: u32,
    pub plans: Option<Vec<String>>,
    pub plan_urls_file: Option<PathBuf>,
    pub quiet: bool,
    pub json_out: bool,
    pub dry_run: bool,
    pub fetch_mode: FetchMode,
    pub cloak_script: PathBuf,
    pub proxy: Option<String>,
}

// The /refresh handler invokes this from a spawned task. Tests never await
// it; we just need a function with the right shape that compiles and won't
// touch the network if it does run.
pub async fn run_scrape(_opts: Opts) -> i32 {
    0
}

// ── Pull in the production api module from src/api/ ─────────────────────────
#[path = "../src/api/mod.rs"]
mod api;

mod common;

// ────────────────────────────────────────────────────────────────────────────
//  Helpers
// ────────────────────────────────────────────────────────────────────────────

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

// ────────────────────────────────────────────────────────────────────────────
//  Open endpoints
// ────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let h = common::spawn_server(None).await;
    let res = client()
        .get(format!("{}/api/v1/health", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    assert!(
        body["version"].is_string(),
        "version field missing/wrong type"
    );
    assert!(
        body["schema_version"].is_string(),
        "schema_version field missing/wrong type"
    );
    h.shutdown().await;
}

#[tokio::test]
async fn meta_endpoint_returns_expected_keys() {
    let h = common::spawn_server(None).await;
    let res = client()
        .get(format!("{}/api/v1/meta", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert!(
        body.get("scraper_version").is_some(),
        "missing scraper_version"
    );
    assert!(body.get("snapshot_meta").is_some(), "missing snapshot_meta");
    assert!(body.get("data_dir").is_some(), "missing data_dir");
    h.shutdown().await;
}

#[tokio::test]
async fn plans_endpoint_returns_array() {
    if !common::fixture_present() {
        eprintln!("skip plans_endpoint_returns_array — data/output/ missing");
        return;
    }
    let h = common::spawn_server(None).await;
    let res = client()
        .get(format!("{}/api/v1/plans", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    let arr = body.as_array().expect("plans is array");
    assert!(!arr.is_empty(), "expected at least one plan in fixture");
    h.shutdown().await;
}

#[tokio::test]
async fn catalog_endpoint_returns_versioned_items() {
    if !common::fixture_present() {
        eprintln!("skip catalog_endpoint_returns_versioned_items — data/output/ missing");
        return;
    }
    let h = common::spawn_server(None).await;
    let res = client()
        .get(format!("{}/api/v1/catalog", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert!(body["catalog_version"]
        .as_str()
        .unwrap_or("")
        .starts_with("catalog-"));
    assert_eq!(body["payload_hash"].as_str().unwrap_or("").len(), 64);
    let items = body["items"].as_array().expect("catalog items is array");
    assert!(!items.is_empty(), "catalog must contain plan items");
    for item in items {
        assert!(item["machine_id"].is_string());
        assert_eq!(item["catalog_version"], body["catalog_version"]);
        assert!(item["source_observed_at"].is_string());
        assert_eq!(item["payload_hash"].as_str().unwrap_or("").len(), 64);
    }
    h.shutdown().await;
}

#[tokio::test]
async fn plans_endpoint_filters_by_family() {
    if !common::fixture_present() {
        eprintln!("skip plans_endpoint_filters_by_family — fixture missing");
        return;
    }
    let h = common::spawn_server(None).await;
    let base = h.base();
    let all: serde_json::Value = client()
        .get(format!("{}/api/v1/plans", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let filtered: serde_json::Value = client()
        .get(format!("{}/api/v1/plans?family=Cloud%20VPS", base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let all_n = all.as_array().unwrap().len();
    let filt_n = filtered.as_array().unwrap().len();
    assert!(filt_n > 0, "filtered list should have ≥1 Cloud VPS plan");
    assert!(filt_n <= all_n, "filtered subset must be ≤ all plans");
    for p in filtered.as_array().unwrap() {
        assert_eq!(
            p["family"], "Cloud VPS",
            "filter leaked a non-Cloud VPS plan"
        );
    }
    h.shutdown().await;
}

#[tokio::test]
async fn plan_by_slug_returns_object() {
    if !common::fixture_present() {
        eprintln!("skip plan_by_slug_returns_object — fixture missing");
        return;
    }
    let h = common::spawn_server(None).await;
    let res = client()
        .get(format!("{}/api/v1/plans/cloud-vps-10", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["product_slug"], "cloud-vps-10");
    h.shutdown().await;
}

#[tokio::test]
async fn plan_by_bogus_slug_returns_404() {
    let h = common::spawn_server(None).await;
    let res = client()
        .get(format!("{}/api/v1/plans/bogus", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404);
    h.shutdown().await;
}

#[tokio::test]
async fn options_endpoint_returns_200() {
    let h = common::spawn_server(None).await;
    let res = client()
        .get(format!("{}/api/v1/options", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    h.shutdown().await;
}

#[tokio::test]
async fn fx_endpoint_returns_object() {
    let h = common::spawn_server(None).await;
    let res = client()
        .get(format!("{}/api/v1/fx", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert!(body.is_object(), "fx response should be a JSON object");
    h.shutdown().await;
}

#[tokio::test]
async fn openapi_endpoint_returns_json() {
    let h = common::spawn_server(None).await;
    let res = client()
        .get(format!("{}/api/v1/openapi.json", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert!(body.is_object(), "openapi response should be a JSON object");
    h.shutdown().await;
}

#[tokio::test]
async fn index_endpoint_returns_embedded_html() {
    let h = common::spawn_server(None).await;
    let res = client().get(format!("{}/", h.base())).send().await.unwrap();
    assert_eq!(res.status(), 200);
    let ct = res
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.contains("text/html"),
        "expected HTML content-type, got {ct}"
    );
    let body = res.bytes().await.unwrap();
    assert!(
        body.len() > 100_000,
        "embedded report.html should be >100 KB, got {} bytes",
        body.len()
    );
    h.shutdown().await;
}

// ────────────────────────────────────────────────────────────────────────────
//  Quote (price math)
// ────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn quote_endpoint_with_gst_and_inr_fx() {
    if !common::fixture_present() {
        eprintln!("skip quote_endpoint_with_gst_and_inr_fx — fixture missing");
        return;
    }
    let h = common::spawn_server(None).await;
    let body = serde_json::json!({
        "plan_slug": "cloud-vps-10",
        "period_months": 12,
        "selections": {},
        "currency": "INR",
        "gst": true,
        "fx_markup": 0.035,
        "fx_rate": 112.317
    });
    let res = client()
        .post(format!("{}/api/v1/quote", h.base()))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let json: serde_json::Value = res.json().await.unwrap();

    let expected = 3.60_f64 * 1.18 * 112.317 * 1.035;
    let got = json["final_monthly"]
        .as_f64()
        .expect("final_monthly missing");
    assert!(
        (got - expected).abs() < 0.01,
        "final_monthly: expected {expected:.4}, got {got:.4}"
    );
    assert_eq!(json["currency"], "INR");

    let breakdown = json["breakdown"].as_array().expect("breakdown is array");
    let joined = breakdown
        .iter()
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(
        joined.contains("GST"),
        "breakdown should mention GST; got {joined}"
    );
    assert!(
        joined.contains("EUR→INR"),
        "breakdown should mention EUR→INR; got {joined}"
    );
    h.shutdown().await;
}

#[tokio::test]
async fn quote_endpoint_without_gst() {
    if !common::fixture_present() {
        eprintln!("skip quote_endpoint_without_gst — fixture missing");
        return;
    }
    let h = common::spawn_server(None).await;
    let body = serde_json::json!({
        "plan_slug": "cloud-vps-10",
        "period_months": 12,
        "selections": {},
        "currency": "INR",
        "gst": false,
        "fx_markup": 0.035,
        "fx_rate": 112.317
    });
    let res = client()
        .post(format!("{}/api/v1/quote", h.base()))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let json: serde_json::Value = res.json().await.unwrap();

    let expected = 3.60_f64 * 112.317 * 1.035; // no GST
    let got = json["final_monthly"]
        .as_f64()
        .expect("final_monthly missing");
    assert!(
        (got - expected).abs() < 0.01,
        "final_monthly (no GST): expected {expected:.4}, got {got:.4}"
    );
    h.shutdown().await;
}

// ────────────────────────────────────────────────────────────────────────────
//  Refresh auth
// ────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn refresh_returns_503_when_no_auth_token_configured() {
    let h = common::spawn_server(None).await;
    let res = client()
        .post(format!("{}/api/v1/refresh", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 503);
    h.shutdown().await;
}

#[tokio::test]
async fn refresh_returns_401_when_token_set_and_no_header() {
    let h = common::spawn_server(Some("secret-token")).await;
    let res = client()
        .post(format!("{}/api/v1/refresh", h.base()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
    h.shutdown().await;
}

#[tokio::test]
async fn refresh_returns_401_on_bad_token() {
    let h = common::spawn_server(Some("secret-token")).await;
    let res = client()
        .post(format!("{}/api/v1/refresh", h.base()))
        .header("Authorization", "Bearer wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
    h.shutdown().await;
}

#[tokio::test]
async fn refresh_accepts_good_token_and_returns_job_id() {
    let h = common::spawn_server(Some("secret-token")).await;
    let res = client()
        .post(format!("{}/api/v1/refresh", h.base()))
        .header("Authorization", "Bearer secret-token")
        .send()
        .await
        .unwrap();
    // 202 Accepted: the scrape runs asynchronously; the response only confirms
    // the job was queued, not that it has completed.
    assert_eq!(
        res.status(),
        202,
        "expected 202 Accepted, got {}",
        res.status()
    );
    let body: serde_json::Value = res.json().await.unwrap();
    let job_id = body["job_id"].as_str().expect("job_id missing");
    assert!(!job_id.is_empty(), "job_id should be non-empty");
    h.shutdown().await;
}
