// Executable producer contract for the Rust API and its WHMCS PHP consumer.
// The fixtures are deliberately minimal, complete, and always required.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use axum::extract::{Query, State};
use axum::Json;
use serde_json::{json, Value};

pub const VERSION: &str = "test-2.3.0";
pub const SCHEMA_VERSION: &str = "1.2";

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

pub async fn run_scrape(_opts: Opts) -> i32 {
    0
}

#[path = "../src/api/mod.rs"]
mod api;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("api")
        .join(format!("v{SCHEMA_VERSION}"))
}

fn load_fixture(name: &str) -> Value {
    let path = fixture_dir().join(name);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("required contract fixture {}: {error}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("valid JSON contract fixture {}: {error}", path.display()))
}

fn write_json(path: &Path, value: &Value) {
    let bytes = serde_json::to_vec_pretty(value).expect("fixture snapshot serializes");
    std::fs::write(path, bytes)
        .unwrap_or_else(|error| panic!("write deterministic snapshot {}: {error}", path.display()));
}

fn expected_fixture(name: &str, actual: &Value) -> Value {
    if std::env::var("UPDATE_SCHEMA_FIXTURES").as_deref() == Ok("1") {
        write_json(&fixture_dir().join(name), actual);
    }
    load_fixture(name)
}

fn create_snapshot_dir() -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("contabo-schema-contract-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("create deterministic snapshot directory");

    let plans = load_fixture("plans.json");
    write_json(
        &dir.join("contabo_view_model.json"),
        &json!({
            "meta": {
                "scraper_version": "fixture-1.0.0",
                "schema_version": SCHEMA_VERSION,
                "generated_at": "2026-07-30T10:20:30Z",
                "plan_count": 1,
                "row_count": 1,
            },
            "rows": [{
                "plan_slug": "cloud-vps-core-4",
                "period_months": 12,
                "effective_monthly": 5.5,
                "setup_fee": 0.0,
            }],
        }),
    );
    write_json(
        &dir.join("contabo_base_plans.json"),
        &json!({
            "schema_version": SCHEMA_VERSION,
            "scraper_version": "fixture-1.0.0",
            "generated_at": "2026-07-30T10:20:30Z",
            "plan_count": 1,
            "plans": plans,
        }),
    );
    write_json(
        &dir.join("contabo_pricing_dataset.json"),
        &json!({ "option_catalog": [] }),
    );
    write_json(&dir.join("contabo_quick_reference.json"), &json!({}));
    write_json(&dir.join("contabo_configs.json"), &json!({ "plans": {} }));

    dir
}

fn assert_same_shape(expected: &Value, actual: &Value, path: &str) {
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            let mut expected_keys = expected.keys().collect::<Vec<_>>();
            let mut actual_keys = actual.keys().collect::<Vec<_>>();
            expected_keys.sort_unstable();
            actual_keys.sort_unstable();
            assert_eq!(expected_keys, actual_keys, "object keys changed at {path}");
            for (key, expected_value) in expected {
                assert_same_shape(
                    expected_value,
                    actual.get(key).expect("key equality already proved"),
                    &format!("{path}.{key}"),
                );
            }
        }
        (Value::Array(expected), Value::Array(actual)) => {
            assert_eq!(
                expected.len(),
                actual.len(),
                "array cardinality changed at {path}"
            );
            for (index, (expected_value, actual_value)) in
                expected.iter().zip(actual.iter()).enumerate()
            {
                assert_same_shape(expected_value, actual_value, &format!("{path}[{index}]"));
            }
        }
        (Value::String(_), Value::String(_))
        | (Value::Number(_), Value::Number(_))
        | (Value::Bool(_), Value::Bool(_))
        | (Value::Null, Value::Null) => {}
        _ => panic!("JSON type changed at {path}: expected {expected:?}, got {actual:?}"),
    }
}

#[tokio::test]
async fn runtime_responses_match_complete_golden_contract() {
    for fixture in [
        "meta.json",
        "plans.json",
        "catalog.json",
        "quote.json",
        "openapi.json",
    ] {
        assert!(
            fixture_dir().join(fixture).is_file(),
            "required contract fixture is missing: {fixture}"
        );
    }

    let snapshot_dir = create_snapshot_dir();
    let args = api::ServeArgs {
        bind: "127.0.0.1:0".into(),
        data_dir: Some(snapshot_dir.clone()),
        auth_token: None,
        auth_token_file: None,
        cors_origins: vec![],
    };
    let state = api::state::AppState::new(&args)
        .await
        .expect("load deterministic snapshot");

    let mut meta = api::handlers::meta(State(state.clone())).await.0;
    meta["scraper_version"] = json!("<runtime-version>");
    meta["data_dir"] = json!("<data-dir>");
    let expected_meta = expected_fixture("meta.json", &meta);
    assert_same_shape(&expected_meta, &meta, "$.meta");
    assert_eq!(expected_meta, meta);

    let plans = api::handlers::list_plans(
        State(state.clone()),
        Query(api::handlers::PlansQuery { family: None }),
    )
    .await
    .0;
    let expected_plans = expected_fixture("plans.json", &plans);
    assert_same_shape(&expected_plans, &plans, "$.plans");
    assert_eq!(expected_plans, plans);

    let catalog = api::handlers::catalog(State(state.clone())).await.0;
    let expected_catalog = expected_fixture("catalog.json", &catalog);
    assert_same_shape(&expected_catalog, &catalog, "$.catalog");
    assert_eq!(
        catalog["schema_version"],
        api::catalog::CATALOG_SCHEMA_VERSION
    );
    assert_eq!(catalog["items"][0]["payload"], plans[0]);
    assert_eq!(
        catalog["items"][0]["catalog_version"],
        catalog["catalog_version"]
    );
    assert_eq!(
        catalog["items"][0]["payload_hash"]
            .as_str()
            .unwrap_or_default()
            .len(),
        64
    );
    assert_eq!(
        catalog["payload_hash"].as_str().unwrap_or_default().len(),
        64
    );

    let quote = serde_json::to_value(
        api::handlers::quote(
            State(state),
            Json(api::handlers::QuoteRequest {
                plan_slug: "cloud-vps-core-4".into(),
                period_months: 12,
                selections: serde_json::Map::new(),
                currency: "INR".into(),
                gst: true,
                fx_markup: 0.035,
                fx_rate: Some(112.317),
            }),
        )
        .await
        .expect("quote from deterministic snapshot")
        .0,
    )
    .expect("quote response serializes");
    let expected_quote = expected_fixture("quote.json", &quote);
    assert_same_shape(&expected_quote, &quote, "$.quote");
    assert_eq!(expected_quote, quote);

    let mut openapi = api::handlers::openapi().await.0;
    openapi["info"]["version"] = json!("<runtime-version>");
    let expected_openapi = expected_fixture("openapi.json", &openapi);
    assert_same_shape(&expected_openapi, &openapi, "$.openapi");
    assert_eq!(expected_openapi, openapi);

    std::fs::remove_dir_all(&snapshot_dir).expect("remove contract-test snapshot");
}

#[test]
fn schema_constants_and_documentation_are_aligned() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let main = std::fs::read_to_string(root.join("src/main.rs")).expect("read src/main.rs");
    let catalog =
        std::fs::read_to_string(root.join("src/api/catalog.rs")).expect("read catalog.rs");
    let installer = std::fs::read_to_string(
        root.join("whmcs-module/modules/addons/contabo_pricing/lib/Installer.php"),
    )
    .expect("read Installer.php");
    let docs =
        std::fs::read_to_string(root.join("SCHEMA_VERSION.md")).expect("read SCHEMA_VERSION.md");

    assert!(main.contains("pub const SCHEMA_VERSION: &str = \"1.2\";"));
    assert!(catalog.contains("pub const CATALOG_SCHEMA_VERSION: &str = \"1.0\";"));
    assert!(installer.contains("public const SCHEMA_VERSION = 14;"));
    assert!(docs.contains("## API 1.2 — current"));
    assert!(docs.contains("## Catalog exchange 1.0 — current"));
    assert!(docs.contains("## WHMCS DB 14 — current"));
}
