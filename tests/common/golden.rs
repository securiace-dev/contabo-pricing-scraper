// Golden fixture helpers for contract / schema-diff tests.
// Every function returns Result so callers can decide whether to
// assert! or silently skip when fixtures are absent.

#![allow(dead_code)]

use std::path::PathBuf;

use serde_json::Value;

pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").join("fixtures")
}

pub fn golden_file(name: &str) -> PathBuf {
    fixtures_dir().join(name)
}

pub fn fixture_exists(name: &str) -> bool {
    golden_file(name).exists()
}

pub fn load_fixture(name: &str) -> Result<Value, String> {
    let path = golden_file(name);
    let bytes = std::fs::read_to_string(&path).map_err(|e| {
        format!("cannot read {}: {}", path.display(), e)
    })?;
    serde_json::from_str(&bytes).map_err(|e| {
        format!("cannot parse {}: {}", path.display(), e)
    })
}

pub fn all_fixtures_present() -> bool {
    ["plans.golden.json", "meta.golden.json", "fx.golden.json", "quote.golden.json"]
        .iter()
        .all(|n| fixture_exists(n))
}

/// Recursively scan all numeric leaves in a JSON value and return
/// true if any leaf is negative.
pub fn has_negative_prices(v: &Value) -> bool {
    match v {
        Value::Number(n) => n.as_f64().map(|f| f < 0.0).unwrap_or(false),
        Value::Array(a) => a.iter().any(has_negative_prices),
        Value::Object(o) => o.values().any(has_negative_prices),
        _ => false,
    }
}

/// Collect all top-level string keys from a JSON object.
pub fn object_keys(obj: &Value) -> Vec<String> {
    obj.as_object()
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default()
}
