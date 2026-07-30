use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use super::state::Snapshot;

/// Build the read-only, versioned catalog contract consumed by the WHMCS
/// pricing addon. Provider IDs are emitted only when the scraper actually
/// observed one; a marketing slug is never relabelled as a Customer API SKU.
pub fn build(snapshot: &Snapshot) -> Value {
    let observed_at = source_observed_at(snapshot);
    let source_version = snapshot
        .meta
        .get("scraper_version")
        .and_then(Value::as_str)
        .unwrap_or(crate::VERSION);

    let plans = snapshot
        .plans
        .get("plans")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut items = Vec::new();
    let mut profiles = Vec::new();

    for plan in &plans {
        let slug = plan
            .get("product_slug")
            .or_else(|| plan.get("slug"))
            .and_then(Value::as_str)
            .unwrap_or("unknown-plan");
        let label = plan
            .get("product_name")
            .or_else(|| plan.get("title"))
            .and_then(Value::as_str)
            .unwrap_or(slug);
        let provider_id = observed_provider_id(plan);

        items.push(catalog_item(
            format!("plan:{slug}"),
            provider_id,
            "plan",
            label,
            &observed_at,
            plan.clone(),
            json!({
                "family": plan.get("family").cloned().unwrap_or(Value::Null),
            }),
        ));

        profiles.push(json!({
            "machine_id": format!("profile:{slug}:default"),
            "plan_machine_id": format!("plan:{slug}"),
            "label": label,
            "source_observed_at": observed_at,
            "availability_state": "observed",
            "deprecated": false,
        }));
    }

    append_configuration_items(snapshot, &observed_at, &mut items);

    let payload = json!({
        "schema_version": "1.0",
        "source_version": source_version,
        "source_observed_at": observed_at,
        "effective_at": observed_at,
        "plans": plans,
        "profiles": profiles,
        "items": items,
        "compatibility": snapshot.option_catalog,
        "configurations": snapshot.configs,
    });
    let canonical = canonical_json(&payload);
    let version_seed_hash = sha256_hex(canonical.as_bytes());
    let timestamp_key: String = observed_at
        .chars()
        .filter(|character| character.is_ascii_digit())
        .take(14)
        .collect();
    let catalog_version = format!(
        "catalog-{}-{}",
        if timestamp_key.is_empty() {
            "unknown"
        } else {
            &timestamp_key
        },
        &version_seed_hash[..16]
    );

    let mut envelope = payload;
    if let Value::Object(ref mut map) = envelope {
        if let Some(Value::Array(items)) = map.get_mut("items") {
            for item in items {
                if let Value::Object(item) = item {
                    item.insert(
                        "catalog_version".to_string(),
                        Value::String(catalog_version.clone()),
                    );
                }
            }
        }
        map.insert(
            "catalog_version".to_string(),
            Value::String(catalog_version),
        );
    }
    let payload_hash = sha256_hex(canonical_json(&envelope).as_bytes());
    if let Value::Object(ref mut map) = envelope {
        map.insert("payload_hash".to_string(), Value::String(payload_hash));
    }
    envelope
}

fn append_configuration_items(snapshot: &Snapshot, observed_at: &str, items: &mut Vec<Value>) {
    let Some(plans) = snapshot.configs.get("plans").and_then(Value::as_object) else {
        return;
    };

    for plan in plans.values() {
        let plan_slug = plan
            .get("slug")
            .and_then(Value::as_str)
            .unwrap_or("unknown-plan");
        let Some(dimensions) = plan.get("options").and_then(Value::as_object) else {
            continue;
        };
        for (dimension, choices) in dimensions {
            let Some(choices) = choices.as_array() else {
                continue;
            };
            for choice in choices {
                let label = choice
                    .get("option_label")
                    .and_then(Value::as_str)
                    .unwrap_or("Unknown option");
                let category = choice
                    .get("category")
                    .and_then(Value::as_str)
                    .unwrap_or("default");
                let machine_id = format!(
                    "option:{}:{}:{}:{}",
                    machine_part(plan_slug),
                    machine_part(dimension),
                    machine_part(category),
                    machine_part(label)
                );
                items.push(catalog_item(
                    machine_id,
                    observed_provider_id(choice),
                    "configuration_option",
                    label,
                    observed_at,
                    choice.clone(),
                    json!({
                        "plan_machine_id": format!("plan:{plan_slug}"),
                        "dimension": dimension,
                        "category": category,
                    }),
                ));
            }
        }
    }
}

fn catalog_item(
    machine_id: String,
    provider_id: Option<String>,
    item_type: &str,
    label: &str,
    observed_at: &str,
    payload: Value,
    compatibility: Value,
) -> Value {
    let payload_hash = sha256_hex(canonical_json(&payload).as_bytes());
    json!({
        "machine_id": machine_id,
        "provider_id": provider_id,
        "item_type": item_type,
        "label": label,
        "catalog_version": Value::Null,
        "effective_at": observed_at,
        "availability_state": "observed",
        "deprecated": false,
        "compatibility": compatibility,
        "source_observed_at": observed_at,
        "payload_hash": payload_hash,
        "payload": payload,
    })
}

fn observed_provider_id(value: &Value) -> Option<String> {
    ["provider_id", "provider_sku_id", "customer_api_id"]
        .iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(ToOwned::to_owned)
}

fn source_observed_at(snapshot: &Snapshot) -> String {
    snapshot
        .meta
        .get("generated_at")
        .or_else(|| snapshot.plans.get("generated_at"))
        .or_else(|| snapshot.configs.get("generated_at"))
        .and_then(Value::as_str)
        .unwrap_or("1970-01-01T00:00:00Z")
        .to_string()
}

fn machine_part(value: &str) -> String {
    let mut result = String::new();
    let mut pending_dash = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            if pending_dash && !result.is_empty() {
                result.push('-');
            }
            result.push(character);
            pending_dash = false;
        } else {
            pending_dash = true;
        }
    }
    result
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn canonical_json(value: &Value) -> String {
    serde_json::to_string(&canonicalize(value)).expect("JSON values are serializable")
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            let mut sorted = Map::new();
            for key in keys {
                sorted.insert(key.clone(), canonicalize(&map[key]));
            }
            Value::Object(sorted)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Snapshot {
        Snapshot {
            meta: json!({"generated_at": "2026-07-30T10:20:30Z", "scraper_version": "test"}),
            view_model: Value::Null,
            plans: json!({"plans": [{
                "product_slug": "cloud-vps-10",
                "product_name": "Cloud VPS 10",
                "family": "Cloud VPS"
            }]}),
            option_catalog: json!({"dimensions": {}}),
            quick_reference: Value::Null,
            configs: json!({"plans": {
                "url": {
                    "slug": "cloud-vps-10",
                    "options": {
                        "Image": [{
                            "category": "OS",
                            "option_label": "Ubuntu 24.04",
                            "provider_id": "image-ubuntu-2404"
                        }]
                    }
                }
            }}),
        }
    }

    #[test]
    fn catalog_is_versioned_and_deterministic() {
        let first = build(&fixture());
        let second = build(&fixture());
        assert_eq!(first["catalog_version"], second["catalog_version"]);
        assert_eq!(first["payload_hash"], second["payload_hash"]);
        assert_eq!(first["items"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn marketing_slug_is_not_presented_as_provider_id() {
        let catalog = build(&fixture());
        let plan = &catalog["items"][0];
        assert_eq!(plan["machine_id"], "plan:cloud-vps-10");
        assert!(plan["provider_id"].is_null());
        assert_eq!(
            catalog["items"][1]["provider_id"],
            Value::String("image-ubuntu-2404".to_string())
        );
    }
}
