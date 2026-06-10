// Golden fixture contract tests. Run against the pre-captured test fixtures
// to validate API response shapes, required fields, and pricing invariants.
// These tests are read-only — they never hit the network or a live server.

#![allow(dead_code)]

use std::path::PathBuf;

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

pub async fn run_scrape(_opts: Opts) -> i32 {
    0
}

// Pull in api module (same pattern as api_smoke.rs)
#[path = "../src/api/mod.rs"]
mod api;

mod common;

use common::golden;

// ────────────────────────────────────────────────────────────────────────────
//  Schema contract tests — validate field existence and types
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn plans_golden_loaded() {
    if !golden::all_fixtures_present() {
        eprintln!("skip plans_golden_loaded — golden fixtures missing");
        return;
    }
    let plans = golden::load_fixture("plans.golden.json").expect("plans fixture");
    let arr = plans.as_array().expect("plans is array");
    assert!(!arr.is_empty(), "plans fixture is empty");
}

#[test]
fn plans_response_has_required_fields() {
    if !golden::all_fixtures_present() {
        eprintln!("skip plans_response_has_required_fields — golden fixtures missing");
        return;
    }
    let plans = golden::load_fixture("plans.golden.json").expect("plans fixture");
    let arr = plans.as_array().expect("plans is array");

    let required = &[
        "product_slug", "product_name", "family",
        "base_monthly_price",
    ];
    let string_fields = &["product_slug", "product_name", "family"];

    for plan in arr {
        let slug = plan["product_slug"].as_str().unwrap_or("?");

        for field in required {
            assert!(
                plan.get(*field).is_some(),
                "plan {slug} missing required field {field}"
            );
        }

        for field in string_fields {
            let val = plan.get(*field);
            if let Some(v) = val {
                assert!(
                    v.is_string(),
                    "plan {slug}: field {field} should be string, got {v}"
                );
            }
        }

        assert!(
            plan["base_monthly_price"].is_number(),
            "plan {slug}: base_monthly_price should be a number"
        );
    }
}

#[test]
fn plans_response_periods_are_valid() {
    if !golden::all_fixtures_present() {
        eprintln!("skip plans_response_periods_are_valid — golden fixtures missing");
        return;
    }
    let plans = golden::load_fixture("plans.golden.json").expect("plans fixture");
    let arr = plans.as_array().expect("plans is array");
    let valid_months = [1, 3, 6, 12, 24, 36];

    for plan in arr {
        let slug = plan["product_slug"].as_str().unwrap_or("?");
        let periods = plan["periods"].as_array();
        if periods.is_none() {
            continue;
        }
        for p in periods.unwrap() {
            let months = p["months"].as_u64();
            assert!(months.is_some(), "plan {slug}: period missing months field");
            let m = months.unwrap();
            assert!(
                valid_months.contains(&(m as i32)),
                "plan {slug}: unexpected period months {m}"
            );
            let price = p["effective_monthly"].as_f64();
            assert!(price.is_some(), "plan {slug}: period missing effective_monthly");
        }
    }
}

#[test]
fn meta_response_has_required_fields() {
    if !golden::all_fixtures_present() {
        eprintln!("skip meta_response_has_required_fields — golden fixtures missing");
        return;
    }
    let meta = golden::load_fixture("meta.golden.json").expect("meta fixture");

    assert!(
        meta.get("scraper_version").is_some(),
        "missing scraper_version"
    );
    assert!(
        meta.get("schema_version").is_some(),
        "missing schema_version"
    );
    assert!(
        meta.get("snapshot_meta").is_some(),
        "missing snapshot_meta"
    );
    assert!(
        meta.get("data_dir").is_some(),
        "missing data_dir"
    );

    assert!(meta["scraper_version"].is_string(), "scraper_version should be string");
    assert!(meta["schema_version"].is_string(), "schema_version should be string");
}

#[test]
fn fx_response_matches_golden_schema() {
    if !golden::all_fixtures_present() {
        eprintln!("skip fx_response_matches_golden_schema — golden fixtures missing");
        return;
    }
    let fx = golden::load_fixture("fx.golden.json").expect("fx fixture");

    let obj = fx.as_object().expect("fx response should be an object");

    assert!(
        obj.get("base").is_some(),
        "fx response missing 'base'"
    );

    if let Some(rates) = obj.get("rates") {
        let rates_obj = rates.as_object().expect("rates should be an object");
        assert!(!rates_obj.is_empty(), "rates should not be empty");
        for (code, rate) in rates_obj {
            assert!(
                code.len() == 3 && code.chars().all(|c| c.is_ascii_uppercase()),
                "currency code {code} should be 3 uppercase letters"
            );
            assert!(
                rate.is_number(),
                "rate for {code} should be a number"
            );
        }
    }
}

#[test]
fn quote_response_matches_golden_schema() {
    if !golden::all_fixtures_present() {
        eprintln!("skip quote_response_matches_golden_schema — golden fixtures missing");
        return;
    }
    let quote = golden::load_fixture("quote.golden.json").expect("quote fixture");

    let required_fields = &[
        "plan_slug", "period_months", "currency",
        "base_monthly_eur", "configured_monthly_eur", "setup_fee_eur",
        "gst_amount_eur", "fx_rate", "fx_markup",
        "final_monthly", "final_total", "breakdown",
    ];

    for field in required_fields {
        assert!(
            quote.get(*field).is_some(),
            "quote missing required field {field}"
        );
    }

    assert!(quote["plan_slug"].is_string(), "plan_slug should be string");
    assert!(quote["period_months"].is_number(), "period_months should be number");
    assert!(quote["currency"].is_string(), "currency should be string");

    let currency = quote["currency"].as_str().unwrap();
    assert!(
        currency.len() == 3 && currency.chars().all(|c| c.is_ascii_uppercase()),
        "currency should be 3 uppercase letters, got {currency}"
    );

    let breakdown = quote["breakdown"].as_array().expect("breakdown is array");
    assert!(!breakdown.is_empty(), "breakdown should not be empty");
}

#[test]
fn no_negative_prices_in_golden() {
    if !golden::all_fixtures_present() {
        eprintln!("skip no_negative_prices_in_golden — golden fixtures missing");
        return;
    }

    for fixture in &["plans.golden.json", "quote.golden.json"] {
        let data = golden::load_fixture(fixture).expect(fixture);
        assert!(
            !golden::has_negative_prices(&data),
            "fixture {fixture} contains negative price values"
        );
    }
}

#[test]
fn cycle_names_match_whmcs_columns() {
    if !golden::all_fixtures_present() {
        eprintln!("skip cycle_names_match_whmcs_columns — golden fixtures missing");
        return;
    }
    let plans = golden::load_fixture("plans.golden.json").expect("plans fixture");
    let arr = plans.as_array().expect("plans is array");
    let valid_cycle_keys = [
        "effective_monthly", "total_period_cost", "discount_total",
        "months", "setup_fee", "is_hidden_from_ui",
    ];

    for plan in arr {
        let slug = plan["product_slug"].as_str().unwrap_or("?");
        let periods = plan["periods"].as_array();
        if periods.is_none() {
            continue;
        }
        for p in periods.unwrap() {
            for key in p.as_object().map(|o| o.keys()).into_iter().flatten() {
                assert!(
                    valid_cycle_keys.contains(&key.as_str()),
                    "plan {slug}: unexpected period key {key}"
                );
            }
        }
    }
}

#[test]
fn configurator_response_matches_golden_schema() {
    let configurator = match golden::load_fixture("configurator.golden.json") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skip configurator_response_matches_golden_schema — fixture absent");
            return;
        }
    };

    let obj = configurator.as_object().expect("configurator should be an object");

    let required = &["slug", "options", "base_monthly_price", "title", "family"];
    let string_fields = &["slug", "title"];

    for field in required {
        assert!(
            obj.get(*field).is_some(),
            "configurator missing required field {field}"
        );
    }

    for field in string_fields {
        assert!(
            obj[*field].is_string(),
            "configurator field {field} should be string"
        );
    }

    let options = obj["options"].as_object().expect("options should be an object");
    assert!(!options.is_empty(), "options should not be empty");

    for (_dim_key, dim_options) in options {
        let arr = dim_options.as_array().expect("dimension options should be array");
        for opt in arr {
            assert!(
                opt.get("option_label").is_some(),
                "option missing option_label"
            );
            assert!(
                opt.get("dimension").is_some(),
                "option missing dimension"
            );
            assert!(
                opt.get("monthly_price_delta").is_some(),
                "option missing monthly_price_delta"
            );
        }
    }
}

#[test]
fn currency_fields_are_uppercase_strings() {
    if !golden::all_fixtures_present() {
        eprintln!("skip currency_fields_are_uppercase_strings — golden fixtures missing");
        return;
    }
    let quote = golden::load_fixture("quote.golden.json").expect("quote fixture");
    let currency = quote["currency"].as_str().expect("currency should be string");
    assert!(
        currency.len() == 3 && currency.chars().all(|c| c.is_ascii_uppercase()),
        "currency should be 3 uppercase letters, got {currency}"
    );

    let fx = golden::load_fixture("fx.golden.json").expect("fx fixture");
    if let Some(rates) = fx["rates"].as_object() {
        for code in rates.keys() {
            assert!(
                code.len() == 3 && code.chars().all(|c| c.is_ascii_uppercase()),
                "currency code {code} should be 3 uppercase letters"
            );
        }
    }
}
