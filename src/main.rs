mod api;

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::{Parser, Subcommand};
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{json, Map, Value};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{sleep, Duration};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const SCHEMA_VERSION: &str = "1.1";

// Exit codes
const EXIT_OK: i32 = 0;
const EXIT_ERROR: i32 = 1;
const EXIT_PARTIAL: i32 = 2;

static ALL_PLAN_URLS: &[&str] = &[
    "https://contabo.com/en/vps/cloud-vps-10/",
    "https://contabo.com/en/vps/cloud-vps-20/",
    "https://contabo.com/en/vps/cloud-vps-30/",
    "https://contabo.com/en/vps/cloud-vps-40/",
    "https://contabo.com/en/vps/cloud-vps-50/",
    "https://contabo.com/en/vps/cloud-vps-60/",
    "https://contabo.com/en/storage-vps/storage-vps-10/",
    "https://contabo.com/en/storage-vps/storage-vps-20/",
    "https://contabo.com/en/storage-vps/storage-vps-30/",
    "https://contabo.com/en/storage-vps/storage-vps-40/",
    "https://contabo.com/en/storage-vps/storage-vps-50/",
    "https://contabo.com/en/vds/vds-s/",
    "https://contabo.com/en/vds/vds-m/",
    "https://contabo.com/en/vds/vds-l/",
    "https://contabo.com/en/vds/vds-xl/",
    "https://contabo.com/en/vds/vds-xxl/",
];

// ─── Types ────────────────────────────────────────────────────────────────────

/// Which HTTP backend to use for page fetches.
#[derive(Clone, Debug, PartialEq, Default, clap::ValueEnum)]
pub enum FetchMode {
    /// Pure reqwest (default). Fast; evades basic WAF header checks.
    #[default]
    Reqwest,
    /// CloakBrowser only. Routes all URLs through a patched Chromium subprocess.
    Cloak,
    /// reqwest first; fall back to CloakBrowser for any blocked/non-200 URLs.
    Auto,
}

#[derive(Clone)]
pub(crate) struct Opts {
    pub output: PathBuf,
    pub concurrency: usize,
    pub retries: u32,
    pub plans: Option<Vec<String>>,
    pub plan_urls_file: Option<PathBuf>,
    /// Retained for callers (e.g. API refresh) that still pass it; the value
    /// is now consumed in `main()` to set the tracing filter level.
    #[allow(dead_code)]
    pub quiet: bool,
    pub json_out: bool,
    pub dry_run: bool,
    pub fetch_mode: FetchMode,
    pub cloak_script: PathBuf,
    pub proxy: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct GapEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    plan_sku: Option<String>,
    gap: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl GapEntry {
    fn to_json(&self) -> Value {
        serde_json::to_value(self).expect("GapEntry serialization is infallible")
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct OptionItem {
    plan_sku: String,
    // Field order intentionally mirrors the Node scraper exactly so that
    // diffing the two outputs (parity_check.sh) sees no field-order drift.
    // The non-obvious bit: region_* fields come BEFORE the price deltas,
    // matching how Node spreads classified region records.
    currency: String,
    dimension: String,
    category: String,
    option_label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    region_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subregion: Option<String>,
    #[serde(serialize_with = "serialize_whole_f64")]
    monthly_price_delta: f64,
    #[serde(serialize_with = "serialize_whole_f64")]
    setup_fee_delta: f64,
    // Node only writes `is_default` when true; omit-when-false matches that.
    #[serde(skip_serializing_if = "is_false", default)]
    is_default: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

// Serde adapter: emit whole f64 values as JSON integers (matches `json_num`
// and JS JSON.stringify behaviour). Used by OptionItem's price-delta fields.
fn serialize_whole_f64<S: serde::Serializer>(v: &f64, s: S) -> Result<S::Ok, S::Error> {
    if v.fract() == 0.0 && v.is_finite() {
        s.serialize_i64(*v as i64)
    } else {
        s.serialize_f64(*v)
    }
}

impl OptionItem {
    fn sort_key(&self) -> String {
        format!("{}|{}|{}", self.dimension, self.category, self.option_label)
    }

    fn dedup_key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.plan_sku, self.dimension, self.category, self.option_label
        )
    }

    fn to_json(&self) -> Value {
        serde_json::to_value(self).expect("OptionItem serialization is infallible")
    }

    #[allow(dead_code)] // retained for backward compat; the new enriched CSV writer inlines its own row builder
    fn to_csv_row(&self) -> Vec<String> {
        vec![
            self.plan_sku.clone(),
            self.dimension.clone(),
            self.category.clone(),
            self.option_label.clone(),
            self.monthly_price_delta.to_string(),
            self.setup_fee_delta.to_string(),
            self.region_group.clone().unwrap_or_default(),
            self.country.clone().unwrap_or_default(),
            self.country_code.clone().unwrap_or_default(),
            self.subregion.clone().unwrap_or_default(),
            if self.is_default {
                "true".into()
            } else {
                "false".into()
            },
            self.currency.clone(),
        ]
    }
}

struct PlanResult {
    base_plan: Value,
    final_options: Vec<OptionItem>,
    plan_config: Value,
    url: String,
}

// ─── CLI ──────────────────────────────────────────────────────────────────────

fn slug_from_url(url: &str) -> &str {
    url.trim_end_matches('/').rsplit('/').next().unwrap_or("")
}

// CLI parsing — clap derive-based. The old hand-rolled parse_args has been
// replaced; flag surface is preserved bit-for-bit by the ScrapeArgs struct
// below. `print_help` is gone too — clap auto-generates --help / --version.

#[derive(Parser, Debug)]
#[command(
    name = "contabo-scraper",
    version,
    about = "Contabo VPS/VDS pricing scraper + API server"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Inherited from old positional flags — only used when no subcommand is given
    #[command(flatten)]
    legacy: ScrapeArgs,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Scrape Contabo's pricing pages and write JSON/CSV outputs (one-shot)
    Scrape(ScrapeArgs),
    /// Start the HTTP API server (long-running)
    Serve(api::ServeArgs),
}

#[derive(clap::Args, Debug, Clone)]
struct ScrapeArgs {
    /// Output directory for JSON/CSV files
    #[arg(short, long, env = "CONTABO_OUTPUT")]
    output: Option<PathBuf>,

    /// Parallel fetches (1-16 recommended)
    #[arg(short, long, env = "SCRAPER_CONCURRENCY", default_value_t = 4)]
    concurrency: usize,

    /// Retries per URL on network error
    #[arg(short, long, env = "SCRAPER_RETRIES", default_value_t = 3)]
    retries: u32,

    /// Comma-separated plan slugs to limit scraping (e.g. cloud-vps-10,vds-s)
    #[arg(short, long, value_delimiter = ',')]
    plans: Option<Vec<String>>,

    /// Load plan URLs from a JSON catalog (filters status=active)
    #[arg(long, value_name = "PATH")]
    plan_urls_file: Option<PathBuf>,

    /// Suppress progress output (stderr stays active for errors)
    #[arg(short, long)]
    quiet: bool,

    /// Print JSON summary to stdout on completion
    #[arg(short, long)]
    json: bool,

    /// Fetch pages but do not write any output files
    #[arg(long)]
    dry_run: bool,

    /// Fetch strategy: reqwest (default), cloak (CloakBrowser only), auto (reqwest + cloak fallback on block)
    #[arg(
        long,
        env = "FETCH_MODE",
        default_value = "reqwest",
        value_name = "MODE"
    )]
    fetch_mode: FetchMode,

    /// Path to cloak-fetch.mjs (used when --fetch-mode is cloak or auto)
    #[arg(
        long,
        env = "CLOAK_SCRIPT",
        default_value = "scripts/cloak-fetch.mjs",
        value_name = "PATH"
    )]
    cloak_script: PathBuf,

    /// Optional HTTP/HTTPS proxy for all fetches (reqwest and CloakBrowser).
    /// Format: http://user:pass@host:port
    #[arg(long, env = "SCRAPER_PROXY", value_name = "URL")]
    proxy: Option<String>,
}

impl ScrapeArgs {
    fn into_opts(self) -> Opts {
        let default_output = std::env::current_dir()
            .map(|d| d.join("data").join("output"))
            .unwrap_or_else(|_| PathBuf::from("data/output"));
        Opts {
            output: self.output.unwrap_or(default_output),
            concurrency: self.concurrency.max(1),
            retries: self.retries,
            plans: self.plans.filter(|v| !v.is_empty()),
            plan_urls_file: self.plan_urls_file,
            quiet: self.quiet,
            json_out: self.json,
            dry_run: self.dry_run,
            fetch_mode: self.fetch_mode,
            cloak_script: self.cloak_script,
            // Normalize a schemeless proxy (user:pass@host:port) to http:// once,
            // so every downstream consumer (reqwest + the CloakBrowser subprocess,
            // whose `new URL()` is strict) receives a valid URL.
            proxy: self.proxy.map(|p| {
                if p.contains("://") {
                    p
                } else {
                    format!("http://{p}")
                }
            }),
        }
    }
}

// ─── HTTP ─────────────────────────────────────────────────────────────────────

async fn fetch_html(client: &reqwest::Client, url: &str) -> Result<String, String> {
    let res = client
        .get(url)
        .header("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .header("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
        .header("accept-language", "en-US,en;q=0.9")
        .header("accept-encoding", "gzip, deflate, br")
        .header("upgrade-insecure-requests", "1")
        .header("sec-fetch-dest", "document")
        .header("sec-fetch-mode", "navigate")
        .header("sec-fetch-site", "none")
        .header("sec-fetch-user", "?1")
        .header("cache-control", "max-age=0")
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !res.status().is_success() {
        return Err(format!("HTTP {}", res.status()));
    }
    res.text().await.map_err(|e| e.to_string())
}

async fn fetch_with_retry(
    client: &reqwest::Client,
    url: &str,
    retries: u32,
) -> Result<String, String> {
    let mut last_error = String::new();
    for attempt in 0..=retries {
        match fetch_html(client, url).await {
            Ok(html) => return Ok(html),
            Err(e) => {
                last_error = e.clone();
                if attempt < retries {
                    let delay_ms = (1000u64 * 2u64.pow(attempt)).min(8000);
                    tracing::warn!(
                        attempt = attempt + 1,
                        retries,
                        url = %url,
                        error = %e,
                        delay_ms,
                        "retry"
                    );
                    sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
    Err(last_error)
}

// ─── SAPPER extraction ────────────────────────────────────────────────────────

const SAPPER_END_MARKERS: &[&str] = &[
    "};(function(){try{eval(\"async function x(){}\")",
    "};(function()",
];

fn extract_sapper_snippet(html: &str) -> Result<String, String> {
    let start = html
        .find("__SAPPER__=")
        .ok_or("__SAPPER__ payload not found in HTML")?;

    let mut end: Option<usize> = None;
    for marker in SAPPER_END_MARKERS {
        if let Some(rel) = html[start..].find(marker) {
            end = Some(start + rel);
            break;
        }
    }

    let end = match end {
        Some(e) => e,
        None => {
            let rel = html[start..]
                .find("</script>")
                .ok_or("Could not find end of __SAPPER__ payload")?;
            let snippet = html[start..start + rel].trim_end();
            let last_brace = snippet.rfind('}').ok_or("Malformed __SAPPER__ block")?;
            start + last_brace
        }
    };

    Ok(html[start..end + 1].to_string())
}

fn eval_sapper_js(snippet: &str) -> Result<Value, String> {
    use rquickjs::{Context, Runtime};

    let rt = Runtime::new().map_err(|e| e.to_string())?;
    let ctx = Context::full(&rt).map_err(|e| e.to_string())?;
    // Declare __SAPPER__ with `var` so it's a proper global accessible
    // in subsequent ctx.eval calls (plain assignment doesn't persist across evals).
    let script = if snippet.trim_start().starts_with("__SAPPER__=") {
        format!(
            "var window={{}}; var document={{}}; var {}",
            snippet.trim_start()
        )
    } else {
        format!("var window={{}}; var document={{}}; {}", snippet)
    };

    ctx.with(|ctx| -> Result<Value, String> {
        ctx.eval::<rquickjs::Value<'_>, _>(script.as_str())
            .map_err(|e| e.to_string())?;
        let json_str: String = ctx
            .eval("JSON.stringify(__SAPPER__)")
            .map_err(|e| e.to_string())?;
        serde_json::from_str(&json_str).map_err(|e| e.to_string())
    })
}

fn extract_sapper(html: &str) -> Result<Value, String> {
    let snippet = extract_sapper_snippet(html)?;
    eval_sapper_js(&snippet)
}

fn extract_password_rules(html: &str) -> Option<Value> {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(\d+)-(\d+) alphanumeric characters \(no special characters\)").unwrap()
    });
    let caps = RE.captures(html)?;
    let min: u32 = caps[1].parse().ok()?;
    let max: u32 = caps[2].parse().ok()?;
    Some(
        json!({ "min_length": min, "max_length": max, "alphanumeric_only": true, "no_special_chars": true }),
    )
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn escape_csv(v: &str) -> String {
    if v.contains('"') || v.contains(',') || v.contains('\n') || v.contains('\r') {
        format!("\"{}\"", v.replace('"', "\"\""))
    } else {
        v.to_string()
    }
}

fn title_from_slug(slug: &str) -> String {
    if let Some(n) = slug.strip_prefix("cloud-vps-") {
        return format!("Cloud VPS {n}");
    }
    if let Some(n) = slug.strip_prefix("storage-vps-") {
        return format!("Storage VPS {n}");
    }
    if let Some(n) = slug.strip_prefix("vds-") {
        return format!("Cloud VDS {}", n.to_uppercase());
    }
    slug.to_string()
}

fn family_from_type(t: &str) -> &'static str {
    match t {
        "vps" => "Cloud VPS",
        "storage-vps" => "Storage VPS",
        "vds" => "Cloud VDS",
        _ => "Unknown",
    }
}

fn normalize_storage_label(label: &str) -> String {
    static RE_NVME_SSD: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bNVMe SSD\b").unwrap());
    static RE_NVME: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bNVME\b").unwrap());
    let s = RE_NVME_SSD.replace_all(label, "NVMe");
    RE_NVME.replace_all(&s, "NVMe").trim().to_string()
}

fn iso_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

// Serialize f64 as integer JSON when the value is whole (e.g. 8.0 → 8, 0.0 → 0),
// matching JavaScript's JSON.stringify behaviour for whole-number floats.
fn json_num(v: f64) -> Value {
    if v.fract() == 0.0 {
        json!(v as i64)
    } else {
        json!(v)
    }
}

// ─── Spec parsers ─────────────────────────────────────────────────────────────

fn parse_cpu_count(s: &str) -> Option<u32> {
    static RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)^(\d+)\s+(?:vCPU\s+|Physical\s+)?Cores?").unwrap());
    RE.captures(s)?.get(1)?.as_str().parse().ok()
}

fn parse_ram_gb(s: &str) -> Option<Value> {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s*GB").unwrap());
    let v: f64 = RE.captures(s)?.get(1)?.as_str().parse().ok()?;
    Some(json_num(v))
}

fn parse_port_speed_mbps(s: &str) -> Option<u32> {
    static RE_M: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s*Mbit/s").unwrap());
    static RE_G: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s*Gbit/s").unwrap());
    if let Some(caps) = RE_M.captures(s) {
        return caps[1].parse::<f64>().ok().map(|v| v as u32);
    }
    if let Some(caps) = RE_G.captures(s) {
        return caps[1]
            .parse::<f64>()
            .ok()
            .map(|v| (v * 1000.0).round() as u32);
    }
    None
}

fn parse_storage_gb(s: &str) -> Option<u32> {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s*(GB|TB)").unwrap());
    let caps = RE.captures(s)?;
    let n: f64 = caps[1].parse().ok()?;
    Some(if caps[2].eq_ignore_ascii_case("TB") {
        (n * 1000.0).round() as u32
    } else {
        n as u32
    })
}

fn monthly_price_for_period(
    base: f64,
    months: u32,
    discount_eur: f64,
    setup_eur: f64,
) -> (f64, f64, f64, f64) {
    let total = base * months as f64 - discount_eur + setup_eur;
    let effective = (base * months as f64 - discount_eur) / months as f64;
    (
        round2(effective),
        round2(setup_eur),
        round2(total),
        round2(discount_eur),
    )
}

// ─── Region classification ────────────────────────────────────────────────────

fn asia_iso_code(country: &str) -> String {
    match country {
        "India" => "IN",
        "Japan" => "JP",
        "Singapore" => "SG",
        "Korea" => "KR",
        "Taiwan" => "TW",
        _ => return country.chars().take(2).collect::<String>().to_uppercase(),
    }
    .to_string()
}

struct RegionInfo {
    region_group: &'static str,
    country: String,
    country_code: String,
    subregion: Option<String>,
}

fn classify_region(title: &str) -> Option<RegionInfo> {
    static RE_US_SUB: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)^United States \(([^)]+)\)$").unwrap());
    static RE_ASIA: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^Asia \(([^)]+)\)$").unwrap());
    static RE_AU_SUB: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)^Australia \(([^)]+)\)$").unwrap());

    let tl = title.to_lowercase();
    if tl == "european union" {
        return Some(RegionInfo {
            region_group: "Europe",
            country: "European Union".into(),
            country_code: "EU".into(),
            subregion: None,
        });
    }
    if tl == "united kingdom" {
        return Some(RegionInfo {
            region_group: "Europe",
            country: "United Kingdom".into(),
            country_code: "UK".into(),
            subregion: None,
        });
    }
    if tl == "germany" {
        return Some(RegionInfo {
            region_group: "Europe",
            country: "Germany".into(),
            country_code: "DE".into(),
            subregion: None,
        });
    }
    if tl.starts_with("canada") {
        return Some(RegionInfo {
            region_group: "America",
            country: "Canada".into(),
            country_code: "CA".into(),
            subregion: None,
        });
    }
    if tl == "united states" {
        return Some(RegionInfo {
            region_group: "America",
            country: "United States".into(),
            country_code: "US".into(),
            subregion: None,
        });
    }

    if let Some(caps) = RE_US_SUB.captures(title) {
        return Some(RegionInfo {
            region_group: "America",
            country: format!("United States ({})", &caps[1]),
            country_code: "US".into(),
            subregion: Some(caps[1].to_string()),
        });
    }
    if let Some(caps) = RE_ASIA.captures(title) {
        let name = caps[1].to_string();
        let code = asia_iso_code(&name);
        return Some(RegionInfo {
            region_group: "Asia",
            country: name,
            country_code: code,
            subregion: None,
        });
    }
    if RE_AU_SUB.is_match(title) || tl == "australia" {
        return Some(RegionInfo {
            region_group: "Australia",
            country: "Australia".into(),
            country_code: "AU".into(),
            subregion: None,
        });
    }
    None
}

fn is_ignored_title(title: &str) -> bool {
    let tl = title.to_lowercase();

    // cPanel/WHM entries are ignored EXCEPT for the "5 accounts" plan
    if tl.starts_with("cpanel/whm (") && !tl.eq_ignore_ascii_case("cpanel/whm (5 accounts)") {
        return true;
    }

    static PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
        vec![
            Regex::new(r"(?i)Object Storage").unwrap(),
            Regex::new(r"(?i)FTP Storage").unwrap(),
            Regex::new(r"(?i)Monitoring").unwrap(),
            Regex::new(r"(?i)^Managed$").unwrap(),
            Regex::new(r"(?i)^Unmanaged$").unwrap(),
            Regex::new(r"(?i)SSL certificate").unwrap(),
            Regex::new(r"(?i)Firewall").unwrap(),
            Regex::new(r"(?i)DevOps Features").unwrap(),
            Regex::new(r"(?i)Custom Images Storage").unwrap(),
            Regex::new(r"(?i)^Password$").unwrap(),
            Regex::new(r"(?i)SSH Keys").unwrap(),
            Regex::new(r"(?i)^No Firewall$").unwrap(),
            Regex::new(r"(?i)^No license required$").unwrap(),
            Regex::new(r"(?i)^None$").unwrap(),
            Regex::new(r"(?i)^Use your existing Custom Image Storage$").unwrap(),
            Regex::new(r"(?i)^Backup Space$").unwrap(),
            Regex::new(r"(?i)In order to use SSH Keys").unwrap(),
        ]
    });
    PATTERNS.iter().any(|p| p.is_match(title))
}

#[allow(clippy::large_enum_variant)]
enum ClassifyResult {
    Include(OptionItem),
    Gap { title: String },
    Skip,
}

fn classify_addon(
    title: &str,
    monthly: f64,
    setup: f64,
    plan_sku: &str,
    product_type: &str,
) -> ClassifyResult {
    let title = title.trim();
    if title.is_empty() {
        return ClassifyResult::Skip;
    }
    if is_ignored_title(title) {
        return ClassifyResult::Skip;
    }

    let make =
        |dimension: &str, category: &str, label: String, ri: Option<RegionInfo>| OptionItem {
            plan_sku: plan_sku.to_string(),
            currency: "EUR".to_string(),
            dimension: dimension.to_string(),
            category: category.to_string(),
            option_label: label,
            monthly_price_delta: monthly,
            setup_fee_delta: setup,
            region_group: ri.as_ref().map(|r| r.region_group.to_string()),
            country: ri.as_ref().map(|r| r.country.clone()),
            country_code: ri.as_ref().map(|r| r.country_code.clone()),
            subregion: ri.as_ref().and_then(|r| r.subregion.clone()),
            is_default: false,
        };

    // Region
    if let Some(ri) = classify_region(title) {
        let label = ri.country.clone();
        let cat = ri.region_group;
        return ClassifyResult::Include(make("Region", cat, label, Some(ri)));
    }

    // Storage e.g. "100 GB NVMe", "400 GB SSD"
    {
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s*(GB|TB)\s*(NVMe|SSD)(?: SSD)?$").unwrap()
        });
        if let Some(caps) = RE.captures(title) {
            let st = if caps[3].eq_ignore_ascii_case("nvme") {
                "NVMe"
            } else {
                "SSD"
            };
            let lbl = normalize_storage_label(&format!("{} {} {}", &caps[1], &caps[2], &caps[3]));
            let dim = if product_type == "vds" {
                "Storage"
            } else {
                "Storage Type"
            };
            return ClassifyResult::Include(make(dim, st, lbl, None));
        }
    }

    // Data protection
    if title.eq_ignore_ascii_case("Auto Backup") {
        return ClassifyResult::Include(make(
            "Data Protection",
            "Auto Backup",
            "Auto Backup".into(),
            None,
        ));
    }

    // Private networking
    {
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)^(No Private Networking|Private Networking Enabled)$").unwrap()
        });
        if RE.is_match(title) {
            return ClassifyResult::Include(make(
                "Networking",
                "Private Networking",
                title.to_string(),
                None,
            ));
        }
    }

    // Bandwidth / traffic
    if title.contains("Traffic") || title.contains("Out + Unlimited In") {
        return ClassifyResult::Include(make("Networking", "Bandwidth", title.to_string(), None));
    }

    // IPv4 (normalize Contabo's "IP adress" typo)
    {
        static RE_MATCH: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)IP Address|IP adress").unwrap());
        static RE_TYPO: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bIP adress\b").unwrap());
        if RE_MATCH.is_match(title) {
            let lbl = RE_TYPO.replace_all(title, "IP Address").to_string();
            return ClassifyResult::Include(make("Networking", "IPv4", lbl, None));
        }
    }

    // OS images
    {
        static RE_OS: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)^(Ubuntu|Debian|AlmaLinux|Rocky Linux|Arch Linux|FreeBSD|Fedora|CentOS|openSUSE|Gentoo)").unwrap()
        });
        let tl = title.to_lowercase();
        if tl.starts_with("windows server") || RE_OS.is_match(title) || tl.contains("custom image")
        {
            return ClassifyResult::Include(make("Image", "OS", title.to_string(), None));
        }
    }

    // Control panels
    {
        static RE_CPANEL: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)^cPanel/WHM \(5 accounts\)$").unwrap());
        static RE_PLESK: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)^Plesk Obsidian Web (Admin|Pro|Host) Edition$").unwrap());
        static RE_WEBMIN: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)^Webmin( \+ LAMP)?$").unwrap());
        static RE_OTHER: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)^(DirectAdmin|ISPConfig)$").unwrap());

        if RE_CPANEL.is_match(title)
            || RE_PLESK.is_match(title)
            || RE_WEBMIN.is_match(title)
            || RE_OTHER.is_match(title)
        {
            let lbl = if let Some(caps) = RE_PLESK.captures(title) {
                format!("Plesk {} Edition", &caps[1])
            } else if RE_WEBMIN.is_match(title) && !title.to_lowercase().contains("lamp") {
                "Webmin".into()
            } else {
                title.to_string()
            };
            return ClassifyResult::Include(make("Image", "Panels", lbl, None));
        }
    }

    // Apps / blockchain
    {
        static RE_APP: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)Server$|^(Docker|LAMP|Coolify|DeepSeek|IPFS Node|Flux Node|Horizen Node|Bitcoin Node|Ethereum Staking Node)$").unwrap()
        });
        if RE_APP.is_match(title) {
            let tl = title.to_lowercase();
            let cat = if tl.contains("node") || tl.contains("staking") {
                "Blockchain"
            } else {
                "Apps"
            };
            let lbl = if title.eq_ignore_ascii_case("Gitlab Server") {
                "GitLab Server".into()
            } else {
                title.to_string()
            };
            return ClassifyResult::Include(make("Image", cat, lbl, None));
        }
    }

    ClassifyResult::Gap {
        title: title.to_string(),
    }
}

// ─── Default injection ────────────────────────────────────────────────────────

fn inject_defaults(
    plan_sku: &str,
    product_type: &str,
    storage_title: Option<&str>,
    storage_subtitle: Option<&str>,
    html: &str,
    classified: &mut Vec<OptionItem>,
) {
    // Build existing set up front (don't hold borrow while mutating)
    let existing: HashSet<String> = classified
        .iter()
        .map(|i| format!("{}|{}", i.dimension, i.option_label))
        .collect();

    // Pre-compute Windows check before we borrow `additions` via the closure
    let has_windows_in_classified = classified
        .iter()
        .any(|i| i.dimension == "Image" && i.option_label.starts_with("Windows Server"));

    let mut additions: Vec<OptionItem> = Vec::new();

    let mut add = |dimension: &str,
                   category: &str,
                   label: &str,
                   is_default: bool,
                   monthly: f64,
                   setup: f64| {
        let key = format!("{dimension}|{label}");
        if !existing.contains(&key) {
            additions.push(OptionItem {
                plan_sku: plan_sku.to_string(),
                currency: "EUR".to_string(),
                dimension: dimension.to_string(),
                category: category.to_string(),
                option_label: label.to_string(),
                monthly_price_delta: monthly,
                setup_fee_delta: setup,
                region_group: None,
                country: None,
                country_code: None,
                subregion: None,
                is_default,
            });
        }
    };

    // No backup / data protection default
    let dp_label = if product_type == "vds" {
        "No Backup Space"
    } else {
        "No Data Protection"
    };
    add("Data Protection", "None", dp_label, true, 0.0, 0.0);

    // Networking defaults
    add(
        "Networking",
        "Private Networking",
        "No Private Networking",
        true,
        0.0,
        0.0,
    );
    add(
        "Networking",
        "Bandwidth",
        "Unlimited Traffic",
        true,
        0.0,
        0.0,
    );
    add("Networking", "IPv4", "1 IP Address", true, 0.0, 0.0);

    // Storage defaults from product spec
    if let Some(st) = storage_title {
        let primary = normalize_storage_label(st);
        if !primary.is_empty() {
            let dimension = if product_type == "vds" {
                "Storage"
            } else {
                "Storage Type"
            };
            let cat = if primary.contains("NVMe") {
                "NVMe"
            } else {
                "SSD"
            };
            add(dimension, cat, &primary, true, 0.0, 0.0);
        }
    }
    if let Some(sub) = storage_subtitle {
        static RE_MORE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)More storage available").unwrap());
        static RE_OR: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^or\s+").unwrap());
        if !RE_MORE.is_match(sub) {
            let alt = normalize_storage_label(RE_OR.replace(sub, "").as_ref());
            let dimension = if product_type == "vds" {
                "Storage"
            } else {
                "Storage Type"
            };
            let cat = if alt.contains("NVMe") { "NVMe" } else { "SSD" };
            add(dimension, cat, &alt, false, 0.0, 0.0);
        }
    }

    // Ubuntu 24.04 default image (detected from HTML presence)
    if html.contains("Ubuntu") {
        add("Image", "OS", "Ubuntu 24.04", true, 0.0, 0.0);
    }

    // Windows Server (if in HTML but not yet in classified)
    if html.contains("Windows Server") && !has_windows_in_classified {
        add("Image", "OS", "Windows Server", false, 0.0, 0.0);
    }

    classified.extend(additions);
}

// ─── Plan processing ──────────────────────────────────────────────────────────

fn process_plan(url: &str, html: &str, gap_report: &mut Vec<GapEntry>) -> Option<PlanResult> {
    let sapper = match extract_sapper(html) {
        Ok(v) => v,
        Err(e) => {
            gap_report.push(GapEntry {
                plan_sku: None,
                gap: "sapper_extract_failed".into(),
                title: None,
                error: Some(e),
            });
            return None;
        }
    };

    let slug = slug_from_url(url);
    let products = sapper.get("preloaded")?.get(0)?.get("products")?;

    let product = products.as_object()?.values().find(|p| {
        p.get("slug").and_then(Value::as_str) == Some(slug)
            || p.get("title")
                .and_then(Value::as_str)
                .map(|t| t == title_from_slug(slug))
                .unwrap_or(false)
    })?;

    let product_type = product.get("type").and_then(Value::as_str).unwrap_or("");
    let family = family_from_type(product_type);
    let product_slug = product.get("slug").and_then(Value::as_str).unwrap_or(slug);
    let product_title = product.get("title").and_then(Value::as_str).unwrap_or(slug);
    let base_monthly = product
        .get("price")
        .and_then(|p| p.get("EUR"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let fetched_at = iso_now();

    // Specs helpers
    let empty_vec = vec![];
    let specs = product
        .get("specs")
        .and_then(Value::as_array)
        .unwrap_or(&empty_vec);
    let find_spec = |spec_type: &str| -> Option<&Value> {
        specs
            .iter()
            .find(|s| s.get("type").and_then(Value::as_str) == Some(spec_type))
    };

    let cpu_str = find_spec("cpu")
        .and_then(|s| s.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let ram_str = find_spec("ram")
        .and_then(|s| s.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let port_str = find_spec("port")
        .and_then(|s| s.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let snapshot_str = find_spec("snapshot")
        .and_then(|s| s.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let storage_spec = find_spec("storage");
    let storage_title = storage_spec
        .and_then(|s| s.get("title"))
        .and_then(Value::as_str);
    let storage_subtitle = storage_spec
        .and_then(|s| s.get("subtitle"))
        .and_then(Value::as_str);

    // base_storage string (human-readable)
    let base_storage = {
        let mut parts: Vec<String> = vec![];
        if let Some(t) = storage_title {
            parts.push(normalize_storage_label(t));
        }
        if let Some(sub) = storage_subtitle {
            static RE_MORE: Lazy<Regex> =
                Lazy::new(|| Regex::new(r"(?i)More storage available").unwrap());
            static RE_OR: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^or\s+").unwrap());
            if !RE_MORE.is_match(sub) {
                parts.push(normalize_storage_label(RE_OR.replace(sub, "").as_ref()));
            }
        }
        parts.join(" or ")
    };

    // Ranks
    let plan_rank = ALL_PLAN_URLS
        .iter()
        .position(|&u| u == url)
        .map(|i| i + 1)
        .unwrap_or(0);
    let family_urls: Vec<&str> = ALL_PLAN_URLS
        .iter()
        .filter(|&&u| {
            let s = slug_from_url(u);
            match family {
                "Cloud VPS" => s.starts_with("cloud-vps-"),
                "Storage VPS" => s.starts_with("storage-vps-"),
                "Cloud VDS" => s.starts_with("vds-"),
                _ => false,
            }
        })
        .copied()
        .collect();
    let plan_family_rank = family_urls
        .iter()
        .position(|&u| u == url)
        .map(|i| i + 1)
        .unwrap_or(0);

    // Periods
    let empty_arr = vec![];
    let periods_raw = product
        .get("periods")
        .and_then(Value::as_array)
        .unwrap_or(&empty_arr);
    let periods: Vec<Value> = periods_raw
        .iter()
        .filter_map(|period| {
            let months = period.get("length").and_then(Value::as_u64)? as u32;
            let discount_eur = period
                .get("discount")
                .and_then(|d| d.get("EUR"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            let setup_eur = period
                .get("setup")
                .and_then(|s| s.get("EUR"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            let (eff, setup, total, disc) =
                monthly_price_for_period(base_monthly, months, discount_eur, setup_eur);
            Some(json!({
                "months":            months,
                "is_hidden_from_ui": months == 3,
                "effective_monthly": json_num(eff),
                "setup_fee":         json_num(setup),
                "total_period_cost": json_num(total),
                "discount_total":    json_num(disc),
            }))
        })
        .collect();

    // Classify addons
    let mut classified: Vec<OptionItem> = vec![];
    if let Some(addons) = product.get("addons").and_then(Value::as_object) {
        for (_, addon) in addons {
            let title = addon
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            let monthly = addon
                .get("price")
                .and_then(|p| p.get("EUR"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            let setup_a = addon
                .get("setupPrice")
                .and_then(|p| p.get("EUR"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            match classify_addon(title, monthly, setup_a, product_slug, product_type) {
                ClassifyResult::Include(item) => classified.push(item),
                ClassifyResult::Gap { title: t } => {
                    gap_report.push(GapEntry {
                        plan_sku: Some(product_slug.to_string()),
                        gap: "unclassified".into(),
                        title: Some(t),
                        error: None,
                    });
                }
                ClassifyResult::Skip => {}
            }
        }
    }

    inject_defaults(
        product_slug,
        product_type,
        storage_title,
        storage_subtitle,
        html,
        &mut classified,
    );

    // Mark defaults
    for item in &mut classified {
        if item.dimension == "Region" && item.option_label == "European Union" {
            item.is_default = true;
        }
        if item.dimension == "Networking"
            && matches!(
                item.option_label.as_str(),
                "Unlimited Traffic" | "No Private Networking" | "1 IP Address"
            )
        {
            item.is_default = true;
        }
        if item.dimension == "Storage Type" || item.dimension == "Storage" {
            if let Some(st) = storage_title {
                if item.option_label == normalize_storage_label(st) {
                    item.is_default = true;
                }
            }
        }
        if item.dimension == "Image" && item.option_label.eq_ignore_ascii_case("Ubuntu 24.04") {
            item.is_default = true;
        }
    }

    // Deduplicate (keep first occurrence per key) then sort
    let mut seen: HashSet<String> = HashSet::new();
    let mut deduped: Vec<OptionItem> = Vec::new();
    for item in classified {
        let key = item.dedup_key();
        if seen.insert(key) {
            deduped.push(item);
        }
    }
    // Match Node's String.localeCompare (case-insensitive) so parity_check.sh
    // doesn't flag pure-ordering differences in the Image:Apps array etc.
    deduped.sort_by_key(|a| a.sort_key().to_lowercase());
    let final_options = deduped;

    // Default config monthly cost per period
    let default_monthly_delta: f64 = final_options
        .iter()
        .filter(|o| o.is_default)
        .map(|o| o.monthly_price_delta)
        .sum();
    let default_setup_delta: f64 = final_options
        .iter()
        .filter(|o| o.is_default)
        .map(|o| o.setup_fee_delta)
        .sum();

    let mut default_monthly_by_period: Map<String, Value> = Map::new();
    let mut default_setup_by_period: Map<String, Value> = Map::new();
    for period in &periods {
        let months = period["months"].as_u64().unwrap_or(1) as u32;
        let raw = periods_raw
            .iter()
            .find(|p| p.get("length").and_then(Value::as_u64) == Some(months as u64));
        let disc_eur = raw
            .and_then(|p| p.get("discount"))
            .and_then(|d| d.get("EUR"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let setup_eur = raw
            .and_then(|p| p.get("setup"))
            .and_then(|s| s.get("EUR"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        default_monthly_by_period.insert(
            months.to_string(),
            json_num(round2(
                base_monthly - disc_eur / months as f64 + default_monthly_delta,
            )),
        );
        default_setup_by_period.insert(
            months.to_string(),
            json_num(round2(setup_eur + default_setup_delta)),
        );
    }

    // VDS storage default guard (runs after all marking is complete)
    if product_type == "vds" {
        let ok = final_options
            .iter()
            .any(|o| o.dimension == "Storage" && o.is_default);
        if !ok {
            eprintln!("  WARN   {product_slug}: no default Storage option — storageSpec.title may not match any addon label");
            gap_report.push(GapEntry {
                plan_sku: Some(product_slug.to_string()),
                gap: "missing_storage_default".into(),
                title: None,
                error: None,
            });
        }
    }

    // Group options by dimension for planConfig
    let mut by_dimension: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for item in &final_options {
        by_dimension
            .entry(item.dimension.clone())
            .or_default()
            .push(item.to_json());
    }

    let password_rules = extract_password_rules(html);

    // specs_parsed
    let storage_primary_type = if storage_title
        .map(|s| s.to_lowercase().contains("nvme"))
        .unwrap_or(false)
    {
        "NVMe"
    } else {
        "SSD"
    };
    let specs_parsed = json!({
        "cpu_count":            parse_cpu_count(cpu_str),
        "ram_gb":               parse_ram_gb(ram_str),
        "port_speed_mbps":      parse_port_speed_mbps(port_str),
        "storage_primary_gb":   parse_storage_gb(storage_title.unwrap_or("")),
        "storage_primary_type": storage_primary_type,
    });

    let base_plan = json!({
        "family":             family,
        "plan_rank":          plan_rank,
        "plan_family_rank":   plan_family_rank,
        "product_name":       product_title,
        "product_slug":       product_slug,
        "product_url":        url,
        "fetched_at":         fetched_at,
        "cpu":                cpu_str,
        "ram":                ram_str,
        "base_storage":       base_storage,
        "snapshots":          snapshot_str,
        "port":               port_str,
        "base_monthly_price": json_num(base_monthly),
        "periods":            periods,
        "specs_parsed":       specs_parsed,
        "password_rules":     password_rules,
        "source":             "sapper",
    });

    let first_setup = periods_raw
        .first()
        .and_then(|p| p.get("setup"))
        .and_then(|s| s.get("EUR"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0);

    let plan_config = json!({
        "slug":                   product_slug,
        "family":                 family,
        "title":                  product_title,
        "fetched_at":             fetched_at,
        "base_monthly_price":     base_monthly,
        "contract_periods":       base_plan["periods"],
        "options":                by_dimension,
        "password_rules":         password_rules,
        "default_monthly_by_period": default_monthly_by_period,
        "default_setup_by_period":   default_setup_by_period,
        "order_summary_default": {
            "monthly":   base_monthly,
            "one_time":  round2(first_setup),
            "due_today": round2(base_monthly + first_setup),
        },
    });

    Some(PlanResult {
        base_plan,
        final_options,
        plan_config,
        url: url.to_string(),
    })
}

// ─── Output builders ──────────────────────────────────────────────────────────

fn dimension_meta_json(generated_at: &str) -> Value {
    json!({
        "schema_version": SCHEMA_VERSION,
        "description": "Selection rules for Contabo plan configurator dimensions. \
            selection_type \"single\" = pick exactly one option; \
            \"grouped_single\" = dimension contains independent categories, each single-select.",
        "generated_at": generated_at,
        "dimensions": {
            "Region":            { "selection_type": "single",         "required": true  },
            "Storage Type":      { "selection_type": "single",         "required": true  },
            "Storage":           { "selection_type": "single",         "required": true  },
            "Data Protection":   { "selection_type": "single",         "required": false },
            "Networking": {
                "selection_type": "grouped_single", "required": true,
                "categories": {
                    "Bandwidth":            { "selection_type": "single", "required": true },
                    "IPv4":                 { "selection_type": "single", "required": true },
                    "Private Networking":   { "selection_type": "single", "required": true },
                }
            },
            "Image":             { "selection_type": "single",         "required": true  },
        }
    })
}

fn build_quick_reference(
    base_plans: &[Value],
    plan_configs: &serde_json::Map<String, Value>,
    generated_at: &str,
) -> Value {
    let plans: Vec<Value> = base_plans
        .iter()
        .map(|plan| {
            let slug = plan["product_slug"].as_str().unwrap_or("");
            let config = plan_configs
                .values()
                .find(|c| c["slug"].as_str() == Some(slug));

            let mut pricing: Map<String, Value> = Map::new();
            if let Some(periods) = plan["periods"].as_array() {
                for p in periods {
                    if p["is_hidden_from_ui"].as_bool().unwrap_or(false) {
                        continue;
                    }
                    let months = p["months"].as_u64().unwrap_or(1);
                    pricing.insert(
                        format!("{months}m"),
                        json!({
                            "effective_monthly": p["effective_monthly"],
                            "setup_fee":         p["setup_fee"],
                            "total_period":      p["total_period_cost"],
                            // alias used by the Node enrich step and downstream consumers
                            "total_period_cost": p["total_period_cost"],
                        }),
                    );
                }
            }

            let default_monthly = config
                .and_then(|c| c["default_monthly_by_period"].get("1"))
                .cloned()
                .unwrap_or_else(|| plan["base_monthly_price"].clone());

            // Savings + best-price enrichments (mirrors enrich_output.js STEP 3)
            let p1m = pricing
                .get("1m")
                .and_then(|p| p["effective_monthly"].as_f64());
            let p6m = pricing
                .get("6m")
                .and_then(|p| p["effective_monthly"].as_f64());
            let p12m = pricing
                .get("12m")
                .and_then(|p| p["effective_monthly"].as_f64());
            let savings_6m = p1m
                .zip(p6m)
                .map(|(a, b)| ((1.0 - b / a) * 100.0).round() as i64);
            let savings_12m = p1m
                .zip(p12m)
                .map(|(a, b)| ((1.0 - b / a) * 100.0).round() as i64);
            let mut candidates: Vec<(&'static str, f64)> = Vec::new();
            if let Some(v) = p1m {
                candidates.push(("1m", v));
            }
            if let Some(v) = p6m {
                candidates.push(("6m", v));
            }
            if let Some(v) = p12m {
                candidates.push(("12m", v));
            }
            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            let best = candidates.first().copied();

            json!({
                "plan_slug":            plan["product_slug"],
                "plan_rank":            plan["plan_rank"],
                "plan_family_rank":     plan["plan_family_rank"],
                "family":               plan["family"],
                "product_name":         plan["product_name"],
                "cpu_count":            plan["specs_parsed"]["cpu_count"],
                "ram_gb":               plan["specs_parsed"]["ram_gb"],
                "storage_primary_gb":   plan["specs_parsed"]["storage_primary_gb"],
                "storage_primary_type": plan["specs_parsed"]["storage_primary_type"],
                "port_speed_mbps":      plan["specs_parsed"]["port_speed_mbps"],
                "base_monthly_eur":     plan["base_monthly_price"],
                "default_monthly_eur":  default_monthly,
                "pricing":              pricing,
                "url":                  plan["product_url"],
                "fetched_at":           plan["fetched_at"],
                "savings_6m_pct":       savings_6m,
                "savings_12m_pct":      savings_12m,
                "best_price_eur":       best.map(|(_, v)| v),
                "best_price_period":    best.map(|(period, _)| period),
            })
        })
        .collect();

    json!({
        "schema_version":  SCHEMA_VERSION,
        "scraper_version": VERSION,
        "generated_at":    generated_at,
        "plan_count":      plans.len(),
        "plans":           plans,
    })
}

/// Canonical, render-ready model: one flat row per (plan × period). The HTML
/// visualizer reads only this file; `contabo_pricing_dataset.json` is reconciled
/// against it downstream to surface scrape/transform drift.
fn build_view_model(
    base_plans: &[Value],
    plan_configs: &serde_json::Map<String, Value>,
    generated_at: &str,
) -> Value {
    let mut rows: Vec<Value> = Vec::new();

    for plan in base_plans {
        let slug = plan["product_slug"].as_str().unwrap_or("");
        let config = plan_configs
            .values()
            .find(|c| c["slug"].as_str() == Some(slug));

        // Per-dimension option summary from the plan config
        let mut options_summary: Map<String, Value> = Map::new();
        if let Some(opts) = config.and_then(|c| c["options"].as_object()) {
            for (dim, list) in opts {
                let arr = match list.as_array() {
                    Some(a) => a,
                    None => continue,
                };
                let defaults: Vec<&str> = arr
                    .iter()
                    .filter(|o| o["is_default"].as_bool().unwrap_or(false))
                    .filter_map(|o| o["option_label"].as_str())
                    .collect();
                let deltas: Vec<f64> = arr
                    .iter()
                    .filter_map(|o| o["monthly_price_delta"].as_f64())
                    .collect();
                let paid_count = deltas.iter().filter(|d| **d > 0.0).count();
                let min_delta = deltas.iter().cloned().fold(f64::INFINITY, f64::min);
                let max_delta = deltas.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                options_summary.insert(dim.clone(), json!({
                    "default_label": if defaults.is_empty() { Value::Null } else { json!(defaults.join(", ")) },
                    "option_count":  arr.len(),
                    "paid_count":    paid_count,
                    "min_delta":     if min_delta.is_finite() { json_num(min_delta) } else { Value::Null },
                    "max_delta":     if max_delta.is_finite() { json_num(max_delta) } else { Value::Null },
                }));
            }
        }

        if let Some(periods) = plan["periods"].as_array() {
            for p in periods {
                rows.push(json!({
                    "plan_slug":         plan["product_slug"],
                    "family":            plan["family"],
                    "product_name":      plan["product_name"],
                    "product_url":       plan["product_url"],
                    "plan_rank":         plan["plan_rank"],
                    "plan_family_rank":  plan["plan_family_rank"],
                    "period_months":     p["months"],
                    "is_hidden_from_ui": p["is_hidden_from_ui"],
                    "effective_monthly": p["effective_monthly"],
                    "setup_fee":         p["setup_fee"],
                    "discount_total":    p["discount_total"],
                    "total_period_cost": p["total_period_cost"],
                    "base_monthly":      plan["base_monthly_price"],
                    "specs": {
                        "cpu_count":            plan["specs_parsed"]["cpu_count"],
                        "ram_gb":               plan["specs_parsed"]["ram_gb"],
                        "storage_primary_gb":   plan["specs_parsed"]["storage_primary_gb"],
                        "storage_primary_type": plan["specs_parsed"]["storage_primary_type"],
                        "port_speed_mbps":      plan["specs_parsed"]["port_speed_mbps"],
                    },
                    "options_summary": options_summary,
                }));
            }
        }
    }

    json!({
        "meta": {
            "scraper_version": VERSION,
            "schema_version":  SCHEMA_VERSION,
            "generated_at":    generated_at,
            "plan_count":      base_plans.len(),
            "row_count":       rows.len(),
        },
        "dimension_schema": dimension_meta_json(generated_at),
        "rows": rows,
    })
}

fn gap_summary(gap_report: &[GapEntry]) -> Vec<Value> {
    let mut acc: BTreeMap<String, (String, String, u32, Vec<String>)> = BTreeMap::new();
    for g in gap_report {
        let t = g.title.clone().unwrap_or_default();
        let e = g.error.clone().unwrap_or_default();
        let key = format!("{}|{}", g.gap, if !t.is_empty() { &t } else { &e });
        let entry = acc
            .entry(key)
            .or_insert_with(|| (g.gap.clone(), t.clone(), 0, vec![]));
        entry.2 += 1;
        if let Some(sku) = &g.plan_sku {
            if !entry.3.contains(sku) {
                entry.3.push(sku.clone());
            }
        }
    }
    let mut rows: Vec<Value> = acc.into_values().map(|(gap, title, count, plans)| {
        json!({ "gap": gap, "title": title, "count": count, "plans": plans })
    }).collect();
    rows.sort_by(|a, b| b["count"].as_u64().cmp(&a["count"].as_u64()));
    rows
}

fn write_csv(rows: &[Vec<String>]) -> String {
    let mut out = String::new();
    for row in rows {
        out.push_str(
            &row.iter()
                .map(|v| escape_csv(v))
                .collect::<Vec<_>>()
                .join(","),
        );
        out.push('\n');
    }
    out
}

fn get_period(plan: &Value, months: u64) -> &Value {
    static NULL: Value = Value::Null;
    plan["periods"]
        .as_array()
        .and_then(|arr| arr.iter().find(|p| p["months"].as_u64() == Some(months)))
        .unwrap_or(&NULL)
}

// ─── CloakBrowser batch fetcher ──────────────────────────────────────────────
//
// Spawns a single `node <cloak_script>` subprocess that fetches all requested
// URLs through a patched Chromium instance and writes HTML to a temp dir.
// The Rust pipeline then reads those files and processes them via the same
// process_plan() path as the reqwest pass — no data-processing changes needed.

async fn cloak_batch_fetch(
    urls: &[String],
    cloak_script: &std::path::Path,
    tmp_dir: &std::path::Path,
    gap_report: Arc<Mutex<Vec<GapEntry>>>,
    proxy: Option<&str>,
) -> Vec<Option<PlanResult>> {
    use tokio::process::Command;

    let urls_file = tmp_dir.join("cloak-urls.json");
    let html_dir = tmp_dir.join("html");

    if let Err(e) = fs::create_dir_all(&html_dir) {
        tracing::error!(error = %e, "CloakBrowser: cannot create temp HTML dir");
        return (0..urls.len()).map(|_| None).collect();
    }

    let urls_json = match serde_json::to_string(urls) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!(error = %e, "CloakBrowser: cannot serialise URL list");
            return (0..urls.len()).map(|_| None).collect();
        }
    };
    if let Err(e) = fs::write(&urls_file, &urls_json) {
        tracing::error!(error = %e, "CloakBrowser: cannot write URL list file");
        return (0..urls.len()).map(|_| None).collect();
    }

    tracing::info!(
        count = urls.len(),
        script = %cloak_script.display(),
        "launching CloakBrowser batch fetch"
    );

    let mut cmd = Command::new("node");
    cmd.args([
        cloak_script.to_str().unwrap_or("scripts/cloak-fetch.mjs"),
        "--urls",
        urls_file.to_str().unwrap_or(""),
        "--out-dir",
        html_dir.to_str().unwrap_or(""),
    ]);
    if let Some(p) = proxy {
        cmd.args(["--proxy", p]);
    }
    let status = cmd.status().await;

    match &status {
        Err(e) => {
            tracing::error!(
                error = %e,
                "CloakBrowser subprocess failed to start — is `node` installed and `npm install cloakbrowser playwright-core` complete?"
            );
            return (0..urls.len()).map(|_| None).collect();
        }
        Ok(s) if s.code() == Some(2) => {
            tracing::error!("CloakBrowser could not fetch any URLs (exit 2)");
        }
        Ok(s) => {
            tracing::info!(exit_code = ?s.code(), "CloakBrowser batch complete");
        }
    }

    let mut results = Vec::with_capacity(urls.len());
    for url in urls {
        let slug = slug_from_url(url).to_string();
        let html_path = html_dir.join(format!("{slug}.html"));

        let html = match fs::read_to_string(&html_path) {
            Ok(h) => h,
            Err(e) => {
                tracing::error!(slug = %slug, error = %e, "CloakBrowser: HTML file missing after fetch");
                gap_report.lock().await.push(GapEntry {
                    plan_sku: None,
                    gap: "cloak_fetch_failed".into(),
                    title: None,
                    error: Some(e.to_string()),
                });
                results.push(None);
                continue;
            }
        };

        let url2 = url.clone();
        let slug2 = slug.clone();
        let gr = Arc::clone(&gap_report);
        let result = tokio::task::spawn_blocking(move || {
            let mut local_gaps: Vec<GapEntry> = vec![];
            let r = process_plan(&url2, &html, &mut local_gaps);
            (r, local_gaps)
        })
        .await;

        match result {
            Ok((plan_result, local_gaps)) => {
                gr.lock().await.extend(local_gaps);
                if plan_result.is_none() {
                    tracing::error!(slug = %slug2, "CloakBrowser: __SAPPER__ parse failed");
                } else {
                    tracing::info!(slug = %slug2, "CloakBrowser: parsed OK");
                }
                results.push(plan_result);
            }
            Err(e) => {
                tracing::error!(slug = %slug2, error = %e, "CloakBrowser: parse panic");
                gap_report.lock().await.push(GapEntry {
                    plan_sku: None,
                    gap: "cloak_parse_panic".into(),
                    title: None,
                    error: Some(e.to_string()),
                });
                results.push(None);
            }
        }
    }

    results
}

// ─── Scrape orchestrator (callable from CLI and API refresh handlers) ────────

pub(crate) async fn run_scrape(opts: Opts) -> i32 {
    let mut urls: Vec<String> = if let Some(ref f) = opts.plan_urls_file {
        let raw = match fs::read_to_string(f) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("ERROR: cannot read --plan-urls-file {}: {e}", f.display());
                return EXIT_ERROR;
            }
        };
        let catalog: Value = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("ERROR: invalid JSON in --plan-urls-file: {e}");
                return EXIT_ERROR;
            }
        };
        catalog["plans"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter(|p| p["status"].as_str() == Some("active"))
            .filter_map(|p| p["url"].as_str().map(|s| s.to_string()))
            .filter(|u| !u.is_empty())
            .collect()
    } else {
        ALL_PLAN_URLS.iter().map(|s| s.to_string()).collect()
    };
    if let Some(ref slugs) = opts.plans {
        urls.retain(|u| slugs.contains(&slug_from_url(u).to_string()));
        if urls.is_empty() {
            eprintln!("No matching URLs for plans: {}", slugs.join(", "));
            return EXIT_ERROR;
        }
    }

    if !opts.dry_run {
        if let Err(e) = fs::create_dir_all(&opts.output) {
            eprintln!("Error creating output dir: {e}");
            return EXIT_ERROR;
        }
    }

    if urls.is_empty() {
        tracing::info!("No active plans to scrape — catalog is empty or all plans filtered out.");
        return EXIT_OK;
    }

    tracing::info!(version = %VERSION, "contabo-scraper (Rust)");
    tracing::info!(
        plans = urls.len(),
        concurrency = opts.concurrency,
        retries = opts.retries,
        dry_run = opts.dry_run,
        "scraping"
    );
    tracing::info!(output = %opts.output.display(), "output dir");

    let gap_report = Arc::new(Mutex::new(Vec::<GapEntry>::new()));
    let failed_fetch_urls = Arc::new(Mutex::new(Vec::<String>::new()));
    let started = Instant::now();

    // ─── reqwest pass (skipped entirely in Cloak-only mode) ──────────────────
    let mut results: Vec<Option<PlanResult>> = if opts.fetch_mode != FetchMode::Cloak {
        // Use the same browser-like User-Agent as the Node scraper. Contabo's WAF
        // returns 403 to anything that looks like a default library client
        // (e.g. reqwest's `reqwest/0.12` default) when the request comes from a
        // data-centre IP range.
        const UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";
        let mut http_builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(UA);
        if let Some(p) = &opts.proxy {
            // opts.proxy is already scheme-normalized in into_opts().
            match reqwest::Proxy::all(p) {
                Ok(proxy) => {
                    http_builder = http_builder.proxy(proxy);
                }
                Err(e) => {
                    eprintln!("ERROR: invalid SCRAPER_PROXY URL '{p}': {e}");
                    return EXIT_ERROR;
                }
            }
        }
        let client = Arc::new(http_builder.build().expect("failed to build HTTP client"));
        let semaphore = Arc::new(Semaphore::new(opts.concurrency));
        let mut handles = Vec::new();

        for url in &urls {
            let client = Arc::clone(&client);
            let semaphore = Arc::clone(&semaphore);
            let gap_report = Arc::clone(&gap_report);
            let failed_fetch_urls = Arc::clone(&failed_fetch_urls);
            let retries = opts.retries;
            let url = url.to_string();

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.expect("semaphore acquire");
                let slug = slug_from_url(&url).to_string();
                let t0 = Instant::now();
                tracing::info!(slug = %slug, "fetch");

                let html = match fetch_with_retry(&client, &url, retries).await {
                    Ok(h) => h,
                    Err(e) => {
                        tracing::error!(slug = %slug, error = %e, "fetch failed");
                        gap_report.lock().await.push(GapEntry {
                            plan_sku: None,
                            gap: "fetch_failed".into(),
                            title: None,
                            error: Some(e),
                        });
                        failed_fetch_urls.lock().await.push(url.clone());
                        return None;
                    }
                };

                // HTML parsing is synchronous + CPU-bound; run in a thread pool
                let url2 = url.clone();
                let slug2 = slug.clone();
                let gap_report = Arc::clone(&gap_report);
                let result = tokio::task::spawn_blocking(move || {
                    let mut local_gaps: Vec<GapEntry> = vec![];
                    let result = process_plan(&url2, &html, &mut local_gaps);
                    (result, local_gaps)
                })
                .await;

                match result {
                    Ok((plan_result, local_gaps)) => {
                        let mut gr = gap_report.lock().await;
                        gr.extend(local_gaps);
                        if plan_result.is_none() {
                            tracing::error!(slug = %slug2, "parse: plan not found in SAPPER payload");
                        }
                        tracing::info!(slug = %slug2, elapsed_ms = t0.elapsed().as_millis() as u64, "done");
                        plan_result
                    }
                    Err(e) => {
                        tracing::error!(slug = %slug2, error = %e, "parse panic");
                        gap_report.lock().await.push(GapEntry {
                            plan_sku: None,
                            gap: "parse_panic".into(),
                            title: None,
                            error: Some(e.to_string()),
                        });
                        None
                    }
                }
            });

            handles.push(handle);
        }

        futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap_or(None))
            .collect()
    } else {
        tracing::info!(
            fetch_mode = "cloak",
            "skipping reqwest pass — all URLs go to CloakBrowser"
        );
        vec![]
    };

    // ─── CloakBrowser pass (Auto: retry blocked URLs; Cloak: all URLs) ────────
    let failed_fetched = Arc::try_unwrap(failed_fetch_urls)
        .expect("all reqwest tasks completed before CloakBrowser pass")
        .into_inner();
    let cloak_urls: Vec<String> = match opts.fetch_mode {
        FetchMode::Cloak => urls.clone(),
        FetchMode::Auto => failed_fetched,
        FetchMode::Reqwest => vec![],
    };
    if !cloak_urls.is_empty() {
        tracing::info!(count = cloak_urls.len(), "starting CloakBrowser fallback");
        let tmp_dir = opts.output.join(".cloak-tmp");
        let cloak_results = cloak_batch_fetch(
            &cloak_urls,
            &opts.cloak_script,
            &tmp_dir,
            Arc::clone(&gap_report),
            opts.proxy.as_deref(),
        )
        .await;
        results.extend(cloak_results);
        let _ = fs::remove_dir_all(&tmp_dir);
    }

    let mut base_plans: Vec<Value> = vec![];
    let mut option_catalog: Vec<OptionItem> = vec![];
    // Insertion-ordered map (requires serde_json `preserve_order` feature).
    // Iteration order = scrape order = URL list order, which keeps the JSON
    // output deterministic across runs and aligned with the Node scraper.
    let mut plan_configs: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut seen_slugs: HashSet<String> = HashSet::new();

    for result in results.into_iter().flatten() {
        let slug = result.base_plan["product_slug"]
            .as_str()
            .unwrap_or("")
            .to_string();
        if seen_slugs.insert(slug) {
            base_plans.push(result.base_plan);
            option_catalog.extend(result.final_options);
            plan_configs.insert(result.url, result.plan_config);
        }
    }

    let gap_report = Arc::try_unwrap(gap_report).unwrap().into_inner();
    let gap_summary_json = gap_summary(&gap_report);

    let generated_at = iso_now();
    // Slug → (family, name) lookup used to enrich the option catalog with
    // canonical aliases that downstream consumers (WHMCS addon, dashboards)
    // expect alongside the bare `plan_sku`.
    let mut slug_to_family: BTreeMap<String, String> = BTreeMap::new();
    let mut slug_to_name: BTreeMap<String, String> = BTreeMap::new();
    for plan in &base_plans {
        if let Some(slug) = plan["product_slug"].as_str() {
            if let Some(fam) = plan["family"].as_str() {
                slug_to_family.insert(slug.into(), fam.into());
            }
            if let Some(name) = plan["product_name"].as_str() {
                slug_to_name.insert(slug.into(), name.into());
            }
        }
    }
    let enriched_catalog: Vec<Value> = option_catalog
        .iter()
        .map(|item| {
            let mut v = item.to_json();
            let sku = item.plan_sku.as_str();
            if let Value::Object(ref mut m) = v {
                m.insert("plan_slug".into(), json!(sku));
                m.insert(
                    "plan_family".into(),
                    json!(slug_to_family.get(sku).cloned()),
                );
                m.insert("plan_name".into(), json!(slug_to_name.get(sku).cloned()));
            }
            v
        })
        .collect();

    let dataset = json!({
        "scraper_version": VERSION,
        "schema_version":  SCHEMA_VERSION,
        "generated_at":    generated_at,
        "source":          "Contabo configurator __SAPPER__ payload + SSR HTML defaults",
        "plan_count":      base_plans.len(),
        "option_count":    option_catalog.len(),
        "gap_count":       gap_report.len(),
        "plans":           base_plans,
        "option_catalog":  enriched_catalog,
        "gap_summary":     gap_summary_json,
        "gaps":            gap_report.iter().map(|g| g.to_json()).collect::<Vec<_>>(),
    });

    let elapsed = started.elapsed().as_secs_f64();
    let failed = urls.len() - base_plans.len();

    // Preserve the previous on-disk snapshot when every fetch failed. Without
    // this guard a single bad run (WAF block, transient network outage,
    // expired TLS cert) would clobber a known-good snapshot with empty JSON
    // and downstream consumers (the API server, the WHMCS addon dashboard)
    // would suddenly see "0 plans" until the next successful scrape.
    // The skip is only triggered when nothing succeeded; partial successes
    // still write what they have so individual plan failures don't block
    // updates for the rest.
    if !opts.dry_run && base_plans.is_empty() && failed > 0 {
        tracing::error!(
            failed = failed,
            "all plan fetches failed; preserving previous snapshot in {}",
            opts.output.display()
        );
        if opts.json_out {
            let summary = json!({
                "ok": false,
                "scraper_version": VERSION,
                "schema_version":  SCHEMA_VERSION,
                "generated_at":    generated_at,
                "elapsed_seconds": (elapsed * 10.0).round() / 10.0,
                "plans_requested": urls.len(),
                "plans_scraped":   0,
                "plans_failed":    failed,
                "snapshot_preserved": true,
                "reason":          "all_plans_failed",
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&summary).unwrap_or_else(|_| "{}".into())
            );
        }
        return EXIT_ERROR;
    }

    // ─── Write output ─────────────────────────────────────────────────────────
    if !opts.dry_run {
        let write = |name: &str, data: &Value| -> Result<(), String> {
            let path = opts.output.join(name);
            let json = serde_json::to_string_pretty(data).map_err(|e| e.to_string())?;
            fs::write(&path, json).map_err(|e| e.to_string())
        };

        let base_plans_wrapped = json!({
            "schema_version":  SCHEMA_VERSION,
            "scraper_version": VERSION,
            "generated_at":    generated_at,
            "plan_count":      base_plans.len(),
            "plans":           base_plans,
        });
        let configs_wrapped = json!({
            "schema_version":  SCHEMA_VERSION,
            "scraper_version": VERSION,
            "generated_at":    generated_at,
            "plan_count":      plan_configs.len(),
            "plans":           plan_configs,
        });
        let files: &[(&str, &Value)] = &[
            ("contabo_base_plans.json", &base_plans_wrapped),
            ("contabo_configs.json", &configs_wrapped),
            ("contabo_pricing_dataset.json", &dataset),
            (
                "contabo_gap_report.json",
                &json!(gap_report.iter().map(|g| g.to_json()).collect::<Vec<_>>()),
            ),
            ("contabo_gap_summary.json", &json!(gap_summary_json)),
            (
                "contabo_dimension_schema.json",
                &dimension_meta_json(&generated_at),
            ),
            (
                "contabo_quick_reference.json",
                &build_quick_reference(&base_plans, &plan_configs, &generated_at),
            ),
            (
                "contabo_view_model.json",
                &build_view_model(&base_plans, &plan_configs, &generated_at),
            ),
        ];
        for (name, data) in files {
            if let Err(e) = write(name, data) {
                eprintln!("  ERROR writing {name}: {e}");
            }
        }

        // Base plans CSV — column set mirrors enrich_output.js STEP 5 exactly
        // (original cols + parsed spec cols + 1m_total alias).
        let base_csv_header = vec![
            "family",
            "product_name",
            "product_slug",
            "product_url",
            "fetched_at",
            "cpu",
            "ram",
            "base_storage",
            "snapshots",
            "port",
            "base_monthly_price",
            "cpu_count",
            "ram_gb",
            "storage_primary_gb",
            "storage_primary_type",
            "port_speed_mbps",
            "1m_effective_monthly",
            "1m_setup_fee",
            "1m_total",
            "3m_effective_monthly",
            "3m_setup_fee",
            "3m_total",
            "6m_effective_monthly",
            "6m_setup_fee",
            "6m_total",
            "6m_discount",
            "12m_effective_monthly",
            "12m_setup_fee",
            "12m_total",
            "12m_discount",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();

        let fnum = |v: &Value| -> String { v.as_f64().map(|x| x.to_string()).unwrap_or_default() };
        let fnum_or_zero = |v: &Value| -> String { v.as_f64().unwrap_or(0.0).to_string() };
        let int_str =
            |v: &Value| -> String { v.as_i64().map(|x| x.to_string()).unwrap_or_default() };
        let str_or_empty = |v: &Value| -> String { v.as_str().unwrap_or("").to_string() };

        let mut base_csv_rows: Vec<Vec<String>> = vec![base_csv_header];
        for plan in &base_plans {
            let p1 = get_period(plan, 1);
            let p3 = get_period(plan, 3);
            let p6 = get_period(plan, 6);
            let p12 = get_period(plan, 12);
            let sp = &plan["specs_parsed"];
            base_csv_rows.push(vec![
                str_or_empty(&plan["family"]),
                str_or_empty(&plan["product_name"]),
                str_or_empty(&plan["product_slug"]),
                str_or_empty(&plan["product_url"]),
                str_or_empty(&plan["fetched_at"]),
                str_or_empty(&plan["cpu"]),
                str_or_empty(&plan["ram"]),
                str_or_empty(&plan["base_storage"]),
                str_or_empty(&plan["snapshots"]),
                str_or_empty(&plan["port"]),
                fnum_or_zero(&plan["base_monthly_price"]),
                int_str(&sp["cpu_count"]),
                int_str(&sp["ram_gb"]),
                int_str(&sp["storage_primary_gb"]),
                str_or_empty(&sp["storage_primary_type"]),
                int_str(&sp["port_speed_mbps"]),
                p1["effective_monthly"]
                    .as_f64()
                    .or_else(|| plan["base_monthly_price"].as_f64())
                    .unwrap_or(0.0)
                    .to_string(),
                fnum_or_zero(&p1["setup_fee"]),
                fnum(&p1["total_period_cost"]),
                fnum(&p3["effective_monthly"]),
                fnum_or_zero(&p3["setup_fee"]),
                fnum(&p3["total_period_cost"]),
                fnum(&p6["effective_monthly"]),
                fnum_or_zero(&p6["setup_fee"]),
                fnum(&p6["total_period_cost"]),
                fnum_or_zero(&p6["discount_total"]),
                fnum(&p12["effective_monthly"]),
                fnum_or_zero(&p12["setup_fee"]),
                fnum(&p12["total_period_cost"]),
                fnum_or_zero(&p12["discount_total"]),
            ]);
        }
        let _ = fs::write(
            opts.output.join("contabo_base_plans.csv"),
            write_csv(&base_csv_rows),
        );

        // Option catalog CSV — column set mirrors enrich_output.js STEP 6.
        let opt_header = vec![
            "plan_sku",
            "plan_slug",
            "plan_family",
            "plan_name",
            "dimension",
            "category",
            "option_label",
            "monthly_price_delta",
            "setup_fee_delta",
            "region_group",
            "country",
            "country_code",
            "subregion",
            "is_default",
            "currency",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let mut opt_rows: Vec<Vec<String>> = vec![opt_header];
        for item in &option_catalog {
            let sku = &item.plan_sku;
            opt_rows.push(vec![
                sku.clone(),
                sku.clone(),
                slug_to_family.get(sku).cloned().unwrap_or_default(),
                slug_to_name.get(sku).cloned().unwrap_or_default(),
                item.dimension.clone(),
                item.category.clone(),
                item.option_label.clone(),
                item.monthly_price_delta.to_string(),
                item.setup_fee_delta.to_string(),
                item.region_group.clone().unwrap_or_default(),
                item.country.clone().unwrap_or_default(),
                item.country_code.clone().unwrap_or_default(),
                item.subregion.clone().unwrap_or_default(),
                if item.is_default {
                    "true".into()
                } else {
                    "false".into()
                },
                item.currency.clone(),
            ]);
        }
        let _ = fs::write(
            opts.output.join("contabo_option_catalog.csv"),
            write_csv(&opt_rows),
        );
    }

    // ─── Summary ──────────────────────────────────────────────────────────────
    tracing::info!(elapsed_seconds = elapsed, "scrape complete");
    tracing::info!(
        scraped = base_plans.len(),
        requested = urls.len(),
        failed,
        options = option_catalog.len(),
        gaps = gap_report.len(),
        "summary"
    );
    if !opts.dry_run {
        tracing::info!(output = %opts.output.display(), "output written");
    }

    if opts.json_out {
        let summary = json!({
            "scraper_version": VERSION,
            "schema_version":  SCHEMA_VERSION,
            "generated_at":    generated_at,
            "elapsed_seconds": (elapsed * 10.0).round() / 10.0,
            "plans_requested": urls.len(),
            "plans_scraped":   base_plans.len(),
            "plans_failed":    failed,
            "options_total":   option_catalog.len(),
            "gaps_total":      gap_report.len(),
            "output_dir":      if opts.dry_run { Value::Null } else { json!(opts.output.display().to_string()) },
            "dry_run":         opts.dry_run,
            "gaps":            gap_summary_json,
        });
        println!("{}", serde_json::to_string_pretty(&summary).unwrap());
    }

    if base_plans.is_empty() {
        return EXIT_ERROR;
    }
    if failed > 0 {
        return EXIT_PARTIAL;
    }
    EXIT_OK
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Determine quiet-ness from the relevant subcommand so --quiet maps to
    // RUST_LOG=warn, suppressing the info-level progress messages while
    // keeping warnings and errors visible.
    let quiet = match &cli.command {
        Some(Command::Scrape(args)) => args.quiet,
        Some(Command::Serve(_)) => false,
        None => cli.legacy.quiet,
    };
    let default_level = if quiet { "warn" } else { "info" };

    // Initialise tracing-subscriber once; non-fatal if a layer is already set.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level)),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init();

    let code = match cli.command {
        Some(Command::Scrape(args)) => run_scrape(args.into_opts()).await,
        Some(Command::Serve(args)) => api::run_serve(args).await,
        None => run_scrape(cli.legacy.into_opts()).await,
    };
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Round-trip an OptionItem with all fields populated, plus a variant where
    // all optional region fields are None, and assert both decode back to the
    // original struct. Catches accidental drift between serde derive and the
    // hand-written `to_json` consumer contract.
    #[test]
    fn option_item_serde_roundtrip() {
        let full = OptionItem {
            plan_sku: "cloud-vps-10".to_string(),
            currency: "EUR".to_string(),
            dimension: "storage".to_string(),
            category: "ssd".to_string(),
            option_label: "200 GB SSD".to_string(),
            monthly_price_delta: 1.5,
            setup_fee_delta: 0.0,
            region_group: Some("EU".to_string()),
            country: Some("Germany".to_string()),
            country_code: Some("DE".to_string()),
            subregion: Some("Western Europe".to_string()),
            is_default: false,
        };
        let json = serde_json::to_string(&full).expect("serialize full OptionItem");
        let back: OptionItem = serde_json::from_str(&json).expect("deserialize full OptionItem");
        assert_eq!(back.plan_sku, full.plan_sku);
        assert_eq!(back.currency, full.currency);
        assert_eq!(back.dimension, full.dimension);
        assert_eq!(back.category, full.category);
        assert_eq!(back.option_label, full.option_label);
        assert_eq!(back.monthly_price_delta, full.monthly_price_delta);
        assert_eq!(back.setup_fee_delta, full.setup_fee_delta);
        assert_eq!(back.region_group, full.region_group);
        assert_eq!(back.country, full.country);
        assert_eq!(back.country_code, full.country_code);
        assert_eq!(back.subregion, full.subregion);
        assert_eq!(back.is_default, full.is_default);

        let bare = OptionItem {
            plan_sku: "vds-s".to_string(),
            currency: "USD".to_string(),
            dimension: "addon".to_string(),
            category: "backup".to_string(),
            option_label: "Auto Backup".to_string(),
            monthly_price_delta: 3.5,
            setup_fee_delta: 0.0,
            region_group: None,
            country: None,
            country_code: None,
            subregion: None,
            is_default: true,
        };
        let json = serde_json::to_string(&bare).expect("serialize bare OptionItem");
        assert!(
            !json.contains("region_group"),
            "skip_serializing_if dropped region_group"
        );
        assert!(
            !json.contains("country"),
            "skip_serializing_if dropped country"
        );
        let back: OptionItem = serde_json::from_str(&json).expect("deserialize bare OptionItem");
        assert_eq!(back.region_group, None);
        assert_eq!(back.country, None);
        assert_eq!(back.country_code, None);
        assert_eq!(back.subregion, None);
        assert!(back.is_default);
        assert_eq!(back.plan_sku, bare.plan_sku);
    }

    // GapEntry with every optional field set to None must serialise without
    // any "null" tokens — downstream consumers grep for missing keys, not nulls.
    #[test]
    fn gap_entry_omits_none_fields() {
        let g = GapEntry {
            plan_sku: None,
            gap: "fetch_failed".to_string(),
            title: None,
            error: None,
        };
        let json = serde_json::to_string(&g).expect("serialize GapEntry");
        assert!(
            !json.contains("null"),
            "GapEntry with all-None optionals should not emit null: {json}"
        );
        // The single required field still has to be present.
        assert!(
            json.contains("\"gap\""),
            "gap field must always serialise: {json}"
        );
    }

    // If a previously-generated view model exists on disk, ensure we can still
    // parse it and that it carries a non-empty `rows` array. This protects
    // against accidental schema drift in the build_view_model output.
    #[test]
    fn parses_existing_view_model() {
        let path = std::path::Path::new("data/output/contabo_view_model.json");
        if !path.exists() {
            return;
        }
        let raw = fs::read_to_string(path).expect("read view model fixture");
        let v: Value = serde_json::from_str(&raw).expect("parse view model JSON");
        let rows_len = v
            .get("rows")
            .and_then(|r| r.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        assert!(
            rows_len > 0,
            "view model rows should be non-empty, got {rows_len}"
        );
    }
}
