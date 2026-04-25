use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{json, Map, Value};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{sleep, Duration};

const VERSION: &str = "2.1.2";
const SCHEMA_VERSION: &str = "1.1";

// Exit codes
const EXIT_OK: i32      = 0;
const EXIT_ERROR: i32   = 1;
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

#[derive(Clone)]
struct Opts {
    output: PathBuf,
    concurrency: usize,
    retries: u32,
    plans: Option<Vec<String>>,
    quiet: bool,
    json_out: bool,
    dry_run: bool,
}

#[derive(Clone, Debug)]
struct GapEntry {
    plan_sku: Option<String>,
    gap: String,
    title: Option<String>,
    error: Option<String>,
}

impl GapEntry {
    fn to_json(&self) -> Value {
        let mut m = Map::new();
        if let Some(s) = &self.plan_sku { m.insert("plan_sku".into(), json!(s)); }
        m.insert("gap".into(), json!(&self.gap));
        if let Some(s) = &self.title { m.insert("title".into(), json!(s)); }
        if let Some(s) = &self.error { m.insert("error".into(), json!(s)); }
        Value::Object(m)
    }
}

#[derive(Clone, Debug)]
struct OptionItem {
    plan_sku: String,
    currency: String,
    dimension: String,
    category: String,
    option_label: String,
    monthly_price_delta: f64,
    setup_fee_delta: f64,
    region_group: Option<String>,
    country: Option<String>,
    country_code: Option<String>,
    subregion: Option<String>,
    is_default: bool,
}

impl OptionItem {
    fn sort_key(&self) -> String {
        format!("{}|{}|{}", self.dimension, self.category, self.option_label)
    }

    fn dedup_key(&self) -> String {
        format!("{}|{}|{}|{}", self.plan_sku, self.dimension, self.category, self.option_label)
    }

    fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("plan_sku".into(), json!(&self.plan_sku));
        m.insert("dimension".into(), json!(&self.dimension));
        m.insert("category".into(), json!(&self.category));
        m.insert("option_label".into(), json!(&self.option_label));
        m.insert("monthly_price_delta".into(), json!(self.monthly_price_delta));
        m.insert("setup_fee_delta".into(), json!(self.setup_fee_delta));
        if let Some(v) = &self.region_group  { m.insert("region_group".into(),  json!(v)); }
        if let Some(v) = &self.country       { m.insert("country".into(),        json!(v)); }
        if let Some(v) = &self.country_code  { m.insert("country_code".into(),   json!(v)); }
        if let Some(v) = &self.subregion     { m.insert("subregion".into(),      json!(v)); }
        m.insert("is_default".into(), json!(self.is_default));
        m.insert("currency".into(), json!(&self.currency));
        Value::Object(m)
    }

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
            if self.is_default { "true".into() } else { "false".into() },
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

fn parse_args() -> Result<Opts, String> {
    let args: Vec<String> = std::env::args().collect();

    // Default output: <binary location>/../../data/output, or ./data/output from cwd
    let default_output = std::env::current_dir()
        .map(|d| d.join("data").join("output"))
        .unwrap_or_else(|_| PathBuf::from("data/output"));

    let mut opts = Opts {
        output: default_output,
        concurrency: 4,
        retries: 3,
        plans: None,
        quiet: false,
        json_out: false,
        dry_run: false,
    };

    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => { print_help(); std::process::exit(EXIT_OK); }
            "-v" | "--version" => { println!("contabo-scraper v{VERSION}"); std::process::exit(EXIT_OK); }
            "-q" | "--quiet"   => opts.quiet = true,
            "-j" | "--json"    => opts.json_out = true,
            "--dry-run"        => opts.dry_run = true,
            flag @ ("-o" | "--output") => {
                i += 1;
                let val = args.get(i).ok_or_else(|| format!("{flag} requires a value"))?;
                opts.output = PathBuf::from(val);
            }
            flag @ ("-c" | "--concurrency") => {
                i += 1;
                let val = args.get(i).ok_or_else(|| format!("{flag} requires a value"))?;
                opts.concurrency = val.parse::<usize>().unwrap_or(4).max(1);
            }
            flag @ ("-r" | "--retries") => {
                i += 1;
                let val = args.get(i).ok_or_else(|| format!("{flag} requires a value"))?;
                opts.retries = val.parse::<u32>().unwrap_or(3);
            }
            flag @ ("-p" | "--plans") => {
                i += 1;
                let val = args.get(i).ok_or_else(|| format!("{flag} requires a value"))?;
                opts.plans = Some(
                    val.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
                );
            }
            other => return Err(format!("Unknown option: {other}")),
        }
        i += 1;
    }
    Ok(opts)
}

fn print_help() {
    print!(
        "\ncontabo-scraper v{VERSION}\n\n\
        USAGE\n  contabo-scraper [options]\n\n\
        OPTIONS\n\
        \x20 -o, --output <dir>       Output directory       (default: data/output/)\n\
        \x20 -c, --concurrency <n>    Parallel fetches        (default: 4)\n\
        \x20 -r, --retries <n>        Retries per URL         (default: 3)\n\
        \x20 -p, --plans <slugs>      Comma-separated plan slugs to limit scraping\n\
        \x20 -q, --quiet              Suppress progress output (stderr stays active)\n\
        \x20 -j, --json               Print JSON summary to stdout on completion\n\
        \x20     --dry-run            Fetch pages but do not write any output files\n\
        \x20 -v, --version            Print version and exit\n\
        \x20 -h, --help               Show this help\n\n\
        EXIT CODES\n\
        \x20 0  All plans scraped and written successfully\n\
        \x20 1  Fatal error — no output written\n\
        \x20 2  Partial success — some plans failed, output written for the rest\n\n\
        EXAMPLES\n\
        \x20 contabo-scraper\n\
        \x20 contabo-scraper --output ./pricing-data --concurrency 8\n\
        \x20 contabo-scraper --plans cloud-vps-10,vds-s\n\
        \x20 contabo-scraper --json --quiet > result.json\n\
        \x20 contabo-scraper --dry-run\n\n\
        AI AGENT USAGE\n\
        \x20 contabo-scraper --json --quiet | jq '.gaps'\n\n"
    );
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
    log: &(dyn Fn(&str) + Send + Sync),
) -> Result<String, String> {
    let mut last_error = String::new();
    for attempt in 0..=retries {
        match fetch_html(client, url).await {
            Ok(html) => return Ok(html),
            Err(e) => {
                last_error = e.clone();
                if attempt < retries {
                    let delay_ms = (1000u64 * 2u64.pow(attempt)).min(8000);
                    log(&format!("  [retry {}/{}] {} — {} (waiting {}ms)", attempt + 1, retries, url, e, delay_ms));
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
    let start = html.find("__SAPPER__=").ok_or("__SAPPER__ payload not found in HTML")?;

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
            let rel = html[start..].find("</script>")
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
        format!("var window={{}}; var document={{}}; var {}", snippet.trim_start())
    } else {
        format!("var window={{}}; var document={{}}; {}", snippet)
    };

    ctx.with(|ctx| -> Result<Value, String> {
        ctx.eval::<rquickjs::Value<'_>, _>(script.as_str())
            .map_err(|e| e.to_string())?;
        let json_str: String = ctx.eval("JSON.stringify(__SAPPER__)")
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
    Some(json!({ "min_length": min, "max_length": max, "alphanumeric_only": true, "no_special_chars": true }))
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
    if let Some(n) = slug.strip_prefix("cloud-vps-")   { return format!("Cloud VPS {n}"); }
    if let Some(n) = slug.strip_prefix("storage-vps-") { return format!("Storage VPS {n}"); }
    if let Some(n) = slug.strip_prefix("vds-")         { return format!("Cloud VDS {}", n.to_uppercase()); }
    slug.to_string()
}

fn family_from_type(t: &str) -> &'static str {
    match t {
        "vps"         => "Cloud VPS",
        "storage-vps" => "Storage VPS",
        "vds"         => "Cloud VDS",
        _             => "Unknown",
    }
}

fn normalize_storage_label(label: &str) -> String {
    static RE_NVME_SSD: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bNVMe SSD\b").unwrap());
    static RE_NVME:     Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bNVME\b").unwrap());
    let s = RE_NVME_SSD.replace_all(label, "NVMe");
    RE_NVME.replace_all(&s, "NVMe").trim().to_string()
}

fn iso_now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

// ─── Spec parsers ─────────────────────────────────────────────────────────────

fn parse_cpu_count(s: &str) -> Option<u32> {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)^(\d+)\s+(?:vCPU\s+|Physical\s+)?Cores?").unwrap()
    });
    RE.captures(s)?.get(1)?.as_str().parse().ok()
}

fn parse_ram_gb(s: &str) -> Option<f64> {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s*GB").unwrap());
    RE.captures(s)?.get(1)?.as_str().parse().ok()
}

fn parse_port_speed_mbps(s: &str) -> Option<u32> {
    static RE_M: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s*Mbit/s").unwrap());
    static RE_G: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s*Gbit/s").unwrap());
    if let Some(caps) = RE_M.captures(s) {
        return caps[1].parse::<f64>().ok().map(|v| v as u32);
    }
    if let Some(caps) = RE_G.captures(s) {
        return caps[1].parse::<f64>().ok().map(|v| (v * 1000.0).round() as u32);
    }
    None
}

fn parse_storage_gb(s: &str) -> Option<u32> {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s*(GB|TB)").unwrap());
    let caps = RE.captures(s)?;
    let n: f64 = caps[1].parse().ok()?;
    Some(if caps[2].eq_ignore_ascii_case("TB") { (n * 1000.0).round() as u32 } else { n as u32 })
}

fn monthly_price_for_period(base: f64, months: u32, discount_eur: f64, setup_eur: f64) -> (f64, f64, f64, f64) {
    let total     = base * months as f64 - discount_eur + setup_eur;
    let effective = (base * months as f64 - discount_eur) / months as f64;
    (round2(effective), round2(setup_eur), round2(total), round2(discount_eur))
}

// ─── Region classification ────────────────────────────────────────────────────

fn asia_iso_code(country: &str) -> String {
    match country {
        "India"     => "IN",
        "Japan"     => "JP",
        "Singapore" => "SG",
        "Korea"     => "KR",
        "Taiwan"    => "TW",
        _           => return country.chars().take(2).collect::<String>().to_uppercase(),
    }.to_string()
}

struct RegionInfo {
    region_group: &'static str,
    country: String,
    country_code: String,
    subregion: Option<String>,
}

fn classify_region(title: &str) -> Option<RegionInfo> {
    static RE_US_SUB: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^United States \(([^)]+)\)$").unwrap());
    static RE_ASIA:   Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^Asia \(([^)]+)\)$").unwrap());
    static RE_AU_SUB: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^Australia \(([^)]+)\)$").unwrap());

    let tl = title.to_lowercase();
    if tl == "european union" { return Some(RegionInfo { region_group: "Europe",    country: "European Union".into(), country_code: "EU".into(), subregion: None }); }
    if tl == "united kingdom" { return Some(RegionInfo { region_group: "Europe",    country: "United Kingdom".into(), country_code: "UK".into(), subregion: None }); }
    if tl == "germany"        { return Some(RegionInfo { region_group: "Europe",    country: "Germany".into(),        country_code: "DE".into(), subregion: None }); }
    if tl.starts_with("canada") { return Some(RegionInfo { region_group: "America", country: "Canada".into(),         country_code: "CA".into(), subregion: None }); }
    if tl == "united states"  { return Some(RegionInfo { region_group: "America",   country: "United States".into(),  country_code: "US".into(), subregion: None }); }

    if let Some(caps) = RE_US_SUB.captures(title) {
        return Some(RegionInfo {
            region_group: "America",
            country:      format!("United States ({})", &caps[1]),
            country_code: "US".into(),
            subregion:    Some(caps[1].to_string()),
        });
    }
    if let Some(caps) = RE_ASIA.captures(title) {
        let name = caps[1].to_string();
        let code = asia_iso_code(&name);
        return Some(RegionInfo { region_group: "Asia", country: name, country_code: code, subregion: None });
    }
    if RE_AU_SUB.is_match(title) || tl == "australia" {
        return Some(RegionInfo { region_group: "Australia", country: "Australia".into(), country_code: "AU".into(), subregion: None });
    }
    None
}

fn is_ignored_title(title: &str) -> bool {
    let tl = title.to_lowercase();

    // cPanel/WHM entries are ignored EXCEPT for the "5 accounts" plan
    if tl.starts_with("cpanel/whm (") && !tl.eq_ignore_ascii_case("cpanel/whm (5 accounts)") {
        return true;
    }

    static PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| vec![
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
    ]);
    PATTERNS.iter().any(|p| p.is_match(title))
}

enum ClassifyResult {
    Include(OptionItem),
    Gap { title: String },
    Skip,
}

fn classify_addon(title: &str, monthly: f64, setup: f64, plan_sku: &str, product_type: &str) -> ClassifyResult {
    let title = title.trim();
    if title.is_empty()        { return ClassifyResult::Skip; }
    if is_ignored_title(title) { return ClassifyResult::Skip; }

    let make = |dimension: &str, category: &str, label: String, ri: Option<RegionInfo>| OptionItem {
        plan_sku: plan_sku.to_string(),
        currency: "EUR".to_string(),
        dimension: dimension.to_string(),
        category: category.to_string(),
        option_label: label,
        monthly_price_delta: monthly,
        setup_fee_delta: setup,
        region_group:  ri.as_ref().map(|r| r.region_group.to_string()),
        country:       ri.as_ref().map(|r| r.country.clone()),
        country_code:  ri.as_ref().map(|r| r.country_code.clone()),
        subregion:     ri.as_ref().and_then(|r| r.subregion.clone()),
        is_default: false,
    };

    // Region
    if let Some(ri) = classify_region(title) {
        let label = ri.country.clone();
        let cat   = ri.region_group;
        return ClassifyResult::Include(make("Region", cat, label, Some(ri)));
    }

    // Storage e.g. "100 GB NVMe", "400 GB SSD"
    {
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s*(GB|TB)\s*(NVMe|SSD)(?: SSD)?$").unwrap()
        });
        if let Some(caps) = RE.captures(title) {
            let st  = if caps[3].eq_ignore_ascii_case("nvme") { "NVMe" } else { "SSD" };
            let lbl = normalize_storage_label(&format!("{} {} {}", &caps[1], &caps[2], &caps[3]));
            let dim = if product_type == "vds" { "Storage" } else { "Storage Type" };
            return ClassifyResult::Include(make(dim, st, lbl, None));
        }
    }

    // Data protection
    if title.eq_ignore_ascii_case("Auto Backup") {
        return ClassifyResult::Include(make("Data Protection", "Auto Backup", "Auto Backup".into(), None));
    }

    // Private networking
    {
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)^(No Private Networking|Private Networking Enabled)$").unwrap()
        });
        if RE.is_match(title) {
            return ClassifyResult::Include(make("Networking", "Private Networking", title.to_string(), None));
        }
    }

    // Bandwidth / traffic
    if title.contains("Traffic") || title.contains("Out + Unlimited In") {
        return ClassifyResult::Include(make("Networking", "Bandwidth", title.to_string(), None));
    }

    // IPv4 (normalize Contabo's "IP adress" typo)
    {
        static RE_MATCH: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)IP Address|IP adress").unwrap());
        static RE_TYPO:  Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bIP adress\b").unwrap());
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
        if tl.starts_with("windows server") || RE_OS.is_match(title) || tl.contains("custom image") {
            return ClassifyResult::Include(make("Image", "OS", title.to_string(), None));
        }
    }

    // Control panels
    {
        static RE_CPANEL: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^cPanel/WHM \(5 accounts\)$").unwrap());
        static RE_PLESK:  Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^Plesk Obsidian Web (Admin|Pro|Host) Edition$").unwrap());
        static RE_WEBMIN: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^Webmin( \+ LAMP)?$").unwrap());
        static RE_OTHER:  Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^(DirectAdmin|ISPConfig)$").unwrap());

        if RE_CPANEL.is_match(title) || RE_PLESK.is_match(title) || RE_WEBMIN.is_match(title) || RE_OTHER.is_match(title) {
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
            let cat = if tl.contains("node") || tl.contains("staking") { "Blockchain" } else { "Apps" };
            let lbl = if title.eq_ignore_ascii_case("Gitlab Server") { "GitLab Server".into() } else { title.to_string() };
            return ClassifyResult::Include(make("Image", cat, lbl, None));
        }
    }

    ClassifyResult::Gap { title: title.to_string() }
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
    let existing: HashSet<String> = classified.iter()
        .map(|i| format!("{}|{}", i.dimension, i.option_label))
        .collect();

    // Pre-compute Windows check before we borrow `additions` via the closure
    let has_windows_in_classified = classified.iter()
        .any(|i| i.dimension == "Image" && i.option_label.starts_with("Windows Server"));

    let mut additions: Vec<OptionItem> = Vec::new();

    let mut add = |dimension: &str, category: &str, label: &str, is_default: bool, monthly: f64, setup: f64| {
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
                region_group: None, country: None, country_code: None, subregion: None,
                is_default,
            });
        }
    };

    // No backup / data protection default
    let dp_label = if product_type == "vds" { "No Backup Space" } else { "No Data Protection" };
    add("Data Protection", "None", dp_label, true, 0.0, 0.0);

    // Networking defaults
    add("Networking", "Private Networking", "No Private Networking", true, 0.0, 0.0);
    add("Networking", "Bandwidth",          "Unlimited Traffic",     true, 0.0, 0.0);
    add("Networking", "IPv4",               "1 IP Address",          true, 0.0, 0.0);

    // Storage defaults from product spec
    if let Some(st) = storage_title {
        let primary = normalize_storage_label(st);
        if !primary.is_empty() {
            let dimension = if product_type == "vds" { "Storage" } else { "Storage Type" };
            let cat = if primary.contains("NVMe") { "NVMe" } else { "SSD" };
            add(dimension, cat, &primary, true, 0.0, 0.0);
        }
    }
    if let Some(sub) = storage_subtitle {
        static RE_MORE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)More storage available").unwrap());
        static RE_OR:   Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^or\s+").unwrap());
        if !RE_MORE.is_match(sub) {
            let alt = normalize_storage_label(&RE_OR.replace(sub, "").to_string());
            let dimension = if product_type == "vds" { "Storage" } else { "Storage Type" };
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
            gap_report.push(GapEntry { plan_sku: None, gap: "sapper_extract_failed".into(), title: None, error: Some(e) });
            return None;
        }
    };

    let slug = slug_from_url(url);
    let products = sapper.get("preloaded")?.get(0)?.get("products")?;

    let product = products.as_object()?.values().find(|p| {
        p.get("slug").and_then(Value::as_str) == Some(slug)
            || p.get("title").and_then(Value::as_str).map(|t| t == title_from_slug(slug)).unwrap_or(false)
    })?;

    let product_type  = product.get("type").and_then(Value::as_str).unwrap_or("");
    let family        = family_from_type(product_type);
    let product_slug  = product.get("slug").and_then(Value::as_str).unwrap_or(slug);
    let product_title = product.get("title").and_then(Value::as_str).unwrap_or(slug);
    let base_monthly  = product.get("price").and_then(|p| p.get("EUR")).and_then(Value::as_f64).unwrap_or(0.0);
    let fetched_at    = iso_now();

    // Specs helpers
    let empty_vec = vec![];
    let specs = product.get("specs").and_then(Value::as_array).unwrap_or(&empty_vec);
    let find_spec = |spec_type: &str| -> Option<&Value> {
        specs.iter().find(|s| s.get("type").and_then(Value::as_str) == Some(spec_type))
    };

    let cpu_str      = find_spec("cpu")     .and_then(|s| s.get("title")).and_then(Value::as_str).unwrap_or("");
    let ram_str      = find_spec("ram")     .and_then(|s| s.get("title")).and_then(Value::as_str).unwrap_or("");
    let port_str     = find_spec("port")    .and_then(|s| s.get("title")).and_then(Value::as_str).unwrap_or("");
    let snapshot_str = find_spec("snapshot").and_then(|s| s.get("title")).and_then(Value::as_str).unwrap_or("");
    let storage_spec = find_spec("storage");
    let storage_title    = storage_spec.and_then(|s| s.get("title"))   .and_then(Value::as_str);
    let storage_subtitle = storage_spec.and_then(|s| s.get("subtitle")).and_then(Value::as_str);

    // base_storage string (human-readable)
    let base_storage = {
        let mut parts: Vec<String> = vec![];
        if let Some(t) = storage_title { parts.push(normalize_storage_label(t)); }
        if let Some(sub) = storage_subtitle {
            static RE_MORE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)More storage available").unwrap());
            static RE_OR:   Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^or\s+").unwrap());
            if !RE_MORE.is_match(sub) {
                parts.push(normalize_storage_label(&RE_OR.replace(sub, "").to_string()));
            }
        }
        parts.join(" or ")
    };

    // Ranks
    let plan_rank = ALL_PLAN_URLS.iter().position(|&u| u == url).map(|i| i + 1).unwrap_or(0);
    let family_urls: Vec<&str> = ALL_PLAN_URLS.iter().filter(|&&u| {
        let s = slug_from_url(u);
        match family {
            "Cloud VPS"   => s.starts_with("cloud-vps-"),
            "Storage VPS" => s.starts_with("storage-vps-"),
            "Cloud VDS"   => s.starts_with("vds-"),
            _ => false,
        }
    }).copied().collect();
    let plan_family_rank = family_urls.iter().position(|&u| u == url).map(|i| i + 1).unwrap_or(0);

    // Periods
    let empty_arr = vec![];
    let periods_raw = product.get("periods").and_then(Value::as_array).unwrap_or(&empty_arr);
    let periods: Vec<Value> = periods_raw.iter().filter_map(|period| {
        let months       = period.get("length").and_then(Value::as_u64)? as u32;
        let discount_eur = period.get("discount").and_then(|d| d.get("EUR")).and_then(Value::as_f64).unwrap_or(0.0);
        let setup_eur    = period.get("setup")   .and_then(|s| s.get("EUR")).and_then(Value::as_f64).unwrap_or(0.0);
        let (eff, setup, total, disc) = monthly_price_for_period(base_monthly, months, discount_eur, setup_eur);
        Some(json!({
            "months":            months,
            "is_hidden_from_ui": months == 3,
            "effective_monthly": eff,
            "setup_fee":         setup,
            "total_period_cost": total,
            "discount_total":    disc,
        }))
    }).collect();

    // Classify addons
    let mut classified: Vec<OptionItem> = vec![];
    if let Some(addons) = product.get("addons").and_then(Value::as_object) {
        for (_, addon) in addons {
            let title   = addon.get("title")     .and_then(Value::as_str).unwrap_or("").trim();
            let monthly  = addon.get("price")    .and_then(|p| p.get("EUR")).and_then(Value::as_f64).unwrap_or(0.0);
            let setup_a  = addon.get("setupPrice").and_then(|p| p.get("EUR")).and_then(Value::as_f64).unwrap_or(0.0);
            match classify_addon(title, monthly, setup_a, product_slug, product_type) {
                ClassifyResult::Include(item) => classified.push(item),
                ClassifyResult::Gap { title: t } => {
                    gap_report.push(GapEntry { plan_sku: Some(product_slug.to_string()), gap: "unclassified".into(), title: Some(t), error: None });
                }
                ClassifyResult::Skip => {}
            }
        }
    }

    inject_defaults(product_slug, product_type, storage_title, storage_subtitle, html, &mut classified);

    // Mark defaults
    for item in &mut classified {
        if item.dimension == "Region" && item.option_label == "European Union" {
            item.is_default = true;
        }
        if item.dimension == "Networking" && matches!(
            item.option_label.as_str(),
            "Unlimited Traffic" | "No Private Networking" | "1 IP Address"
        ) {
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
    deduped.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
    let final_options = deduped;

    // Default config monthly cost per period
    let default_monthly_delta: f64 = final_options.iter().filter(|o| o.is_default).map(|o| o.monthly_price_delta).sum();
    let default_setup_delta:   f64 = final_options.iter().filter(|o| o.is_default).map(|o| o.setup_fee_delta).sum();

    let mut default_monthly_by_period: Map<String, Value> = Map::new();
    let mut default_setup_by_period:   Map<String, Value> = Map::new();
    for period in &periods {
        let months = period["months"].as_u64().unwrap_or(1) as u32;
        let raw = periods_raw.iter().find(|p| p.get("length").and_then(Value::as_u64) == Some(months as u64));
        let disc_eur  = raw.and_then(|p| p.get("discount")).and_then(|d| d.get("EUR")).and_then(Value::as_f64).unwrap_or(0.0);
        let setup_eur = raw.and_then(|p| p.get("setup"))   .and_then(|s| s.get("EUR")).and_then(Value::as_f64).unwrap_or(0.0);
        default_monthly_by_period.insert(months.to_string(), json!(round2(base_monthly - disc_eur / months as f64 + default_monthly_delta)));
        default_setup_by_period  .insert(months.to_string(), json!(round2(setup_eur + default_setup_delta)));
    }

    // VDS storage default guard (runs after all marking is complete)
    if product_type == "vds" {
        let ok = final_options.iter().any(|o| o.dimension == "Storage" && o.is_default);
        if !ok {
            eprintln!("  WARN   {product_slug}: no default Storage option — storageSpec.title may not match any addon label");
            gap_report.push(GapEntry { plan_sku: Some(product_slug.to_string()), gap: "missing_storage_default".into(), title: None, error: None });
        }
    }

    // Group options by dimension for planConfig
    let mut by_dimension: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for item in &final_options {
        by_dimension.entry(item.dimension.clone()).or_default().push(item.to_json());
    }

    let password_rules = extract_password_rules(html);

    // specs_parsed
    let storage_primary_type = if storage_title.map(|s| s.to_lowercase().contains("nvme")).unwrap_or(false) { "NVMe" } else { "SSD" };
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
        "base_monthly_price": base_monthly,
        "periods":            periods,
        "specs_parsed":       specs_parsed,
        "password_rules":     password_rules,
        "source":             "sapper",
    });

    let first_setup = periods_raw.first()
        .and_then(|p| p.get("setup")).and_then(|s| s.get("EUR")).and_then(Value::as_f64)
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

    Some(PlanResult { base_plan, final_options, plan_config, url: url.to_string() })
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

fn build_quick_reference(base_plans: &[Value], plan_configs: &HashMap<String, Value>, generated_at: &str) -> Value {
    let plans: Vec<Value> = base_plans.iter().map(|plan| {
        let slug = plan["product_slug"].as_str().unwrap_or("");
        let config = plan_configs.values().find(|c| c["slug"].as_str() == Some(slug));

        let mut pricing: Map<String, Value> = Map::new();
        if let Some(periods) = plan["periods"].as_array() {
            for p in periods {
                if p["is_hidden_from_ui"].as_bool().unwrap_or(false) { continue; }
                let months = p["months"].as_u64().unwrap_or(1);
                pricing.insert(format!("{months}m"), json!({
                    "effective_monthly": p["effective_monthly"],
                    "setup_fee":         p["setup_fee"],
                    "total_period":      p["total_period_cost"],
                }));
            }
        }

        let default_monthly = config
            .and_then(|c| c["default_monthly_by_period"].get("1"))
            .and_then(Value::as_f64)
            .unwrap_or_else(|| plan["base_monthly_price"].as_f64().unwrap_or(0.0));

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
        })
    }).collect();

    json!({ "generated_at": generated_at, "plans": plans })
}

fn gap_summary(gap_report: &[GapEntry]) -> Vec<Value> {
    let mut acc: BTreeMap<String, (String, String, u32, Vec<String>)> = BTreeMap::new();
    for g in gap_report {
        let t = g.title.clone().unwrap_or_default();
        let e = g.error.clone().unwrap_or_default();
        let key = format!("{}|{}", g.gap, if !t.is_empty() { &t } else { &e });
        let entry = acc.entry(key).or_insert_with(|| (g.gap.clone(), t.clone(), 0, vec![]));
        entry.2 += 1;
        if let Some(sku) = &g.plan_sku {
            if !entry.3.contains(sku) { entry.3.push(sku.clone()); }
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
        out.push_str(&row.iter().map(|v| escape_csv(v)).collect::<Vec<_>>().join(","));
        out.push('\n');
    }
    out
}

fn get_period<'a>(plan: &'a Value, months: u64) -> &'a Value {
    static NULL: Value = Value::Null;
    plan["periods"]
        .as_array()
        .and_then(|arr| arr.iter().find(|p| p["months"].as_u64() == Some(months)))
        .unwrap_or(&NULL)
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let opts = match parse_args() {
        Ok(o) => o,
        Err(e) => { eprintln!("Error: {e}"); std::process::exit(EXIT_ERROR); }
    };

    let log: Arc<dyn Fn(&str) + Send + Sync> = if opts.quiet {
        Arc::new(|_: &str| {})
    } else {
        Arc::new(|s: &str| eprintln!("{s}"))
    };

    let mut urls: Vec<&str> = ALL_PLAN_URLS.to_vec();
    if let Some(ref slugs) = opts.plans {
        urls.retain(|u| slugs.contains(&slug_from_url(u).to_string()));
        if urls.is_empty() {
            eprintln!("No matching URLs for plans: {}", slugs.join(", "));
            std::process::exit(EXIT_ERROR);
        }
    }

    if !opts.dry_run {
        if let Err(e) = fs::create_dir_all(&opts.output) {
            eprintln!("Error creating output dir: {e}");
            std::process::exit(EXIT_ERROR);
        }
    }

    log(&format!("contabo-scraper v{VERSION} (Rust)"));
    log(&format!("Scraping {} plan(s) — concurrency={} retries={}{}", urls.len(), opts.concurrency, opts.retries, if opts.dry_run { " [dry-run]" } else { "" }));
    log(&format!("Output → {}", opts.output.display()));

    let client = Arc::new(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client"),
    );

    let semaphore  = Arc::new(Semaphore::new(opts.concurrency));
    let gap_report = Arc::new(Mutex::new(Vec::<GapEntry>::new()));
    let started    = Instant::now();

    let mut handles = Vec::new();

    for &url in &urls {
        let client     = Arc::clone(&client);
        let semaphore  = Arc::clone(&semaphore);
        let gap_report = Arc::clone(&gap_report);
        let log        = Arc::clone(&log);
        let retries    = opts.retries;
        let url        = url.to_string();

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let slug    = slug_from_url(&url).to_string();
            let t0      = Instant::now();
            log(&format!("  fetch  {slug}"));

            let html = match fetch_with_retry(&client, &url, retries, log.as_ref()).await {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("  ERROR  {slug}: {e}");
                    gap_report.lock().await.push(GapEntry {
                        plan_sku: None, gap: "fetch_failed".into(), title: None, error: Some(e),
                    });
                    return None;
                }
            };

            // HTML parsing is synchronous + CPU-bound; run in a thread pool
            let url2       = url.clone();
            let slug2      = slug.clone();
            let gap_report = Arc::clone(&gap_report);
            let result = tokio::task::spawn_blocking(move || {
                let mut local_gaps: Vec<GapEntry> = vec![];
                let result = process_plan(&url2, &html, &mut local_gaps);
                (result, local_gaps)
            }).await;

            match result {
                Ok((plan_result, local_gaps)) => {
                    let mut gr = gap_report.lock().await;
                    gr.extend(local_gaps);
                    if plan_result.is_none() {
                        eprintln!("  ERROR  {slug2} (parse): plan not found in SAPPER payload");
                    }
                    log(&format!("  done   {slug2} ({}ms)", t0.elapsed().as_millis()));
                    plan_result
                }
                Err(e) => {
                    eprintln!("  ERROR  {slug2} (parse panic): {e}");
                    gap_report.lock().await.push(GapEntry {
                        plan_sku: None, gap: "parse_panic".into(), title: None, error: Some(e.to_string()),
                    });
                    None
                }
            }
        });

        handles.push(handle);
    }

    let results: Vec<Option<PlanResult>> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap_or(None))
        .collect();

    let mut base_plans:    Vec<Value>             = vec![];
    let mut option_catalog: Vec<OptionItem>        = vec![];
    let mut plan_configs:   HashMap<String, Value> = HashMap::new();
    let mut seen_slugs:     HashSet<String>        = HashSet::new();

    for result in results.into_iter().flatten() {
        let slug = result.base_plan["product_slug"].as_str().unwrap_or("").to_string();
        if seen_slugs.insert(slug) {
            base_plans.push(result.base_plan);
            option_catalog.extend(result.final_options);
            plan_configs.insert(result.url, result.plan_config);
        }
    }

    let gap_report = Arc::try_unwrap(gap_report).unwrap().into_inner();
    let gap_summary_json = gap_summary(&gap_report);

    let generated_at = iso_now();
    let dataset = json!({
        "scraper_version": VERSION,
        "schema_version":  SCHEMA_VERSION,
        "generated_at":    generated_at,
        "source":          "Contabo configurator __SAPPER__ payload + SSR HTML defaults",
        "plan_count":      base_plans.len(),
        "option_count":    option_catalog.len(),
        "gap_count":       gap_report.len(),
        "plans":           base_plans,
        "option_catalog":  option_catalog.iter().map(|i| i.to_json()).collect::<Vec<_>>(),
        "gap_summary":     gap_summary_json,
        "gaps":            gap_report.iter().map(|g| g.to_json()).collect::<Vec<_>>(),
    });

    let elapsed = started.elapsed().as_secs_f64();
    let failed  = urls.len() - base_plans.len();

    // ─── Write output ─────────────────────────────────────────────────────────
    if !opts.dry_run {
        let write = |name: &str, data: &Value| -> Result<(), String> {
            let path = opts.output.join(name);
            let json = serde_json::to_string_pretty(data).map_err(|e| e.to_string())?;
            fs::write(&path, json).map_err(|e| e.to_string())
        };

        let files: &[(&str, &Value)] = &[
            ("contabo_base_plans.json",      &json!(base_plans)),
            ("contabo_configs.json",          &json!(plan_configs)),
            ("contabo_pricing_dataset.json",  &dataset),
            ("contabo_gap_report.json",       &json!(gap_report.iter().map(|g| g.to_json()).collect::<Vec<_>>())),
            ("contabo_gap_summary.json",      &json!(gap_summary_json)),
            ("contabo_dimension_schema.json", &dimension_meta_json(&generated_at)),
            ("contabo_quick_reference.json",  &build_quick_reference(&base_plans, &plan_configs, &generated_at)),
        ];
        for (name, data) in files {
            if let Err(e) = write(name, data) {
                eprintln!("  ERROR writing {name}: {e}");
            }
        }

        // Base plans CSV
        let base_csv_header = vec![
            "family","product_name","product_slug","product_url","fetched_at",
            "cpu","ram","base_storage","snapshots","port","base_monthly_price",
            "1m_setup_fee","1m_effective_monthly",
            "3m_effective_monthly","3m_setup_fee","3m_total",
            "6m_effective_monthly","6m_setup_fee","6m_total","6m_discount",
            "12m_effective_monthly","12m_setup_fee","12m_total","12m_discount",
        ].into_iter().map(str::to_string).collect::<Vec<_>>();

        let mut base_csv_rows: Vec<Vec<String>> = vec![base_csv_header];
        for plan in &base_plans {
            let p1  = get_period(plan, 1);
            let p3  = get_period(plan, 3);
            let p6  = get_period(plan, 6);
            let p12 = get_period(plan, 12);
            base_csv_rows.push(vec![
                plan["family"].as_str().unwrap_or("").to_string(),
                plan["product_name"].as_str().unwrap_or("").to_string(),
                plan["product_slug"].as_str().unwrap_or("").to_string(),
                plan["product_url"].as_str().unwrap_or("").to_string(),
                plan["fetched_at"].as_str().unwrap_or("").to_string(),
                plan["cpu"].as_str().unwrap_or("").to_string(),
                plan["ram"].as_str().unwrap_or("").to_string(),
                plan["base_storage"].as_str().unwrap_or("").to_string(),
                plan["snapshots"].as_str().unwrap_or("").to_string(),
                plan["port"].as_str().unwrap_or("").to_string(),
                plan["base_monthly_price"].as_f64().unwrap_or(0.0).to_string(),
                p1["setup_fee"].as_f64().unwrap_or(0.0).to_string(),
                p1["effective_monthly"].as_f64().or_else(|| plan["base_monthly_price"].as_f64()).unwrap_or(0.0).to_string(),
                p3["effective_monthly"].as_f64().map(|v| v.to_string()).unwrap_or_default(),
                p3["setup_fee"].as_f64().unwrap_or(0.0).to_string(),
                p3["total_period_cost"].as_f64().map(|v| v.to_string()).unwrap_or_default(),
                p6["effective_monthly"].as_f64().map(|v| v.to_string()).unwrap_or_default(),
                p6["setup_fee"].as_f64().unwrap_or(0.0).to_string(),
                p6["total_period_cost"].as_f64().map(|v| v.to_string()).unwrap_or_default(),
                p6["discount_total"].as_f64().unwrap_or(0.0).to_string(),
                p12["effective_monthly"].as_f64().map(|v| v.to_string()).unwrap_or_default(),
                p12["setup_fee"].as_f64().unwrap_or(0.0).to_string(),
                p12["total_period_cost"].as_f64().map(|v| v.to_string()).unwrap_or_default(),
                p12["discount_total"].as_f64().unwrap_or(0.0).to_string(),
            ]);
        }
        let _ = fs::write(opts.output.join("contabo_base_plans.csv"), write_csv(&base_csv_rows));

        // Option catalog CSV
        let opt_header = vec![
            "plan_sku","dimension","category","option_label",
            "monthly_price_delta","setup_fee_delta",
            "region_group","country","country_code","subregion","is_default","currency",
        ].into_iter().map(str::to_string).collect::<Vec<_>>();
        let mut opt_rows: Vec<Vec<String>> = vec![opt_header];
        for item in &option_catalog {
            opt_rows.push(item.to_csv_row());
        }
        let _ = fs::write(opts.output.join("contabo_option_catalog.csv"), write_csv(&opt_rows));
    }

    // ─── Summary ──────────────────────────────────────────────────────────────
    log(&format!("\nDone in {elapsed:.1}s"));
    log(&format!("  Plans:   {}/{}{}", base_plans.len(), urls.len(), if failed > 0 { format!(" ({failed} failed)") } else { String::new() }));
    log(&format!("  Options: {}", option_catalog.len()));
    log(&format!("  Gaps:    {}", gap_report.len()));
    if !opts.dry_run { log(&format!("  Output:  {}", opts.output.display())); }

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

    if base_plans.is_empty() { std::process::exit(EXIT_ERROR); }
    if failed > 0            { std::process::exit(EXIT_PARTIAL); }
    std::process::exit(EXIT_OK);
}
