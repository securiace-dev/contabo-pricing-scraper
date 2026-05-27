// Endpoint handlers. Most return JSON from the in-memory Snapshot held in
// AppState; /refresh kicks off a background scrape task.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::state::{AppState, JobStatus, RefreshJob};

// ── GET /api/v1/health ───────────────────────────────────────────────────────
pub async fn health() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "version": crate::VERSION,
        "schema_version": crate::SCHEMA_VERSION,
    }))
}

// ── GET /api/v1/meta ─────────────────────────────────────────────────────────
pub async fn meta(State(s): State<AppState>) -> Json<Value> {
    let snap = s.snapshot.read().await;
    Json(json!({
        "scraper_version": crate::VERSION,
        "schema_version":  crate::SCHEMA_VERSION,
        "snapshot_meta":   snap.meta.clone(),
        "data_dir":        s.data_dir.display().to_string(),
    }))
}

// ── GET /api/v1/plans ────────────────────────────────────────────────────────
#[derive(Deserialize)]
pub struct PlansQuery {
    family: Option<String>,
}

pub async fn list_plans(State(s): State<AppState>, Query(q): Query<PlansQuery>) -> Json<Value> {
    let snap = s.snapshot.read().await;
    let plans = snap
        .plans
        .get("plans")
        .cloned()
        .unwrap_or(snap.plans.clone());
    let arr = plans.as_array().cloned().unwrap_or_default();
    let filtered: Vec<Value> = arr
        .into_iter()
        .filter(|p| {
            q.family
                .as_ref()
                .is_none_or(|f| p.get("family").and_then(|v| v.as_str()) == Some(f.as_str()))
        })
        .collect();
    Json(json!(filtered))
}

// ── GET /api/v1/plans/:slug ──────────────────────────────────────────────────
pub async fn get_plan(
    State(s): State<AppState>,
    Path(slug): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let snap = s.snapshot.read().await;
    let plans = snap
        .plans
        .get("plans")
        .cloned()
        .unwrap_or(snap.plans.clone());
    let arr = plans.as_array().cloned().unwrap_or_default();
    arr.into_iter()
        .find(|p| p.get("product_slug").and_then(|v| v.as_str()) == Some(slug.as_str()))
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

// ── GET /api/v1/plans/:slug/configurator ─────────────────────────────────────
pub async fn get_configurator(
    State(s): State<AppState>,
    Path(slug): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let snap = s.snapshot.read().await;
    let configs = snap
        .configs
        .get("plans")
        .cloned()
        .unwrap_or(snap.configs.clone());
    let obj = configs.as_object().cloned().ok_or(StatusCode::NOT_FOUND)?;
    obj.into_values()
        .find(|c| c.get("slug").and_then(|v| v.as_str()) == Some(slug.as_str()))
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

// ── GET /api/v1/options ──────────────────────────────────────────────────────
pub async fn list_options(State(s): State<AppState>) -> Json<Value> {
    let snap = s.snapshot.read().await;
    Json(
        snap.option_catalog
            .get("option_catalog")
            .cloned()
            .unwrap_or(Value::Null),
    )
}

// ── GET /api/v1/fx ───────────────────────────────────────────────────────────
pub async fn fx() -> Json<Value> {
    // Phase 2 will fetch from frankfurter with cache; placeholder for now.
    Json(json!({
        "source": "frankfurter.app",
        "note":   "phase-2 placeholder — wire live fetch",
    }))
}

// ── POST /api/v1/quote ───────────────────────────────────────────────────────
#[derive(Deserialize)]
pub struct QuoteRequest {
    pub plan_slug: String,
    pub period_months: u32,
    #[serde(default)]
    #[allow(dead_code)]
    pub selections: serde_json::Map<String, Value>,
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default)]
    pub gst: bool,
    #[serde(default)]
    pub fx_markup: f64,
    #[serde(default)]
    pub fx_rate: Option<f64>,
}
fn default_currency() -> String {
    "EUR".into()
}

#[derive(Serialize)]
pub struct QuoteResponse {
    pub plan_slug: String,
    pub period_months: u32,
    pub currency: String,
    pub base_monthly_eur: f64,
    pub configured_monthly_eur: f64,
    pub setup_fee_eur: f64,
    pub gst_amount_eur: f64,
    pub fx_rate: Option<f64>,
    pub fx_markup: f64,
    pub final_monthly: f64,
    pub final_total: f64,
    pub breakdown: Vec<String>,
}

pub async fn quote(
    State(s): State<AppState>,
    Json(req): Json<QuoteRequest>,
) -> Result<Json<QuoteResponse>, StatusCode> {
    let snap = s.snapshot.read().await;
    let vm = snap
        .view_model
        .get("rows")
        .and_then(|v| v.as_array())
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let row = vm
        .iter()
        .find(|r| {
            r.get("plan_slug").and_then(|v| v.as_str()) == Some(req.plan_slug.as_str())
                && r.get("period_months").and_then(|v| v.as_u64()) == Some(req.period_months as u64)
        })
        .ok_or(StatusCode::NOT_FOUND)?;

    let base = row
        .get("effective_monthly")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let setup = row.get("setup_fee").and_then(|v| v.as_f64()).unwrap_or(0.0);

    // Phase 2 will apply selection deltas from configs.json; for now configured == base
    let configured = base;
    let gst_amt = if req.gst { configured * 0.18 } else { 0.0 };
    let after_gst = configured + gst_amt;
    let mut breakdown = vec![format!("base €{:.2}/mo", base)];
    if req.gst {
        breakdown.push(format!("+18% GST €{:.2}", gst_amt));
    }

    let (final_monthly, fx_rate) = match req.currency.as_str() {
        "EUR" => (after_gst, None),
        "INR" => {
            let rate = req.fx_rate.unwrap_or(0.0);
            let with_markup = rate * (1.0 + req.fx_markup);
            breakdown.push(format!(
                "× EUR→INR {:.4} ({}% markup)",
                with_markup,
                (req.fx_markup * 100.0).round()
            ));
            (after_gst * with_markup, Some(rate))
        }
        other => {
            tracing::warn!("unknown currency: {other}");
            (after_gst, None)
        }
    };

    let final_total = final_monthly * req.period_months as f64 + setup * fx_rate.unwrap_or(1.0);

    Ok(Json(QuoteResponse {
        plan_slug: req.plan_slug,
        period_months: req.period_months,
        currency: req.currency,
        base_monthly_eur: base,
        configured_monthly_eur: configured,
        setup_fee_eur: setup,
        gst_amount_eur: gst_amt,
        fx_rate,
        fx_markup: req.fx_markup,
        final_monthly,
        final_total,
        breakdown,
    }))
}

// ── POST /api/v1/refresh ─────────────────────────────────────────────────────
// Returns 202 Accepted — the scrape runs asynchronously; the response only
// confirms the job was queued, not that it has finished. Both paths (newly
// queued + idempotent return of an in-flight job) use the same status: from
// the client's perspective the request was accepted; the only difference is
// whether the worker is starting fresh or already mid-flight.
pub async fn refresh(
    State(s): State<AppState>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    let mut guard = s.refresh_lock.lock().await;
    if let Some(j) = &guard.current_job {
        if matches!(j.status, JobStatus::Queued | JobStatus::Running) {
            return Ok((
                StatusCode::ACCEPTED,
                Json(json!({ "job_id": j.id, "status": j.status })),
            ));
        }
    }
    let id = uuid::Uuid::new_v4().to_string();
    let job = RefreshJob {
        id: id.clone(),
        started_at: chrono::Utc::now(),
        status: JobStatus::Queued,
    };
    guard.current_job = Some(job);
    drop(guard);

    let s2 = s.clone();
    let job_id = id.clone();
    tokio::spawn(async move {
        {
            let mut g = s2.refresh_lock.lock().await;
            if let Some(j) = g.current_job.as_mut() {
                j.status = JobStatus::Running;
            }
        }

        let opts = crate::Opts {
            output: s2.data_dir.clone(),
            concurrency: 4,
            retries: 3,
            plans: None,
            plan_urls_file: None,
            quiet: true,
            json_out: false,
            dry_run: false,
            fetch_mode: crate::FetchMode::Reqwest,
            cloak_script: std::path::PathBuf::from("scripts/cloak-fetch.mjs"),
            proxy: None,
        };
        let code = crate::run_scrape(opts).await;

        // After successful scrape, reload the in-memory snapshot from disk.
        let new_snap = super::state::load_snapshot_from_disk(&s2.data_dir).ok();

        let mut g = s2.refresh_lock.lock().await;
        if let Some(j) = g.current_job.as_mut() {
            j.status = if code == 0 || code == 2 {
                JobStatus::Succeeded
            } else {
                JobStatus::Failed
            };
        }
        drop(g);

        if let Some(ns) = new_snap {
            *s2.snapshot.write().await = ns;
            tracing::info!(%job_id, "snapshot reloaded after refresh");
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "job_id": id, "status": "queued" })),
    ))
}

// ── GET /api/v1/jobs/:id ─────────────────────────────────────────────────────
pub async fn get_job(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let g = s.refresh_lock.lock().await;
    match &g.current_job {
        Some(j) if j.id == id => Ok(Json(json!({
            "id": j.id, "status": j.status, "started_at": j.started_at,
        }))),
        _ => Err(StatusCode::NOT_FOUND),
    }
}

// ── GET /api/v1/openapi.json ─────────────────────────────────────────────────
pub async fn openapi() -> Json<Value> {
    // Phase 2 will generate via utoipa; for now ship a hand-written stub
    Json(json!({
        "openapi": "3.0.0",
        "info":    { "title": "Contabo Pricing API", "version": crate::VERSION },
        "paths":   { /* populated in Phase 2 */ },
    }))
}

// Unused import suppressor — Arc is referenced once auth wiring lands
#[allow(dead_code)]
fn _arc_marker(_: Arc<()>) {}
