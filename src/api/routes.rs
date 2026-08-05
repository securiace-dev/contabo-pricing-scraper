// Router assembly. Mutating routes get the bearer-token layer; everything
// else is open. CORS, compression, and tracing layers wrap the whole app.

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use super::auth::require_bearer;
use super::embed_assets;
use super::handlers;
use super::proposals;
use super::state::AppState;
use super::ServeArgs;

pub fn build_router(state: AppState, args: &ServeArgs) -> Router {
    // CORS: explicit allowlist if any origins were configured, otherwise no CORS
    let cors = if args.cors_origins.is_empty() {
        CorsLayer::new()
    } else {
        let mut layer = CorsLayer::new().allow_methods(Any).allow_headers(Any);
        for o in &args.cors_origins {
            match o.parse::<axum::http::HeaderValue>() {
                Ok(parsed) => layer = layer.allow_origin(parsed),
                Err(e) => tracing::warn!("invalid --cors-origin {o:?}: {e}"),
            }
        }
        layer
    };

    let v1 = Router::new()
        .route("/health", get(handlers::health))
        .route("/meta", get(handlers::meta))
        .route("/plans", get(handlers::list_plans))
        .route("/plans/:slug", get(handlers::get_plan))
        .route("/plans/:slug/configurator", get(handlers::get_configurator))
        .route("/options", get(handlers::list_options))
        .route("/fx", get(handlers::fx))
        .route("/quote", post(handlers::quote))
        .route("/jobs/:id", get(handlers::get_job))
        .route("/openapi.json", get(handlers::openapi))
        .route(
            "/refresh",
            post(handlers::refresh).route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                require_bearer,
            )),
        );

    let proposal_routes = Router::new()
        .route("/proposals/capabilities", get(proposals::capabilities))
        .route("/proposals/preview", post(proposals::preview))
        .route("/proposals/generate", post(proposals::generate))
        .route("/proposals/:id", get(proposals::get_job))
        .route("/proposals/:id/artifact", get(proposals::artifact))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            super::auth::require_proposal_access,
        ))
        .layer(DefaultBodyLimit::max(512 * 1024));

    Router::new()
        .nest("/api/v1", v1.merge(proposal_routes))
        .route("/", get(embed_assets::index))
        .route("/report.html", get(embed_assets::index))
        .route("/assets/:file", get(embed_assets::asset))
        .layer(cors)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
