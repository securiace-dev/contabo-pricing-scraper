// Bearer-token auth middleware. Applied via `Router::route_layer` only to
// mutating endpoints (POST /refresh). When no token is configured the layer
// rejects every request to those routes with 503.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use super::state::AppState;

#[derive(Clone)]
pub struct AuthConfig {
    pub token: Option<String>,
}

impl AuthConfig {
    pub fn from_args(args: &super::ServeArgs) -> anyhow::Result<Self> {
        if let Some(t) = &args.auth_token {
            return Ok(Self {
                token: Some(t.clone()),
            });
        }
        if let Some(f) = &args.auth_token_file {
            let raw = std::fs::read_to_string(f)?;
            return Ok(Self {
                token: Some(raw.trim().to_string()),
            });
        }
        Ok(Self { token: None })
    }
}

pub async fn require_bearer(
    State(s): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let Some(expected) = s.auth.token.as_deref() else {
        // No token configured → mutating endpoints are disabled
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    };

    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));

    match presented {
        Some(t) if constant_time_eq(t.as_bytes(), expected.as_bytes()) => Ok(next.run(req).await),
        _ => Ok(Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header(header::WWW_AUTHENTICATE, "Bearer realm=\"contabo-pricing\"")
            .body(Body::empty())
            .unwrap()),
    }
}

/// Proposal generation is available without a token only when the server is
/// explicitly bound to loopback. A configured bearer token enables it for a
/// deliberately exposed service without turning the endpoint into an open
/// process-spawning surface.
pub async fn require_proposal_access(
    State(s): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if s.auth.token.is_some() {
        return require_bearer(State(s), req, next).await;
    }
    if !s.proposal_local_only {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(next.run(req).await)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
