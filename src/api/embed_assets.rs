// Embeds the interactive report.html (and any other static assets we want
// to ship) into the binary via rust-embed. Served at `/`.

use axum::body::Body;
use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::Response;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "."]
#[include = "report.html"]
struct Assets;

pub async fn index() -> Response {
    serve_asset("report.html").await
}

pub async fn asset(Path(file): Path<String>) -> Response {
    serve_asset(&file).await
}

async fn serve_asset(name: &str) -> Response {
    match Assets::get(name) {
        Some(content) => {
            let mime = mime_for(name);
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                .header(header::CACHE_CONTROL, "public, max-age=300")
                .body(Body::from(content.data.into_owned()))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("not found"))
            .unwrap(),
    }
}

fn mime_for(name: &str) -> &'static str {
    if name.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if name.ends_with(".js") {
        "application/javascript"
    } else if name.ends_with(".css") {
        "text/css"
    } else if name.ends_with(".json") {
        "application/json"
    } else if name.ends_with(".png") {
        "image/png"
    } else if name.ends_with(".svg") {
        "image/svg+xml"
    } else {
        "application/octet-stream"
    }
}
