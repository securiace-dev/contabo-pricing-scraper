use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

fn temp_workspace() -> PathBuf {
    let path = std::env::temp_dir().join(format!("contabo-cli-contract-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path).expect("create CLI contract workspace");
    path
}

fn fixture_html() -> String {
    let sapper = json!({
        "preloaded": [{
            "products": {
                "canonical-core-4": {
                    "slug": "cloud-vps-core-4",
                    "title": "Cloud VPS 4",
                    "type": "vps",
                    "price": { "EUR": 5.5 },
                    "periods": [{
                        "length": 1,
                        "discount": { "EUR": 0.0 },
                        "setup": { "EUR": 0.0 }
                    }],
                    "specs": [
                        { "type": "cpu", "title": "4 vCPU Cores" },
                        { "type": "ram", "title": "8 GB RAM" },
                        { "type": "storage", "title": "100 GB SSD", "subtitle": "More storage available" },
                        { "type": "snapshot", "title": "1 Snapshot" },
                        { "type": "port", "title": "200 Mbit/s Port" }
                    ],
                    "addons": {}
                }
            }
        }]
    });
    format!(
        "<!doctype html><html><body>Ubuntu 24.04<script>__SAPPER__={sapper};</script></body></html>"
    )
}

fn spawn_fixture_server(body: String) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
    let addr = listener.local_addr().expect("fixture server address");
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept scraper request");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set fixture read timeout");
        let mut request = [0_u8; 8192];
        let _ = stream.read(&mut request).expect("read scraper request");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write fixture response");
    });
    (format!("http://{addr}/vps/cloud-vps-core-4"), join)
}

fn write_plan_catalog(path: &Path, url: &str) {
    let catalog = json!({
        "schema_version": "1.1",
        "plans": [{
            "slug": "cloud-vps-core-4",
            "url": url,
            "family": "Core VPS",
            "status": "active"
        }]
    });
    fs::write(
        path,
        serde_json::to_vec_pretty(&catalog).expect("serialize plan catalog"),
    )
    .expect("write plan catalog");
}

#[test]
fn dry_run_mixed_flag_positions_writes_nothing_and_prints_json_summary() {
    let workspace = temp_workspace();
    let output_dir = workspace.join("must-not-exist");
    let catalog_path = workspace.join("plans.json");
    let (url, server) = spawn_fixture_server(fixture_html());
    write_plan_catalog(&catalog_path, &url);

    let output = Command::new(env!("CARGO_BIN_EXE_contabo-scraper"))
        .arg("--dry-run")
        .arg("--output")
        .arg(&output_dir)
        .arg("scrape")
        .arg("--json")
        .arg("--retries")
        .arg("0")
        .arg("--plan-urls-file")
        .arg(&catalog_path)
        .env_remove("CONTABO_OUTPUT")
        .env_remove("SCRAPER_CONCURRENCY")
        .env_remove("SCRAPER_RETRIES")
        .env_remove("FETCH_MODE")
        .env_remove("CLOAK_SCRIPT")
        .env_remove("SCRAPER_PROXY")
        .output()
        .expect("run scraper binary");

    server.join().expect("fixture server exits");
    assert!(
        output.status.success(),
        "scraper failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output_dir.exists(),
        "--dry-run created the requested output directory"
    );

    let summary: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout is not a JSON summary: {error}\n{}",
            String::from_utf8_lossy(&output.stdout)
        )
    });
    assert_eq!(summary["dry_run"], true);
    assert_eq!(summary["output_dir"], Value::Null);
    assert_eq!(summary["plans_requested"], 1);
    assert_eq!(summary["plans_scraped"], 1);
    assert_eq!(summary["plans_failed"], 0);

    fs::remove_dir_all(&workspace).expect("remove CLI contract workspace");
}
