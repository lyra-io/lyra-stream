use axum::Router;
use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use opentelemetry::metrics::{Histogram, MeterProvider};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Registry, TextEncoder};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    pub append_total: opentelemetry::metrics::Counter<u64>,
    pub append_duration: Histogram<f64>,
}

pub fn init() -> (Metrics, Registry) {
    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .expect("build prometheus exporter");
    let provider = SdkMeterProvider::builder().with_reader(exporter).build();
    let meter = provider.meter("lyra-stream");
    // The provider must outlive the server: its drop shuts the reader down.
    std::mem::forget(provider);
    let metrics = Metrics {
        append_total: meter.u64_counter("lyra_stream_append_total").build(),
        append_duration: meter
            .f64_histogram("lyra_stream_append_duration_seconds")
            .build(),
    };
    (metrics, registry)
}

async fn metrics_handler(State(registry): State<Arc<Registry>>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let families = registry.gather();
    let text = encoder.encode_to_string(&families).unwrap_or_default();
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text)
}

#[cfg(target_os = "linux")]
async fn pprof_handler(Query(params): Query<HashMap<String, String>>) -> Response {
    let seconds: u64 = params
        .get("seconds")
        .and_then(|s| s.parse().ok())
        .unwrap_or(10)
        .min(60);
    let data = tokio::task::spawn_blocking(move || {
        let Ok(guard) = pprof::ProfilerGuard::new(99) else {
            return Vec::new();
        };
        std::thread::sleep(std::time::Duration::from_secs(seconds));
        let Ok(report) = guard.report().build() else {
            return Vec::new();
        };
        let mut buf = Vec::new();
        let _ = report.flamegraph(&mut buf);
        buf
    })
    .await
    .unwrap_or_default();
    ([(header::CONTENT_TYPE, "image/svg+xml")], data).into_response()
}

#[cfg(not(target_os = "linux"))]
async fn pprof_handler(_: Query<HashMap<String, String>>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain")],
        "pprof is only supported on Linux",
    )
        .into_response()
}

pub fn router(registry: Registry) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/debug/pprof/profile", get(pprof_handler))
        .with_state(Arc::new(registry))
}

pub async fn serve(registry: Registry, listen: SocketAddr) {
    let app = router(registry);
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .expect("bind metrics listener");
    axum::serve(listener, app).await.expect("serve metrics");
}
