use bytes::Bytes;
use clap::Parser;
use lyra_stream_proto::pb_stream::lyra_client::LyraClient;
use lyra_stream_proto::pb_stream::{
    AppendEventsRequest, AppendEventsRequestItem, Event, ReadEventsRequest,
};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;
use tonic::Request;
use tonic::transport::Endpoint;

/// One chaos test case, expressed as a template.
#[derive(Clone, Debug, Deserialize)]
struct Case {
    stream_id: i64,
    mode: String,
}

impl Default for Case {
    fn default() -> Self {
        Self {
            stream_id: 1,
            mode: "primary-offset".into(),
        }
    }
}

#[derive(Parser)]
struct Cli {
    /// Stream server address.
    #[arg(long, default_value = "http://127.0.0.1:7070")]
    address: String,
    /// Test case template (TOML).
    #[arg(long, default_value = "cases/primary-offset.toml")]
    case: String,
    /// Total soak duration in seconds. Defaults to 8 hours.
    #[arg(long, default_value = "28800")]
    duration_secs: u64,
    /// Number of streams; each stream gets a writer and a reader coroutine.
    #[arg(long, default_value = "10")]
    streams: u64,
    /// Pause between writer appends, in milliseconds.
    #[arg(long, default_value = "20")]
    write_pause_ms: u64,
    /// Log a reader summary every this many successful reads.
    #[arg(long, default_value = "100")]
    log_every: u64,
}

fn event(stream_id: i64, offset: i64, payload: &'static [u8]) -> AppendEventsRequest {
    AppendEventsRequest {
        items: vec![AppendEventsRequestItem {
            event: Some(Event {
                stream_id,
                term: 1,
                offset,
                payload: Some(Bytes::from_static(payload)),
                crc32: None,
                timestamp: 0,
                schema_id: 0,
            }),
            trunc: false,
            lra: 0,
        }],
    }
}

async fn connect(address: &str) -> LyraClient<tonic::transport::Channel> {
    loop {
        match Endpoint::from_shared(address.to_string())
            .unwrap()
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
        {
            Ok(channel) => return LyraClient::new(channel),
            Err(error) => {
                tracing::debug!(%error, "connection failed; retrying");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// One read attempt over a live connection; returns the offsets present.
async fn read_once(
    reader: &mut LyraClient<tonic::transport::Channel>,
    stream_id: i64,
    start: i64,
    end: i64,
) -> Result<BTreeSet<i64>, tonic::Status> {
    let request = Request::new(tokio_stream::iter(vec![ReadEventsRequest {
        stream_id,
        start_offset: start,
        end_offset: end,
    }]));
    let response = reader.read(request).await?;
    let mut stream = response.into_inner();
    let mut present = BTreeSet::new();
    while let Some(resp) = stream.message().await? {
        for e in resp.event {
            present.insert(e.offset);
        }
    }
    Ok(present)
}

/// Writer coroutine: appends events and publishes the latest write offset via
/// a watch channel so the reader can follow without polling.
async fn writer_loop(
    address: String,
    stream_id: i64,
    write_tx: watch::Sender<i64>,
    stop: Arc<AtomicBool>,
    pause_ms: u64,
) {
    let mut writer = connect(&address).await;
    let mut local = 0i64;
    while !stop.load(Ordering::Relaxed) {
        let request = Request::new(tokio_stream::iter([event(stream_id, local, b"x")]));
        match writer.append(request).await {
            Ok(response) => {
                let mut stream = response.into_inner();
                match stream.message().await {
                    Ok(Some(resp)) => {
                        let _ = write_tx.send(resp.commit_offset);
                        local += 1;
                    }
                    Ok(None) => {}
                    Err(_) => writer = connect(&address).await,
                }
            }
            Err(_) => writer = connect(&address).await,
        }
        if pause_ms > 0 {
            sleep(Duration::from_millis(pause_ms)).await;
        }
    }
}

/// Reader coroutine: wakes on each write notification, reads back the new
/// range, and advances the verified offset. An empty read where a record was
/// expected is a write/read violation.
async fn reader_loop(
    address: String,
    stream_id: i64,
    mut write_rx: watch::Receiver<i64>,
    stop: Arc<AtomicBool>,
    error_tx: mpsc::Sender<(i64, String)>,
    log_every: u64,
) {
    let mut reader = connect(&address).await;
    let mut verified: Option<i64> = None;
    let mut reads = 0u64;
    while !stop.load(Ordering::Relaxed) {
        if write_rx.changed().await.is_err() {
            break;
        }
        let target = *write_rx.borrow();
        let from = verified.map_or(target, |v| v.saturating_add(1));
        while from <= target && !stop.load(Ordering::Relaxed) {
            let present = match read_once(&mut reader, stream_id, from, target + 1).await {
                Ok(present) => present,
                Err(error) => {
                    tracing::warn!(%error, stream_id, "read failed; reconnecting");
                    reader = connect(&address).await;
                    continue;
                }
            };
            if present.is_empty() {
                let error = format!("writer offset {target} not readable from {from}");
                tracing::error!(stream_id, from, target, "write/read VIOLATION");
                let _ = error_tx.send((stream_id, error)).await;
                return;
            }
            let max_present = present.iter().max().copied().unwrap();
            verified = Some(max_present);
            reads += 1;
            if log_every > 0 && reads % log_every == 0 {
                tracing::info!(
                    stream_id,
                    write = target,
                    verified = max_present,
                    reads,
                    "reader verified"
                );
            }
            break;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();
    let case: Case = toml::from_str(&std::fs::read_to_string(&cli.case)?)?;
    tracing::info!(
        ?case,
        address = %cli.address,
        duration_secs = cli.duration_secs,
        streams = cli.streams,
        write_pause_ms = cli.write_pause_ms,
        "chaos soak starting"
    );

    if case.mode != "primary-offset" {
        eprintln!("error: unsupported mode {}", case.mode);
        std::process::exit(2);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let (error_tx, mut error_rx) = mpsc::channel(cli.streams as usize);
    for index in 0..cli.streams {
        let stream_id = case.stream_id + index as i64;
        let (write_tx, write_rx) = watch::channel(-1i64);
        tokio::spawn(writer_loop(
            cli.address.clone(),
            stream_id,
            write_tx,
            stop.clone(),
            cli.write_pause_ms,
        ));
        tokio::spawn(reader_loop(
            cli.address.clone(),
            stream_id,
            write_rx,
            stop.clone(),
            error_tx.clone(),
            cli.log_every,
        ));
    }
    drop(error_tx);

    tokio::select! {
        Some((stream_id, error)) = error_rx.recv() => {
            tracing::error!(stream_id, error = %error, "soak failed");
            stop.store(true, Ordering::Relaxed);
            std::process::exit(1);
        }
        _ = sleep(Duration::from_secs(cli.duration_secs)) => {
            tracing::info!("soak complete: duration reached, no write/read violations");
            stop.store(true, Ordering::Relaxed);
        }
    }

    Ok(())
}
