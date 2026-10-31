use clap::Parser;
use lyra_stream::streams::{PageCacheStream, StreamOptions, StreamStorage};
use lyra_stream_proto::pb_stream::lyra_server::{Lyra, LyraServer};
use lyra_stream_proto::pb_stream::{
    AppendEventsRequest, AppendEventsResponse, Event, FenceRequest, FenceResponse,
    ReadEventsRequest, ReadEventsResponse,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

mod observability;

#[derive(Parser)]
#[command(name = "lyra-stream", about = "Lyra streaming storage")]
struct Cli {
    /// Bind address for the gRPC server.
    #[arg(long, default_value = "0.0.0.0:7070")]
    listen: String,
    /// Legacy WAL directory. Kept for deployment compatibility; the new
    /// storage keeps the WAL inside `store_dir`.
    #[arg(long, default_value = "/tmp/lyra-wal")]
    #[allow(dead_code)]
    wal_dir: String,
    /// Legacy RocksDB index directory. No longer used.
    #[arg(long, default_value = "/tmp/lyra-stream-index")]
    #[allow(dead_code)]
    stream_index_dir: String,
    /// Directory for the WAL, data segments, index snapshot and checkpoint.
    #[arg(long, default_value = "/tmp/lyra-store")]
    store_dir: String,
    /// Size of each mmap'd data segment.
    #[arg(long, default_value = "67108864")]
    segment_size: usize,
    /// Checkpoint cadence in seconds.
    #[arg(long, default_value = "5")]
    checkpoint_interval_secs: u64,
    /// Bind address for the metrics/pprof HTTP server.
    #[arg(long, default_value = "0.0.0.0:9090")]
    metrics_listen: String,
}

/// Single-node state shared by the gRPC handlers.
#[derive(Clone)]
struct State {
    storage: Arc<dyn StreamStorage>,
    term: Arc<AtomicI64>,
    metrics: Arc<observability::Metrics>,
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[tonic::async_trait]
impl Lyra for State {
    type AppendStream = ReceiverStream<Result<AppendEventsResponse, Status>>;
    type ReadStream = ReceiverStream<Result<ReadEventsResponse, Status>>;

    async fn append(
        &self,
        request: Request<Streaming<AppendEventsRequest>>,
    ) -> Result<Response<Self::AppendStream>, Status> {
        let mut inbound = request.into_inner();
        let state = self.clone();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            while let Some(req) = inbound
                .message()
                .await
                .map_err(|error| Status::internal(error.to_string()))?
            {
                for item in req.items {
                    let Some(event) = item.event else { continue };
                    let payload = event.payload.unwrap_or_default();
                    let started = Instant::now();
                    let offset = state
                        .storage
                        .append(event.stream_id, payload)
                        .map_err(internal)?;
                    state
                        .metrics
                        .append_duration
                        .record(started.elapsed().as_secs_f64(), &[]);
                    state.metrics.append_total.add(1, &[]);

                    let response = AppendEventsResponse {
                        code: 0,
                        commit_offset: offset as i64,
                        stream_id: event.stream_id,
                        term: event.term,
                    };
                    if tx.send(Ok(response)).await.is_err() {
                        return Ok::<(), Status>(());
                    }
                }
            }
            Ok::<(), Status>(())
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn read(
        &self,
        request: Request<Streaming<ReadEventsRequest>>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let mut inbound = request.into_inner();
        let state = self.clone();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            while let Some(req) = inbound
                .message()
                .await
                .map_err(|error| Status::internal(error.to_string()))?
            {
                let records =
                    state
                        .storage
                        .read_range(req.stream_id, req.start_offset, req.end_offset);
                for (offset, payload) in records {
                    let response = ReadEventsResponse {
                        code: 0,
                        r#type: 0,
                        stream_id: req.stream_id,
                        event: vec![Event {
                            stream_id: req.stream_id,
                            term: state.term.load(Ordering::SeqCst),
                            offset: offset as i64,
                            payload: Some(payload),
                            crc32: None,
                            timestamp: 0,
                            schema_id: 0,
                        }],
                        advanced_offset: offset as i64,
                    };
                    if tx.send(Ok(response)).await.is_err() {
                        return Ok::<(), Status>(());
                    }
                }
            }
            Ok::<(), Status>(())
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn fence(
        &self,
        request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        let req = request.into_inner();
        self.term.fetch_max(req.term, Ordering::SeqCst);
        Ok(Response::new(FenceResponse {
            code: 0,
            lra: 0,
            term: self.term.load(Ordering::SeqCst),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();
    lyra_stream_cli::banner::print_banner("Stream");
    tracing::info!(
        listen = %cli.listen,
        store_dir = %cli.store_dir,
        segment_size = cli.segment_size,
        checkpoint_interval_secs = cli.checkpoint_interval_secs,
        metrics_listen = %cli.metrics_listen,
        "stream configuration"
    );
    tracing::warn!(
        legacy_wal_dir = %cli.wal_dir,
        legacy_index_dir = %cli.stream_index_dir,
        "legacy WAL and RocksDB index directories are no longer used"
    );

    let (metrics, registry) = observability::init();
    let metrics_listen: std::net::SocketAddr = cli.metrics_listen.parse()?;
    tokio::spawn(observability::serve(registry, metrics_listen));

    if cli.segment_size == 0 {
        return Err("segment size must be greater than zero".into());
    }
    let data_dir = PathBuf::from(&cli.store_dir);
    let storage: Arc<dyn StreamStorage> = Arc::new(PageCacheStream::open_standard(
        &data_dir,
        StreamOptions {
            segment_size: cli.segment_size,
        },
    )?);
    tracing::info!(
        records = storage.record_count(),
        commit_offset = storage.commit_offset(),
        "storage opened"
    );

    let checkpoint_storage = storage.clone();
    let checkpoint_interval = cli.checkpoint_interval_secs.max(1);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(checkpoint_interval));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // consume the immediate first tick
        loop {
            ticker.tick().await;
            let storage = checkpoint_storage.clone();
            match tokio::task::spawn_blocking(move || storage.checkpoint()).await {
                Ok(Ok(offset)) => tracing::debug!(
                    target: "lyra_stream::checkpoint",
                    offset,
                    "checkpoint completed"
                ),
                Ok(Err(error)) => tracing::error!(
                    target: "lyra_stream::checkpoint",
                    error = %error,
                    "checkpoint failed"
                ),
                Err(error) => tracing::error!(
                    target: "lyra_stream::checkpoint",
                    error = %error,
                    "checkpoint task failed"
                ),
            }
        }
    });

    let state = State {
        storage,
        term: Arc::new(AtomicI64::new(1)),
        metrics: Arc::new(metrics),
    };

    let address = cli.listen.parse()?;
    tracing::info!(%address, "stream server listening");
    tonic::transport::Server::builder()
        .add_service(LyraServer::new(state))
        .serve(address)
        .await?;
    Ok(())
}
