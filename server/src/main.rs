use bytes::Bytes;
use clap::Parser;
use lyra_stream::{Log, LogOptions, SegmentLog};
use lyra_stream_proto::pb_stream::lyra_server::{Lyra, LyraServer};
use lyra_stream_proto::pb_stream::{
    AppendEventsRequest, AppendEventsResponse, Event, FenceRequest, FenceResponse,
    ReadEventsRequest, ReadEventsResponse,
};
use rocksdb::{DB, Direction, IteratorMode, Options};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;
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
    /// Directory for the write-ahead log.
    #[arg(long, default_value = "/tmp/lyra-wal")]
    wal_dir: String,
    /// Directory for the RocksDB index.
    #[arg(long, default_value = "/tmp/lyra-db")]
    db_dir: String,
    /// Bind address for the metrics/pprof HTTP server.
    #[arg(long, default_value = "0.0.0.0:9090")]
    metrics_listen: String,
}

/// Single-node state: a WAL for append durability and RocksDB as the durable
/// record store that serves reads.
#[derive(Clone)]
struct State {
    log: Arc<SegmentLog>,
    db: Arc<DB>,
    term: Arc<AtomicI64>,
    metrics: Arc<observability::Metrics>,
}

/// Encodes `(stream_id, offset)` as a 16-byte sortable key.
fn record_key(stream_id: i64, offset: i64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&stream_id.to_be_bytes());
    key[8..].copy_from_slice(&offset.to_be_bytes());
    key
}

fn db_range(db: &DB, stream_id: i64, start: i64, end: i64) -> Vec<(i64, Bytes)> {
    let mut out = Vec::new();
    let start_key = record_key(stream_id, start);
    for item in db.iterator(IteratorMode::From(&start_key, Direction::Forward)) {
        let Ok((key, value)) = item else { break };
        if key.len() != 16 {
            break;
        }
        let sid = i64::from_be_bytes(key[..8].try_into().unwrap());
        if sid != stream_id {
            break;
        }
        let offset = i64::from_be_bytes(key[8..].try_into().unwrap());
        if offset >= end {
            break;
        }
        out.push((offset, Bytes::from(value)));
    }
    out
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
                    let sequence = state
                        .log
                        .append(payload.clone())
                        .await
                        .map_err(|error| Status::internal(error.to_string()))?;
                    state
                        .metrics
                        .append_duration
                        .record(started.elapsed().as_secs_f64(), &[]);
                    let offset = sequence as i64;
                    state
                        .db
                        .put(record_key(event.stream_id, offset), &payload)
                        .map_err(|error| Status::internal(error.to_string()))?;
                    state.metrics.append_total.add(1, &[]);
                    let response = AppendEventsResponse {
                        code: 0,
                        commit_offset: offset,
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
                let records = db_range(&state.db, req.stream_id, req.start_offset, req.end_offset);
                for (offset, payload) in records {
                    let response = ReadEventsResponse {
                        code: 0,
                        r#type: 0,
                        stream_id: req.stream_id,
                        event: vec![Event {
                            stream_id: req.stream_id,
                            term: state.term.load(Ordering::SeqCst),
                            offset,
                            payload: Some(payload),
                            crc32: None,
                            timestamp: 0,
                            schema_id: 0,
                        }],
                        advanced_offset: offset,
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
        wal_dir = %cli.wal_dir,
        db_dir = %cli.db_dir,
        metrics_listen = %cli.metrics_listen,
        "stream configuration"
    );
    let (metrics, registry) = observability::init();
    let metrics_listen: std::net::SocketAddr = cli.metrics_listen.parse()?;
    tokio::spawn(observability::serve(registry, metrics_listen));
    let log = SegmentLog::open(LogOptions::new(&cli.wal_dir, true)).await?;
    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    let db = DB::open(&db_options, &cli.db_dir)?;
    let state = State {
        log,
        db: Arc::new(db),
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
