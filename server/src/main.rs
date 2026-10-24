use bytes::Bytes;
use clap::Parser;
use lyra_stream::storage::{FileStore, Location, Storage};
use lyra_stream::{Log, LogOptions, SegmentLog};
use lyra_stream_proto::pb_stream::lyra_server::{Lyra, LyraServer};
use lyra_stream_proto::pb_stream::{
    AppendEventsRequest, AppendEventsResponse, Event, FenceRequest, FenceResponse,
    ReadEventsRequest, ReadEventsResponse,
};
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions};
use std::path::{Path, PathBuf};
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
    /// Directory for the stream index (RocksDB).
    #[arg(long, default_value = "/tmp/lyra-stream-index")]
    stream_index_dir: String,
    /// Directory for the tiered stream store files.
    #[arg(long, default_value = "/tmp/lyra-store")]
    store_dir: String,
    /// L0 memstream size that triggers a switch.
    #[arg(long, default_value = "67108864")]
    memstream_flush_bytes: u64,
    /// L1 memstream size that triggers L1 -> L2 compaction.
    #[arg(long, default_value = "67108864")]
    l1_flush_bytes: u64,
    /// Bind address for the metrics/pprof HTTP server.
    #[arg(long, default_value = "0.0.0.0:9090")]
    metrics_listen: String,
}

/// Single-node state shared by the gRPC handlers and the compaction chain.
#[derive(Clone)]
struct State {
    log: Arc<SegmentLog>,
    index: Arc<DB>,
    storage: Arc<Storage>,
    l0_tx: mpsc::UnboundedSender<()>,
    term: Arc<AtomicI64>,
    metrics: Arc<observability::Metrics>,
}

/// Encodes `(stream_id, offset)` as a 16-byte sortable primary index key.
fn record_key(stream_id: i64, offset: i64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&stream_id.to_be_bytes());
    key[8..].copy_from_slice(&offset.to_be_bytes());
    key
}

/// Encodes the WAL envelope: stream id followed by the raw payload.
fn envelope(stream_id: i64, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + payload.len());
    bytes.extend_from_slice(&stream_id.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn write_options() -> WriteOptions {
    let mut options = WriteOptions::new();
    options.disable_wal(true);
    options
}

impl State {
    /// Resolves a primary index location into the actual record payload.
    fn fetch(&self, stream_id: i64, offset: i64, location: &[u8]) -> Option<Bytes> {
        let location = Location::decode(location)?;
        self.storage.get(location, stream_id, offset)
    }

    /// Reads `[start, end)` records for a stream from the index + store.
    fn read_range(&self, stream_id: i64, start: i64, end: i64) -> Vec<(i64, Bytes)> {
        let mut out = Vec::new();
        let start_key = record_key(stream_id, start);
        for item in self
            .index
            .iterator(IteratorMode::From(&start_key, Direction::Forward))
        {
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
            if let Some(payload) = self.fetch(stream_id, offset, &value) {
                out.push((offset, payload));
            }
        }
        out
    }
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
                        .append(Bytes::from(envelope(event.stream_id, &payload)))
                        .await
                        .map_err(|error| Status::internal(error.to_string()))?;
                    state
                        .metrics
                        .append_duration
                        .record(started.elapsed().as_secs_f64(), &[]);

                    let offset = sequence as i64;
                    if state.storage.put(event.stream_id, offset, payload.clone()) {
                        let _ = state.l0_tx.send(());
                    }
                    let mut batch = WriteBatch::default();
                    batch.put(
                        record_key(event.stream_id, offset),
                        Location::memstream(payload.len() as u32).encode(),
                    );
                    state
                        .index
                        .write_opt(batch, &write_options())
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
                let records = state.read_range(req.stream_id, req.start_offset, req.end_offset);
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
        index_dir = %cli.stream_index_dir,
        store_dir = %cli.store_dir,
        memstream_flush_bytes = cli.memstream_flush_bytes,
        l1_flush_bytes = cli.l1_flush_bytes,
        metrics_listen = %cli.metrics_listen,
        "stream configuration"
    );

    let (metrics, registry) = observability::init();
    let metrics_listen: std::net::SocketAddr = cli.metrics_listen.parse()?;
    tokio::spawn(observability::serve(registry, metrics_listen));

    let log = SegmentLog::open(LogOptions::new(&cli.wal_dir, true)).await?;
    let _ = std::fs::remove_dir_all(&cli.store_dir);
    std::fs::create_dir_all(&cli.store_dir)?;

    let mut index_options = Options::default();
    index_options.create_if_missing(true);
    let index = DB::open(&index_options, &cli.stream_index_dir)?;
    let file_store = Arc::new(FileStore::new(PathBuf::from(&cli.store_dir)));
    let storage = Arc::new(Storage::new(
        cli.memstream_flush_bytes,
        cli.l1_flush_bytes,
        file_store,
    ));

    // Rebuild the L0 memstream and primary index from the durable WAL.
    let replayed = lyra_stream::wal::replay(Path::new(&cli.wal_dir))?;
    let mut batch = WriteBatch::default();
    let mut replayed_records = 0usize;
    for (sequence, record) in replayed {
        if record.len() < 8 {
            return Err(format!("WAL record {sequence} is missing its stream id").into());
        }
        let stream_id = i64::from_be_bytes(record[..8].try_into().unwrap());
        let payload = Bytes::copy_from_slice(&record[8..]);
        let offset = sequence as i64;
        storage.replay_put(stream_id, offset, payload.clone());
        batch.put(
            record_key(stream_id, offset),
            Location::memstream(payload.len() as u32).encode(),
        );
        replayed_records += 1;
        if batch.len() >= 1024 {
            index.write_opt(batch, &write_options())?;
            batch = WriteBatch::default();
        }
    }
    if replayed_records > 0 {
        index.write_opt(batch, &write_options())?;
    }
    tracing::info!(replayed_records, "WAL replay complete");

    let (l0_tx, mut l0_rx) = mpsc::unbounded_channel::<()>();
    let (l1_tx, mut l1_rx) = mpsc::unbounded_channel::<()>();
    let state = State {
        log,
        index: Arc::new(index),
        storage: storage.clone(),
        l0_tx,
        term: Arc::new(AtomicI64::new(1)),
        metrics: Arc::new(metrics),
    };

    // Reactive compaction chain: L0 -> L1 -> L2. No polling loop.
    let l0_state = state.clone();
    tokio::spawn(async move {
        tracing::info!(target: "lyra_stream::compaction", stage = "l0", "compaction component started");
        while l0_rx.recv().await.is_some() {
            let moved = l0_state.storage.promote_backup();
            let mut batch = WriteBatch::default();
            for (stream_id, offset, length) in &moved {
                batch.put(
                    record_key(*stream_id, *offset),
                    Location::immutable(*length).encode(),
                );
            }
            if !moved.is_empty() {
                match l0_state.index.write_opt(batch, &write_options()) {
                    Ok(()) => tracing::info!(
                        target: "lyra_stream::compaction",
                        moved = moved.len(),
                        "compacted L0 -> L1"
                    ),
                    Err(error) => tracing::error!(
                        target: "lyra_stream::compaction",
                        error = %error,
                        "L0 index update failed"
                    ),
                }
            }
            let _ = l1_tx.send(());
        }
        tracing::info!(target: "lyra_stream::compaction", stage = "l0", "compaction component stopped");
    });

    let l1_state = state.clone();
    tokio::spawn(async move {
        tracing::info!(target: "lyra_stream::compaction", stage = "l1", "compaction component started");
        while l1_rx.recv().await.is_some() {
            if !l1_state.storage.l1_needs_compaction() {
                continue;
            }
            let locations = l1_state.storage.compact_l1();
            let mut batch = WriteBatch::default();
            for (stream_id, offset, location) in &locations {
                batch.put(record_key(*stream_id, *offset), location.encode());
            }
            if !locations.is_empty() {
                match l1_state.index.write_opt(batch, &write_options()) {
                    Ok(()) => {
                        l1_state.storage.discard_l1(&locations);
                        tracing::info!(
                            target: "lyra_stream::compaction",
                            moved = locations.len(),
                            "compacted L1 -> L2"
                        )
                    }
                    Err(error) => tracing::error!(
                        target: "lyra_stream::compaction",
                        error = %error,
                        "L1 index update failed"
                    ),
                }
            }
        }
        tracing::info!(target: "lyra_stream::compaction", stage = "l1", "compaction component stopped");
    });

    let address = cli.listen.parse()?;
    tracing::info!(%address, "stream server listening");
    tonic::transport::Server::builder()
        .add_service(LyraServer::new(state))
        .serve(address)
        .await?;
    Ok(())
}
