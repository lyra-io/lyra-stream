use bytes::Bytes;
use clap::Parser;
use lyra_stream::{Log, LogOptions, SegmentLog, Sequence};
use lyra_stream_proto::pb_stream::lyra_server::{Lyra, LyraServer};
use lyra_stream_proto::pb_stream::{
    AppendEventsRequest, AppendEventsResponse, Event, FenceRequest, FenceResponse,
    ReadEventsRequest, ReadEventsResponse,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

#[derive(Parser)]
#[command(name = "lyra-stream", about = "Lyra streaming storage")]
struct Cli {
    /// Bind address for the gRPC server.
    #[arg(long, default_value = "0.0.0.0:7070")]
    listen: String,
    /// Directory for the write-ahead log.
    #[arg(long, default_value = "/tmp/lyra-wal")]
    wal_dir: String,
}

/// Minimal single-node state: a WAL for durability plus an in-memory memstream
/// that serves reads.
#[derive(Clone)]
struct State {
    log: Arc<SegmentLog>,
    store: Arc<RwLock<BTreeMap<Sequence, Bytes>>>,
    term: Arc<AtomicI64>,
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
                    let sequence = state
                        .log
                        .append(payload.clone())
                        .await
                        .map_err(|error| Status::internal(error.to_string()))?;
                    state.store.write().await.insert(sequence, payload);
                    let response = AppendEventsResponse {
                        code: 0,
                        commit_offset: sequence as i64,
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
                let store = state.store.read().await;
                let start = u64::try_from(req.start_offset).unwrap_or(0);
                let end = u64::try_from(req.end_offset).unwrap_or(u64::MAX);
                for (sequence, payload) in store.range(start..end) {
                    let response = ReadEventsResponse {
                        code: 0,
                        r#type: 0,
                        stream_id: req.stream_id,
                        event: vec![Event {
                            stream_id: req.stream_id,
                            term: state.term.load(Ordering::SeqCst),
                            offset: *sequence as i64,
                            payload: Some(payload.clone()),
                            crc32: None,
                            timestamp: 0,
                            schema_id: 0,
                        }],
                        advanced_offset: *sequence as i64,
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
    let log = SegmentLog::open(LogOptions::new(&cli.wal_dir, true)).await?;
    let state = State {
        log,
        store: Arc::new(RwLock::new(BTreeMap::new())),
        term: Arc::new(AtomicI64::new(1)),
    };
    let address = cli.listen.parse()?;
    tracing::info!(%address, "stream server listening");
    tonic::transport::Server::builder()
        .add_service(LyraServer::new(state))
        .serve(address)
        .await?;
    Ok(())
}
