use bytes::Bytes;
use clap::Parser;
use lyra_stream_proto::pb_stream::lyra_client::LyraClient;
use lyra_stream_proto::pb_stream::{
    AppendEventsRequest, AppendEventsRequestItem, Event, ReadEventsRequest,
};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::sleep;
use tonic::Request;
use tonic::transport::Endpoint;

/// One chaos test case, expressed as a template.
#[derive(Clone, Debug, Deserialize)]
struct Case {
    stream_id: i64,
    mode: String,
    records: u64,
}

impl Default for Case {
    fn default() -> Self {
        Self {
            stream_id: 1,
            mode: "primary-offset".into(),
            records: 10_000,
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
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();
    let case: Case = toml::from_str(&std::fs::read_to_string(&cli.case)?)?;
    tracing::info!(?case, address = %cli.address, "chaos case");

    if case.mode != "primary-offset" {
        anyhow_like_exit(&format!("unsupported mode {}", case.mode));
    }

    // Writer: append records, tracking acknowledged offsets only.
    let mut acked: BTreeSet<i64> = BTreeSet::new();
    let mut writer = connect(&cli.address).await;
    let mut written = 0u64;
    while written < case.records {
        match writer
            .append(Request::new(tokio_stream::iter([event(
                case.stream_id,
                written as i64,
                b"x",
            )])))
            .await
        {
            Ok(response) => {
                let mut stream = response.into_inner();
                match stream.message().await {
                    Ok(Some(resp)) => {
                        acked.insert(resp.commit_offset);
                        written += 1;
                    }
                    Ok(None) => {}
                    Err(_) => writer = connect(&cli.address).await,
                }
            }
            Err(_) => writer = connect(&cli.address).await,
        }
        if written % 1_000 == 0 {
            tracing::info!(written, acked = acked.len(), "writing");
        }
    }

    // Reader: read every record back and verify acknowledged records are present.
    let mut reader = connect(&cli.address).await;
    let end = acked.iter().max().map_or(0, |max| max + 1);
    let reads = vec![ReadEventsRequest {
        stream_id: case.stream_id,
        start_offset: 0,
        end_offset: end,
    }];
    let mut present: BTreeSet<i64> = BTreeSet::new();
    match reader.read(Request::new(tokio_stream::iter(reads))).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            while let Ok(Some(resp)) = stream.message().await {
                for e in resp.event {
                    present.insert(e.offset);
                }
            }
        }
        Err(error) => tracing::error!(%error, "read failed"),
    }

    let missing: Vec<i64> = acked.difference(&present).copied().collect();
    if missing.is_empty() {
        tracing::info!(
            acked = acked.len(),
            present = present.len(),
            "at-least-once verified: no acknowledged record is missing"
        );
        Ok(())
    } else {
        tracing::error!(missing = ?missing, acked = acked.len(), present = present.len(), "at-least-once VIOLATION");
        std::process::exit(1);
    }
}

fn anyhow_like_exit(message: &str) -> ! {
    eprintln!("error: {message}");
    std::process::exit(2);
}
