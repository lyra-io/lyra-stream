use bytes::Bytes;
use lyra_stream_proto::pb_stream::lyra_client::LyraClient;
use lyra_stream_proto::pb_stream::{
    AppendEventsRequest, AppendEventsRequestItem, Event, ReadEventsRequest,
};
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = LyraClient::connect("http://127.0.0.1:7070").await?;

    let appends = vec![AppendEventsRequest {
        items: vec![AppendEventsRequestItem {
            event: Some(Event {
                stream_id: 1,
                term: 1,
                offset: 0,
                payload: Some(Bytes::from_static(b"hello")),
                crc32: None,
                timestamp: 0,
                schema_id: 0,
            }),
            trunc: false,
            lra: 0,
        }],
    }];
    let mut append_stream = client
        .append(Request::new(tokio_stream::iter(appends)))
        .await?
        .into_inner();
    while let Some(response) = append_stream.message().await? {
        println!("append commit_offset={}", response.commit_offset);
    }

    let reads = vec![ReadEventsRequest {
        stream_id: 1,
        start_offset: 0,
        end_offset: 100,
    }];
    let mut read_stream = client
        .read(Request::new(tokio_stream::iter(reads)))
        .await?
        .into_inner();
    while let Some(response) = read_stream.message().await? {
        for event in response.event {
            println!(
                "read offset={} payload={:?}",
                event.offset,
                event.payload.as_deref().map(String::from_utf8_lossy)
            );
        }
    }
    Ok(())
}
