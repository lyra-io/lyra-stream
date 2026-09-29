fn main() {
    let mut config = prost_build::Config::new();
    config.bytes(["."]);

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos_with_config(config, &["schema/pb_stream.proto"], &["schema"])
        .unwrap();
}
