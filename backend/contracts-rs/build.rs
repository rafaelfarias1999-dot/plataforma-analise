fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";
    prost_build::Config::new()
        .compile_protos(
            &[
                format!("{}/tick.proto", proto_dir),
                format!("{}/market_stream.proto", proto_dir),
            ],
            &[proto_dir],
        )?;
    Ok(())
}
