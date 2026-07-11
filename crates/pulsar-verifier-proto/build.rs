use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let proto_root = PathBuf::from("proto");
    let files = [
        proto_root.join("pulsar/verifier/v1/availability.proto"),
        proto_root.join("pulsar/verifier/v1/exchange.proto"),
    ];
    let protoc = protoc_bin_vendored::protoc_bin_path()?;

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.compile_protos(&files, &[proto_root])?;

    println!("cargo:rerun-if-changed=proto/pulsar/verifier/v1/availability.proto");
    println!("cargo:rerun-if-changed=proto/pulsar/verifier/v1/exchange.proto");
    Ok(())
}
