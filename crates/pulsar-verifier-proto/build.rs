use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let proto_root = PathBuf::from("proto");
    let files = [
        proto_root.join("pulsarchain/verification/v1/types.proto"),
        proto_root.join("pulsar/verifier/v1/proof.proto"),
        proto_root.join("pulsar/verifier/v1/availability.proto"),
        proto_root.join("pulsar/verifier/v1/exchange.proto"),
        proto_root.join("pulsar/verifier/v1/verification_service.proto"),
    ];
    let protoc = protoc_bin_vendored::protoc_bin_path()?;

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    tonic_build::configure().compile_protos_with_config(config, &files, &[proto_root])?;

    println!("cargo:rerun-if-changed=proto/pulsarchain/verification/v1/types.proto");
    println!("cargo:rerun-if-changed=proto/pulsar/verifier/v1/proof.proto");
    println!("cargo:rerun-if-changed=proto/pulsar/verifier/v1/availability.proto");
    println!("cargo:rerun-if-changed=proto/pulsar/verifier/v1/exchange.proto");
    println!("cargo:rerun-if-changed=proto/pulsar/verifier/v1/verification_service.proto");
    println!("cargo:rerun-if-changed=proto/amino/amino.proto");
    println!("cargo:rerun-if-changed=proto/gogoproto/gogo.proto");
    Ok(())
}
