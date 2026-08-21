use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let proto_root = PathBuf::from("proto");
    let files = [
        proto_root.join("pulsarchain/verification/v1/types.proto"),
        proto_root.join("pulsarchain/verification/v1/params.proto"),
        proto_root.join("pulsarchain/verification/v1/query.proto"),
        proto_root.join("pulsarchain/verification/v1/tx.proto"),
        proto_root.join("cosmos/tx/v1beta1/tx.proto"),
        proto_root.join("pulsar/verifier/v1/proof.proto"),
        proto_root.join("pulsar/verifier/v1/availability.proto"),
        proto_root.join("pulsar/verifier/v1/exchange.proto"),
        proto_root.join("pulsar/verifier/v1/verification_service.proto"),
    ];
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let protoc_include = protoc_bin_vendored::include_path()?;

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    tonic_build::configure().compile_protos_with_config(
        config,
        &files,
        &[proto_root, protoc_include],
    )?;

    println!("cargo:rerun-if-changed=proto/pulsarchain/verification/v1/types.proto");
    println!("cargo:rerun-if-changed=proto/pulsarchain/verification/v1/params.proto");
    println!("cargo:rerun-if-changed=proto/pulsarchain/verification/v1/query.proto");
    println!("cargo:rerun-if-changed=proto/pulsarchain/verification/v1/tx.proto");
    println!("cargo:rerun-if-changed=proto/cosmos/tx/v1beta1/tx.proto");
    println!("cargo:rerun-if-changed=proto/cosmos/tx/signing/v1beta1/signing.proto");
    println!("cargo:rerun-if-changed=proto/cosmos/crypto/multisig/v1beta1/multisig.proto");
    println!("cargo:rerun-if-changed=proto/cosmos/base/v1beta1/coin.proto");
    println!("cargo:rerun-if-changed=proto/cosmos/msg/v1/msg.proto");
    println!("cargo:rerun-if-changed=proto/cosmos/base/query/v1beta1/pagination.proto");
    println!("cargo:rerun-if-changed=proto/cosmos_proto/cosmos.proto");
    println!("cargo:rerun-if-changed=proto/google/api/annotations.proto");
    println!("cargo:rerun-if-changed=proto/google/api/http.proto");
    println!("cargo:rerun-if-changed=proto/pulsar/verifier/v1/proof.proto");
    println!("cargo:rerun-if-changed=proto/pulsar/verifier/v1/availability.proto");
    println!("cargo:rerun-if-changed=proto/pulsar/verifier/v1/exchange.proto");
    println!("cargo:rerun-if-changed=proto/pulsar/verifier/v1/verification_service.proto");
    println!("cargo:rerun-if-changed=proto/amino/amino.proto");
    println!("cargo:rerun-if-changed=proto/gogoproto/gogo.proto");
    Ok(())
}
