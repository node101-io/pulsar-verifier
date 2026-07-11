use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use libp2p::{PeerId, identity};
use serde::Deserialize;
use zeroize::Zeroize;

use crate::{Error, Result};

const ED25519_PUBLIC_KEY_LEN: usize = 32;
const ED25519_COMET_PRIVATE_KEY_LEN: usize = 64;
const ED25519_PUBLIC_TYPES: &[&str] = &["tendermint/PubKeyEd25519", "cometbft/PubKeyEd25519"];
const ED25519_PRIVATE_TYPES: &[&str] = &["tendermint/PrivKeyEd25519", "cometbft/PrivKeyEd25519"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidatorKeyFile {
    pub_key: EncodedKey,
    priv_key: EncodedKey,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EncodedKey {
    #[serde(rename = "type")]
    key_type: String,
    value: String,
}

/// Loads the consensus Ed25519 key without exposing key material to logs.
pub(crate) fn load_validator_identity(path: &Path) -> Result<identity::Keypair> {
    let metadata = fs::metadata(path).map_err(|error| {
        Error::P2pIdentity(format!("failed to inspect {}: {error}", path.display()))
    })?;
    if !metadata.is_file() {
        return Err(Error::P2pIdentity(format!(
            "validator key path is not a file: {}",
            path.display()
        )));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::P2pIdentity(format!(
            "validator key {} must not be accessible by group or others (mode {:o})",
            path.display(),
            metadata.mode() & 0o777
        )));
    }

    let contents = fs::read_to_string(path).map_err(|error| {
        Error::P2pIdentity(format!("failed to read {}: {error}", path.display()))
    })?;
    let encoded: ValidatorKeyFile = serde_json::from_str(&contents).map_err(|error| {
        Error::P2pIdentity(format!("failed to parse {}: {error}", path.display()))
    })?;

    ensure_key_type(&encoded.pub_key.key_type, ED25519_PUBLIC_TYPES, "public")?;
    ensure_key_type(&encoded.priv_key.key_type, ED25519_PRIVATE_TYPES, "private")?;

    let public = decode_key(&encoded.pub_key.value, ED25519_PUBLIC_KEY_LEN, "public")?;
    let mut private = decode_key(
        &encoded.priv_key.value,
        ED25519_COMET_PRIVATE_KEY_LEN,
        "private",
    )?;
    let mut secret = private[..ED25519_PUBLIC_KEY_LEN].to_vec();
    private.zeroize();

    let keypair = identity::Keypair::ed25519_from_bytes(&mut secret)
        .map_err(|error| Error::P2pIdentity(format!("invalid Ed25519 private key: {error}")))?;
    let derived = keypair
        .public()
        .try_into_ed25519()
        .map_err(|error| Error::P2pIdentity(format!("invalid Ed25519 public key: {error}")))?;
    if derived.to_bytes().as_slice() != public.as_slice() {
        return Err(Error::P2pIdentity(
            "validator public and private keys do not match".to_owned(),
        ));
    }

    Ok(keypair)
}

/// Converts one `CometBFT` Ed25519 public key into its deterministic libp2p `PeerId`.
pub(crate) fn peer_id_from_ed25519(public_key: &[u8]) -> Result<PeerId> {
    let public = identity::ed25519::PublicKey::try_from_bytes(public_key)
        .map_err(|error| Error::P2pAuthorization(format!("invalid validator key: {error}")))?;
    let public: identity::PublicKey = public.into();
    Ok(PeerId::from_public_key(&public))
}

fn ensure_key_type(actual: &str, expected: &[&str], label: &str) -> Result<()> {
    if expected.contains(&actual) {
        Ok(())
    } else {
        Err(Error::P2pIdentity(format!(
            "unsupported validator {label} key type: {actual}"
        )))
    }
}

fn decode_key(value: &str, expected_len: usize, label: &str) -> Result<Vec<u8>> {
    let decoded = STANDARD
        .decode(value)
        .map_err(|error| Error::P2pIdentity(format!("invalid base64 {label} key: {error}")))?;
    if decoded.len() != expected_len {
        return Err(Error::P2pIdentity(format!(
            "{label} key must be {expected_len} bytes, got {}",
            decoded.len()
        )));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use std::{io::Write, os::unix::fs::PermissionsExt};

    use tempfile::NamedTempFile;

    use super::*;

    fn validator_file(keypair: &identity::ed25519::Keypair) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        let bytes = keypair.to_bytes();
        let json = serde_json::json!({
            "pub_key": {
                "type": "tendermint/PubKeyEd25519",
                "value": STANDARD.encode(keypair.public().to_bytes()),
            },
            "priv_key": {
                "type": "tendermint/PrivKeyEd25519",
                "value": STANDARD.encode(bytes),
            }
        });
        file.write_all(json.to_string().as_bytes()).unwrap();
        fs::set_permissions(file.path(), fs::Permissions::from_mode(0o600)).unwrap();
        file
    }

    #[test]
    fn loads_stable_peer_id() {
        let keypair = identity::ed25519::Keypair::generate();
        let file = validator_file(&keypair);

        let first = load_validator_identity(file.path()).unwrap();
        let second = load_validator_identity(file.path()).unwrap();

        assert_eq!(first.public().to_peer_id(), second.public().to_peer_id());
    }

    #[test]
    fn rejects_insecure_permissions() {
        let keypair = identity::ed25519::Keypair::generate();
        let file = validator_file(&keypair);
        fs::set_permissions(file.path(), fs::Permissions::from_mode(0o644)).unwrap();

        assert!(matches!(
            load_validator_identity(file.path()),
            Err(Error::P2pIdentity(_))
        ));
    }

    #[test]
    fn rejects_mismatched_public_key() {
        let keypair = identity::ed25519::Keypair::generate();
        let other = identity::ed25519::Keypair::generate();
        let mut file = validator_file(&keypair);
        let json = serde_json::json!({
            "pub_key": {
                "type": "tendermint/PubKeyEd25519",
                "value": STANDARD.encode(other.public().to_bytes()),
            },
            "priv_key": {
                "type": "tendermint/PrivKeyEd25519",
                "value": STANDARD.encode(keypair.to_bytes()),
            }
        });
        file.as_file_mut().set_len(0).unwrap();
        file.write_all(json.to_string().as_bytes()).unwrap();

        assert!(matches!(
            load_validator_identity(file.path()),
            Err(Error::P2pIdentity(_))
        ));
    }
}
