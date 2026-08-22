use std::collections::HashSet;

use libp2p::PeerId;

use crate::{Error, Result, chain::PulsarClient};

use super::identity::peer_id_from_ed25519;

/// Converts exact-height chain validator snapshots into the P2P allow-list.
#[derive(Clone)]
pub(crate) struct ValidatorSetClient {
    chain: PulsarClient,
}

impl ValidatorSetClient {
    pub(crate) const fn new(chain: PulsarClient) -> Self {
        Self { chain }
    }

    pub(crate) async fn load(&self) -> Result<HashSet<PeerId>> {
        let status = self.chain.status().await?;
        self.load_at(status.latest_height).await
    }

    pub(crate) async fn load_at(&self, height: u64) -> Result<HashSet<PeerId>> {
        let keys = self.chain.validator_public_keys(height).await?;
        let peers = keys
            .iter()
            .map(|key| peer_id_from_ed25519(key))
            .collect::<Result<HashSet<_>>>()?;
        if peers.len() != keys.len() {
            return Err(Error::P2pAuthorization(
                "validator set contains duplicate peer identities".to_owned(),
            ));
        }
        Ok(peers)
    }
}
