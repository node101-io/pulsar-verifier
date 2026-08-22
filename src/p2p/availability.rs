use std::collections::{HashMap, HashSet};

use libp2p::PeerId;
use prost::Message as _;
use pulsar_verifier_proto::v1::{
    AvailabilityAnnouncement, AvailabilityMessage, AvailabilityQuery, AvailabilityResponse,
    availability_message,
};

use crate::{Error, Result};

use super::{QueryId, types::QUERY_ID_LEN};
use crate::proof::VerificationId;

#[derive(Debug)]
pub(crate) enum ValidatedAvailability {
    Announcement {
        verification_id: VerificationId,
    },
    Query {
        query_id: QueryId,
        verification_id: VerificationId,
    },
    Response {
        query_id: QueryId,
        verification_id: VerificationId,
        providers: Vec<PeerId>,
    },
}

#[derive(Default)]
pub(crate) struct AvailabilityIndex {
    providers: HashMap<VerificationId, HashSet<PeerId>>,
}

impl AvailabilityIndex {
    pub(crate) fn add(&mut self, verification_id: VerificationId, peer: PeerId) {
        self.providers
            .entry(verification_id)
            .or_default()
            .insert(peer);
    }

    pub(crate) fn providers(&self, verification_id: VerificationId) -> Vec<PeerId> {
        let mut providers = self
            .providers
            .get(&verification_id)
            .into_iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        providers.sort_by_key(|peer| peer.to_bytes());
        providers
    }

    pub(crate) fn remove_provider(&mut self, verification_id: VerificationId, peer: PeerId) {
        if let Some(providers) = self.providers.get_mut(&verification_id) {
            providers.remove(&peer);
            if providers.is_empty() {
                self.providers.remove(&verification_id);
            }
        }
    }

    pub(crate) fn remove_peer(&mut self, peer: PeerId) {
        self.providers.retain(|_, providers| {
            providers.remove(&peer);
            !providers.is_empty()
        });
    }

    pub(crate) fn replace_provider_proofs(
        &mut self,
        peer: PeerId,
        verification_ids: &HashSet<VerificationId>,
    ) {
        self.remove_peer(peer);
        for verification_id in verification_ids {
            self.add(*verification_id, peer);
        }
    }
}

pub(crate) fn decode_and_validate(
    bytes: &[u8],
    expected_chain_id: &str,
    authorized: &HashSet<PeerId>,
    maximum_providers: usize,
) -> Result<ValidatedAvailability> {
    let envelope = AvailabilityMessage::decode(bytes)
        .map_err(|error| Error::P2pProtocol(format!("invalid availability protobuf: {error}")))?;
    if envelope.chain_id != expected_chain_id {
        return Err(Error::P2pProtocol(format!(
            "availability chain ID mismatch: expected {expected_chain_id}, got {}",
            envelope.chain_id
        )));
    }

    match envelope.payload {
        Some(availability_message::Payload::Announcement(value)) => {
            Ok(ValidatedAvailability::Announcement {
                verification_id: VerificationId::try_from(value.verification_id.as_slice())?,
            })
        }
        Some(availability_message::Payload::Query(value)) => Ok(ValidatedAvailability::Query {
            query_id: query_id(&value.request_id)?,
            verification_id: VerificationId::try_from(value.verification_id.as_slice())?,
        }),
        Some(availability_message::Payload::Response(value)) => {
            if value.provider_peer_ids.len() > maximum_providers {
                return Err(Error::P2pProtocol(format!(
                    "availability response exceeds {maximum_providers} providers"
                )));
            }
            let mut unique = HashSet::new();
            for bytes in value.provider_peer_ids {
                let peer = PeerId::from_bytes(&bytes).map_err(|error| {
                    Error::P2pProtocol(format!("invalid provider PeerId: {error}"))
                })?;
                if !authorized.contains(&peer) {
                    return Err(Error::P2pProtocol(format!(
                        "provider {peer} is not an active validator"
                    )));
                }
                unique.insert(peer);
            }
            let mut providers = unique.into_iter().collect::<Vec<_>>();
            providers.sort_by_key(|peer| peer.to_bytes());
            Ok(ValidatedAvailability::Response {
                query_id: query_id(&value.request_id)?,
                verification_id: VerificationId::try_from(value.verification_id.as_slice())?,
                providers,
            })
        }
        None => Err(Error::P2pProtocol(
            "availability message has no payload".to_owned(),
        )),
    }
}

pub(crate) fn announcement(chain_id: &str, verification_id: VerificationId) -> Vec<u8> {
    AvailabilityMessage {
        chain_id: chain_id.to_owned(),
        payload: Some(availability_message::Payload::Announcement(
            AvailabilityAnnouncement {
                verification_id: verification_id.as_bytes().to_vec(),
            },
        )),
    }
    .encode_to_vec()
}

pub(crate) fn query(chain_id: &str, query_id: QueryId, verification_id: VerificationId) -> Vec<u8> {
    AvailabilityMessage {
        chain_id: chain_id.to_owned(),
        payload: Some(availability_message::Payload::Query(AvailabilityQuery {
            request_id: query_id.as_bytes().to_vec(),
            verification_id: verification_id.as_bytes().to_vec(),
        })),
    }
    .encode_to_vec()
}

pub(crate) fn response(
    chain_id: &str,
    query_id: QueryId,
    verification_id: VerificationId,
    providers: &[PeerId],
) -> Vec<u8> {
    AvailabilityMessage {
        chain_id: chain_id.to_owned(),
        payload: Some(availability_message::Payload::Response(
            AvailabilityResponse {
                request_id: query_id.as_bytes().to_vec(),
                verification_id: verification_id.as_bytes().to_vec(),
                provider_peer_ids: providers.iter().map(|peer| peer.to_bytes()).collect(),
            },
        )),
    }
    .encode_to_vec()
}

fn query_id(bytes: &[u8]) -> Result<QueryId> {
    let value = bytes.try_into().map_err(|_| {
        Error::P2pProtocol(format!(
            "query ID must be {QUERY_ID_LEN} bytes, got {}",
            bytes.len()
        ))
    })?;
    Ok(QueryId(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proof::{Proof, ProofType};

    fn verification_id() -> VerificationId {
        Proof {
            proof_type: ProofType::MinaPickles,
            proof: b"proof".as_slice().into(),
            public_inputs: b"inputs".as_slice().into(),
            verification_key: b"key".as_slice().into(),
        }
        .verification_id()
    }

    #[test]
    fn filters_unauthorized_provider_hints() {
        let authorized = PeerId::random();
        let unauthorized = PeerId::random();
        let bytes = response(
            "chain",
            QueryId([1; QUERY_ID_LEN]),
            verification_id(),
            &[authorized, unauthorized],
        );

        assert!(decode_and_validate(&bytes, "chain", &HashSet::from([authorized]), 128).is_err());
    }

    #[test]
    fn index_removes_disconnected_peer() {
        let first = PeerId::random();
        let second = PeerId::random();
        let verification_id = verification_id();
        let mut index = AvailabilityIndex::default();
        index.add(verification_id, first);
        index.add(verification_id, second);

        index.remove_peer(first);

        assert_eq!(index.providers(verification_id), vec![second]);
    }
}
