use std::{convert::Infallible, time::Duration};

use libp2p::{
    PeerId, StreamProtocol, allow_block_list,
    gossipsub::{self, IdentTopic, MessageAuthenticity, MessageId, ValidationMode},
    identify, identity, ping,
    request_response::{self, ProtocolSupport},
    swarm::NetworkBehaviour,
};
use prost::Message as _;
use pulsar_verifier_proto::v1::{AvailabilityMessage, availability_message};

use crate::{Error, Result, config::P2pConfig};

use super::codec::ProofExchangeCodec;

pub(crate) const PROOF_EXCHANGE_PROTOCOL: &str = "/pulsar/verifier/proof-exchange/1";

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "PulsarBehaviourEvent")]
pub(crate) struct PulsarBehaviour {
    pub(crate) allowed_peers: allow_block_list::Behaviour<allow_block_list::AllowedPeers>,
    pub(crate) gossipsub: gossipsub::Behaviour,
    pub(crate) proof_exchange: request_response::Behaviour<ProofExchangeCodec>,
    pub(crate) identify: identify::Behaviour,
    pub(crate) ping: ping::Behaviour,
}

#[derive(Debug)]
pub(crate) enum PulsarBehaviourEvent {
    Allowed(Infallible),
    Gossipsub(gossipsub::Event),
    ProofExchange(
        request_response::Event<
            pulsar_verifier_proto::v1::GetProofRequest,
            pulsar_verifier_proto::v1::GetProofResponse,
        >,
    ),
    Identify(Box<identify::Event>),
    Ping(ping::Event),
}

impl From<Infallible> for PulsarBehaviourEvent {
    fn from(event: Infallible) -> Self {
        Self::Allowed(event)
    }
}

impl From<gossipsub::Event> for PulsarBehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self::Gossipsub(event)
    }
}

impl
    From<
        request_response::Event<
            pulsar_verifier_proto::v1::GetProofRequest,
            pulsar_verifier_proto::v1::GetProofResponse,
        >,
    > for PulsarBehaviourEvent
{
    fn from(
        event: request_response::Event<
            pulsar_verifier_proto::v1::GetProofRequest,
            pulsar_verifier_proto::v1::GetProofResponse,
        >,
    ) -> Self {
        Self::ProofExchange(event)
    }
}

impl From<identify::Event> for PulsarBehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(Box::new(event))
    }
}

impl From<ping::Event> for PulsarBehaviourEvent {
    fn from(event: ping::Event) -> Self {
        Self::Ping(event)
    }
}

pub(crate) fn build(
    identity: &identity::Keypair,
    config: &P2pConfig,
    authorized_peers: impl IntoIterator<Item = PeerId>,
) -> Result<(PulsarBehaviour, IdentTopic)> {
    let topic = IdentTopic::new(format!(
        "/pulsar/verifier/{}/availability/1",
        config.chain_id
    ));
    let gossip_config = gossipsub::ConfigBuilder::default()
        .validation_mode(ValidationMode::Strict)
        .validate_messages()
        .max_transmit_size(config.max_availability_message_bytes)
        .message_id_fn(semantic_message_id)
        .build()
        .map_err(|error| Error::P2pDriver(format!("invalid GossipSub config: {error}")))?;
    let mut gossipsub =
        gossipsub::Behaviour::new(MessageAuthenticity::Signed(identity.clone()), gossip_config)
            .map_err(|error| Error::P2pDriver(format!("failed to build GossipSub: {error}")))?;
    gossipsub
        .subscribe(&topic)
        .map_err(|error| Error::P2pDriver(format!("failed to subscribe topic: {error}")))?;

    let mut allowed_peers = allow_block_list::Behaviour::default();
    for peer in authorized_peers {
        allowed_peers.allow_peer(peer);
        gossipsub.add_explicit_peer(&peer);
    }

    let exchange_config =
        request_response::Config::default().with_request_timeout(config.proof_request_timeout);
    let proof_exchange = request_response::Behaviour::with_codec(
        ProofExchangeCodec::new(config.max_proof_bytes, &config.chain_id)?,
        [(
            StreamProtocol::new(PROOF_EXCHANGE_PROTOCOL),
            ProtocolSupport::Full,
        )],
        exchange_config,
    );

    let identify_behaviour = identify::Behaviour::new(identify::Config::new(
        format!("/pulsar/verifier/{}/1", config.chain_id),
        identity.public(),
    ));
    let ping = ping::Behaviour::new(
        ping::Config::new()
            .with_interval(Duration::from_secs(15))
            .with_timeout(Duration::from_secs(10)),
    );

    Ok((
        PulsarBehaviour {
            allowed_peers,
            gossipsub,
            proof_exchange,
            identify: identify_behaviour,
            ping,
        },
        topic,
    ))
}

fn semantic_message_id(message: &gossipsub::Message) -> MessageId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"pulsar-availability-message-v1");
    if let Some(source) = message.source {
        hasher.update(&source.to_bytes());
    }

    if let Ok(envelope) = AvailabilityMessage::decode(message.data.as_slice()) {
        hasher.update(envelope.chain_id.as_bytes());
        match envelope.payload {
            Some(availability_message::Payload::Announcement(value)) => {
                hasher.update(b"announcement");
                hasher.update(&value.verification_id);
            }
            Some(availability_message::Payload::Query(value)) => {
                hasher.update(b"query");
                hasher.update(&value.request_id);
                hasher.update(&value.verification_id);
            }
            Some(availability_message::Payload::Response(mut value)) => {
                hasher.update(b"response");
                hasher.update(&value.request_id);
                hasher.update(&value.verification_id);
                value.provider_peer_ids.sort();
                for provider in value.provider_peer_ids {
                    hasher.update(&provider);
                }
            }
            None => {
                hasher.update(b"missing-payload");
                hasher.update(&message.data);
            }
        }
    } else {
        hasher.update(b"malformed");
        hasher.update(&message.data);
    }
    MessageId::from(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use libp2p::gossipsub::TopicHash;
    use pulsar_verifier_proto::v1::{AvailabilityAnnouncement, availability_message};

    use super::*;

    fn announcement(source: PeerId) -> gossipsub::Message {
        let envelope = AvailabilityMessage {
            chain_id: "test-chain".to_owned(),
            payload: Some(availability_message::Payload::Announcement(
                AvailabilityAnnouncement {
                    verification_id: vec![7; 32],
                },
            )),
        };
        gossipsub::Message {
            source: Some(source),
            data: envelope.encode_to_vec(),
            sequence_number: None,
            topic: TopicHash::from_raw("test"),
        }
    }

    #[test]
    fn provider_identity_is_part_of_announcement_id() {
        let first = PeerId::random();
        let second = PeerId::random();

        assert_eq!(
            semantic_message_id(&announcement(first)),
            semantic_message_id(&announcement(first))
        );
        assert_ne!(
            semantic_message_id(&announcement(first)),
            semantic_message_id(&announcement(second))
        );
    }
}
