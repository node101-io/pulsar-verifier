use std::{
    collections::{HashMap, HashSet},
    future::Future,
    time::{Duration, Instant},
};

use futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered};
use libp2p::{
    Multiaddr, PeerId, Swarm, SwarmBuilder,
    core::multiaddr::Protocol,
    gossipsub::{self, MessageAcceptance},
    identity,
    request_response::{self, OutboundRequestId, ResponseChannel},
    swarm::{SwarmEvent, dial_opts::DialOpts},
};
use pulsar_verifier_proto::v1::{
    GetProofRequest, GetProofResponse, ProofNotFound, get_proof_response,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{Error, Result, config::P2pConfig};

use super::{
    InboundProofRequestId, P2pEvent, ProofContent, ProofHash, ProofRequestId, QueryId,
    availability::{self, AvailabilityIndex, ValidatedAvailability},
    behaviour::{self, PulsarBehaviour, PulsarBehaviourEvent},
};

const MAX_PROVIDER_HINTS: usize = 128;
const QUERY_RESPONSE_MIN_DELAY: u64 = 50;
const QUERY_RESPONSE_MAX_DELAY: u64 = 250;

type CommandReply<T> = oneshot::Sender<Result<T>>;
type P2pParts = (
    P2pDriver,
    P2pHandle,
    mpsc::Receiver<P2pEvent>,
    oneshot::Receiver<Result<()>>,
);

/// Cloneable application facade; the Swarm remains owned by one driver task.
#[derive(Clone)]
pub struct P2pHandle {
    commands: mpsc::Sender<Command>,
}

impl P2pHandle {
    /// Dials an already authorized peer using all known transport addresses.
    ///
    /// # Errors
    ///
    /// Returns an error if the driver is unavailable or rejects the dial.
    pub async fn dial(&self, peer: PeerId, addresses: Vec<Multiaddr>) -> Result<()> {
        self.call(|reply| Command::Dial {
            peer,
            addresses,
            reply,
        })
        .await
    }

    /// Announces local ownership of one proof hash.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver is unavailable or publishing fails.
    pub async fn announce(&self, proof_hash: ProofHash) -> Result<()> {
        self.call(|reply| Command::Announce { proof_hash, reply })
            .await
    }

    /// Removes local ownership after proof content leaves the process cache.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver task is unavailable.
    pub async fn forget_local_proof(&self, proof_hash: ProofHash) -> Result<()> {
        self.call(|reply| Command::ForgetLocalProof { proof_hash, reply })
            .await
    }

    /// Reconciles local ownership after a lagged store event subscription.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver task is unavailable.
    pub async fn replace_local_proofs(&self, proof_hashes: HashSet<ProofHash>) -> Result<()> {
        self.call(|reply| Command::ReplaceLocalProofs {
            proof_hashes,
            reply,
        })
        .await
    }

    /// Broadcasts a provider lookup for one proof hash.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver is unavailable or publishing fails.
    pub async fn query_availability(&self, proof_hash: ProofHash) -> Result<QueryId> {
        self.call(|reply| Command::QueryAvailability { proof_hash, reply })
            .await
    }

    /// Returns providers currently known by the local ephemeral index.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver task is unavailable.
    pub async fn providers(&self, proof_hash: ProofHash) -> Result<Vec<PeerId>> {
        self.call(|reply| Command::Providers { proof_hash, reply })
            .await
    }

    /// Starts one opaque proof request; active-chain gating belongs to `ProofService`.
    ///
    /// # Errors
    ///
    /// Returns an error for unauthorized peers, duplicate requests, or driver failure.
    pub async fn request_proof(
        &self,
        peer: PeerId,
        proof_hash: ProofHash,
    ) -> Result<ProofRequestId> {
        self.call(|reply| Command::RequestProof {
            peer,
            proof_hash,
            reply,
        })
        .await
    }

    /// Answers one inbound request with opaque content or `ProofNotFound`.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown token, invalid content, or closed driver.
    pub async fn respond_proof(
        &self,
        request_id: InboundProofRequestId,
        content: Option<ProofContent>,
    ) -> Result<()> {
        self.call(|reply| Command::RespondProof {
            request_id,
            content,
            reply,
        })
        .await
    }

    /// Atomically replaces active-validator authorization state.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver task is unavailable.
    pub async fn replace_authorized_peers(&self, peers: HashSet<PeerId>) -> Result<()> {
        self.call(|reply| Command::ReplaceAuthorizedPeers { peers, reply })
            .await
    }

    async fn call<T>(&self, command: impl FnOnce(CommandReply<T>) -> Command) -> Result<T> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| Error::P2pDriver("driver command channel is closed".to_owned()))?;
        response
            .await
            .map_err(|_| Error::P2pDriver("driver dropped command response".to_owned()))?
    }
}

enum Command {
    Dial {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        reply: CommandReply<()>,
    },
    Announce {
        proof_hash: ProofHash,
        reply: CommandReply<()>,
    },
    ForgetLocalProof {
        proof_hash: ProofHash,
        reply: CommandReply<()>,
    },
    ReplaceLocalProofs {
        proof_hashes: HashSet<ProofHash>,
        reply: CommandReply<()>,
    },
    QueryAvailability {
        proof_hash: ProofHash,
        reply: CommandReply<QueryId>,
    },
    Providers {
        proof_hash: ProofHash,
        reply: CommandReply<Vec<PeerId>>,
    },
    RequestProof {
        peer: PeerId,
        proof_hash: ProofHash,
        reply: CommandReply<ProofRequestId>,
    },
    RespondProof {
        request_id: InboundProofRequestId,
        content: Option<ProofContent>,
        reply: CommandReply<()>,
    },
    ReplaceAuthorizedPeers {
        peers: HashSet<PeerId>,
        reply: CommandReply<()>,
    },
}

struct OutboundProofRequest {
    application_id: ProofRequestId,
    peer: PeerId,
    proof_hash: ProofHash,
}

struct InboundProofRequest {
    peer: PeerId,
    proof_hash: ProofHash,
    channel: ResponseChannel<GetProofResponse>,
}

/// Single owner of transport state, protocol behaviours and request correlation.
pub struct P2pDriver {
    swarm: Swarm<PulsarBehaviour>,
    config: P2pConfig,
    local_peer_id: PeerId,
    topic: gossipsub::IdentTopic,
    authorized_peers: HashSet<PeerId>,
    availability: AvailabilityIndex,
    outstanding_queries: HashMap<QueryId, (ProofHash, Instant)>,
    outbound_requests: HashMap<OutboundRequestId, OutboundProofRequest>,
    in_flight: HashSet<(PeerId, ProofHash)>,
    inbound_requests: HashMap<InboundProofRequestId, InboundProofRequest>,
    next_proof_request_id: u64,
    next_inbound_request_id: u64,
    commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<P2pEvent>,
    delayed_responses: FuturesUnordered<futures::future::BoxFuture<'static, Vec<u8>>>,
    pending_listeners: HashSet<libp2p::core::transport::ListenerId>,
    ready: Option<oneshot::Sender<Result<()>>>,
}

impl P2pDriver {
    /// Builds a driver and its bounded application channels without spawning tasks.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid transports, listeners, bootnodes or behaviour config.
    pub fn new(
        config: P2pConfig,
        identity: identity::Keypair,
        mut authorized_peers: HashSet<PeerId>,
    ) -> Result<P2pParts> {
        let local_peer_id = identity.public().to_peer_id();
        authorized_peers.remove(&local_peer_id);
        let (behaviour, topic) =
            behaviour::build(&identity, &config, authorized_peers.iter().copied())?;
        let mut swarm = SwarmBuilder::with_existing_identity(identity)
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default().nodelay(true),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )
            .map_err(|error| Error::P2pDriver(format!("failed to build TCP transport: {error}")))?
            .with_quic()
            .with_dns()
            .map_err(|error| Error::P2pDriver(format!("failed to build DNS transport: {error}")))?
            .with_behaviour(|_| behaviour)
            .map_err(|error| Error::P2pDriver(format!("failed to build behaviour: {error}")))?
            .build();

        let mut pending_listeners = HashSet::new();
        for address in &config.listen_addresses {
            let id = swarm.listen_on(address.clone()).map_err(|error| {
                Error::P2pDriver(format!("failed to listen on {address}: {error}"))
            })?;
            pending_listeners.insert(id);
        }

        let mut bootnodes = HashMap::<PeerId, Vec<Multiaddr>>::new();
        for bootnode in &config.bootnodes {
            let (peer, address) = split_peer_address(bootnode.clone())?;
            if !authorized_peers.contains(&peer) {
                return Err(Error::P2pAuthorization(format!(
                    "bootnode {peer} is not in the active validator set"
                )));
            }
            swarm.add_peer_address(peer, address.clone());
            bootnodes.entry(peer).or_default().push(address);
        }
        for (peer, addresses) in bootnodes {
            swarm
                .dial(DialOpts::peer_id(peer).addresses(addresses).build())
                .map_err(|error| {
                    Error::P2pDriver(format!("failed to dial bootnode {peer}: {error}"))
                })?;
        }

        let (command_tx, commands) = mpsc::channel(config.command_buffer);
        let (events, event_rx) = mpsc::channel(config.event_buffer);
        let (ready_tx, ready_rx) = oneshot::channel();

        Ok((
            Self {
                swarm,
                config,
                local_peer_id,
                topic,
                authorized_peers,
                availability: AvailabilityIndex::default(),
                outstanding_queries: HashMap::new(),
                outbound_requests: HashMap::new(),
                in_flight: HashSet::new(),
                inbound_requests: HashMap::new(),
                next_proof_request_id: 1,
                next_inbound_request_id: 1,
                commands,
                events,
                delayed_responses: FuturesUnordered::new(),
                pending_listeners,
                ready: Some(ready_tx),
            },
            P2pHandle {
                commands: command_tx,
            },
            event_rx,
            ready_rx,
        ))
    }

    /// Runs until cancellation or an unrecoverable command/network failure.
    ///
    /// # Errors
    ///
    /// Returns an error when the command channel closes unexpectedly or a fatal event occurs.
    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        let mut cleanup = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
                command = self.commands.recv() => {
                    let command = command.ok_or_else(|| {
                        Error::P2pDriver("driver command channel closed".to_owned())
                    })?;
                    self.handle_command(command);
                }
                event = self.swarm.select_next_some() => self.handle_swarm_event(event).await?,
                Some(response) = self.delayed_responses.next(), if !self.delayed_responses.is_empty() => {
                    if let Err(error) = self.swarm.behaviour_mut().gossipsub.publish(self.topic.clone(), response) {
                        tracing::debug!(%error, "availability response was not published");
                    }
                }
                _ = cleanup.tick() => self.expire_queries(),
            }
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Dial {
                peer,
                addresses,
                reply,
            } => {
                let _ = reply.send(self.dial(peer, addresses));
            }
            Command::Announce { proof_hash, reply } => {
                let _ = reply.send(self.announce(proof_hash));
            }
            Command::ForgetLocalProof { proof_hash, reply } => {
                self.availability
                    .remove_provider(proof_hash, self.local_peer_id);
                // TODO: Add availability leases and periodic re-announcement so remote
                // peers eventually discard ownership that could not be withdrawn.
                let _ = reply.send(Ok(()));
            }
            Command::ReplaceLocalProofs {
                proof_hashes,
                reply,
            } => {
                self.availability
                    .replace_provider_proofs(self.local_peer_id, &proof_hashes);
                let _ = reply.send(Ok(()));
            }
            Command::QueryAvailability { proof_hash, reply } => {
                let _ = reply.send(self.query_availability(proof_hash));
            }
            Command::Providers { proof_hash, reply } => {
                let _ = reply.send(Ok(self.availability.providers(proof_hash)));
            }
            Command::RequestProof {
                peer,
                proof_hash,
                reply,
            } => {
                let _ = reply.send(self.request_proof(peer, proof_hash));
            }
            Command::RespondProof {
                request_id,
                content,
                reply,
            } => {
                let _ = reply.send(self.respond_proof(request_id, content));
            }
            Command::ReplaceAuthorizedPeers { peers, reply } => {
                self.replace_authorized_peers(peers);
                let _ = reply.send(Ok(()));
            }
        }
    }

    fn dial(&mut self, peer: PeerId, addresses: Vec<Multiaddr>) -> Result<()> {
        if !self.authorized_peers.contains(&peer) {
            return Err(Error::P2pAuthorization(format!(
                "refusing to dial unauthorized peer {peer}"
            )));
        }
        if addresses.is_empty() {
            return Err(Error::P2pDriver(
                "dial requires at least one address".to_owned(),
            ));
        }
        for address in &addresses {
            self.swarm.add_peer_address(peer, address.clone());
        }
        self.swarm
            .dial(DialOpts::peer_id(peer).addresses(addresses).build())
            .map_err(|error| Error::P2pDriver(format!("failed to dial {peer}: {error}")))
    }

    fn announce(&mut self, proof_hash: ProofHash) -> Result<()> {
        self.availability.add(proof_hash, self.local_peer_id);
        let bytes = availability::announcement(&self.config.chain_id, proof_hash);
        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(self.topic.clone(), bytes)
            .map(|_| ())
            .or_else(|error| match error {
                gossipsub::PublishError::NoPeersSubscribedToTopic
                | gossipsub::PublishError::Duplicate => Ok(()),
                error => Err(Error::P2pDriver(format!(
                    "failed to publish announcement: {error}"
                ))),
            })
    }

    fn query_availability(&mut self, proof_hash: ProofHash) -> Result<QueryId> {
        let query_id = QueryId::random();
        let bytes = availability::query(&self.config.chain_id, query_id, proof_hash);
        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(self.topic.clone(), bytes)
            .map_err(|error| Error::P2pDriver(format!("failed to publish query: {error}")))?;
        self.outstanding_queries
            .insert(query_id, (proof_hash, Instant::now()));
        Ok(query_id)
    }

    fn request_proof(&mut self, peer: PeerId, proof_hash: ProofHash) -> Result<ProofRequestId> {
        if !self.authorized_peers.contains(&peer) {
            return Err(Error::P2pAuthorization(format!(
                "refusing proof request to unauthorized peer {peer}"
            )));
        }
        if !self.in_flight.insert((peer, proof_hash)) {
            return Err(Error::P2pDriver(format!(
                "proof request to {peer} is already in flight"
            )));
        }

        let application_id = ProofRequestId(self.next_proof_request_id);
        self.next_proof_request_id += 1;
        let request_id = self.swarm.behaviour_mut().proof_exchange.send_request(
            &peer,
            GetProofRequest {
                chain_id: self.config.chain_id.clone(),
                proof_hash: proof_hash.as_bytes().to_vec(),
            },
        );
        self.outbound_requests.insert(
            request_id,
            OutboundProofRequest {
                application_id,
                peer,
                proof_hash,
            },
        );
        Ok(application_id)
    }

    fn respond_proof(
        &mut self,
        request_id: InboundProofRequestId,
        content: Option<ProofContent>,
    ) -> Result<()> {
        let inbound = self.inbound_requests.remove(&request_id).ok_or_else(|| {
            Error::P2pDriver(format!("unknown inbound proof request {request_id:?}"))
        })?;
        let result = match content {
            Some(content) => {
                if content.proof.len() > self.config.max_proof_bytes {
                    return Err(Error::P2pProtocol(
                        "proof exceeds configured limit".to_owned(),
                    ));
                }
                if content.proof_hash != inbound.proof_hash
                    || ProofHash::digest(&content.proof) != content.proof_hash
                {
                    return Err(Error::P2pProtocol(
                        "proof response does not match inbound request".to_owned(),
                    ));
                }
                get_proof_response::Result::Content(pulsar_verifier_proto::v1::ProofContent {
                    proof_hash: content.proof_hash.as_bytes().to_vec(),
                    proof: content.proof.to_vec(),
                })
            }
            None => get_proof_response::Result::NotFound(ProofNotFound {}),
        };
        self.swarm
            .behaviour_mut()
            .proof_exchange
            .send_response(
                inbound.channel,
                GetProofResponse {
                    chain_id: self.config.chain_id.clone(),
                    result: Some(result),
                },
            )
            .map_err(|_| Error::P2pDriver(format!("failed to respond to {}", inbound.peer)))
    }

    fn replace_authorized_peers(&mut self, mut peers: HashSet<PeerId>) {
        peers.remove(&self.local_peer_id);
        for removed in self
            .authorized_peers
            .difference(&peers)
            .copied()
            .collect::<Vec<_>>()
        {
            self.swarm
                .behaviour_mut()
                .allowed_peers
                .disallow_peer(removed);
            self.swarm
                .behaviour_mut()
                .gossipsub
                .remove_explicit_peer(&removed);
            self.availability.remove_peer(removed);
        }
        for added in peers
            .difference(&self.authorized_peers)
            .copied()
            .collect::<Vec<_>>()
        {
            self.swarm.behaviour_mut().allowed_peers.allow_peer(added);
            self.swarm
                .behaviour_mut()
                .gossipsub
                .add_explicit_peer(&added);
        }
        self.authorized_peers = peers;

        // TODO: Pulsar Listener should fetch and submit a new complete set only on change events.
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<PulsarBehaviourEvent>) -> Result<()> {
        match event {
            SwarmEvent::NewListenAddr {
                listener_id,
                address,
            } => {
                self.pending_listeners.remove(&listener_id);
                self.emit(P2pEvent::Listening { address }).await;
                if self.pending_listeners.is_empty() {
                    if let Some(ready) = self.ready.take() {
                        let _ = ready.send(Ok(()));
                    }
                }
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                self.pending_listeners.remove(&listener_id);
                if let Some(ready) = self.ready.take() {
                    let _ = ready.send(Err(Error::P2pDriver(format!(
                        "listener failed during startup: {error}"
                    ))));
                }
                return Err(Error::P2pDriver(format!("listener failed: {error}")));
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                self.emit(P2pEvent::PeerConnected { peer: peer_id }).await;
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established: 0,
                ..
            } => {
                self.availability.remove_peer(peer_id);
                self.emit(P2pEvent::PeerDisconnected { peer: peer_id })
                    .await;
            }
            SwarmEvent::Behaviour(PulsarBehaviourEvent::Gossipsub(event)) => {
                self.handle_gossip(event).await;
            }
            SwarmEvent::Behaviour(PulsarBehaviourEvent::ProofExchange(event)) => {
                self.handle_exchange(event).await;
            }
            SwarmEvent::Behaviour(PulsarBehaviourEvent::Identify(event)) => {
                tracing::debug!(?event, "identify event");
            }
            SwarmEvent::Behaviour(PulsarBehaviourEvent::Ping(event)) => {
                tracing::trace!(?event, "ping event");
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_gossip(&mut self, event: gossipsub::Event) {
        let gossipsub::Event::Message {
            propagation_source,
            message_id,
            message,
        } = event
        else {
            return;
        };
        let source = message.source;
        let validated = source
            .filter(|peer| self.authorized_peers.contains(peer))
            .filter(|_| self.authorized_peers.contains(&propagation_source))
            .ok_or_else(|| Error::P2pProtocol("unauthorized gossip source".to_owned()))
            .and_then(|source| {
                availability::decode_and_validate(
                    &message.data,
                    &self.config.chain_id,
                    &self.authorized_peers,
                    MAX_PROVIDER_HINTS,
                )
                .map(|payload| (source, payload))
            });

        let acceptance = if validated.is_ok() {
            MessageAcceptance::Accept
        } else {
            MessageAcceptance::Reject
        };
        if !self
            .swarm
            .behaviour_mut()
            .gossipsub
            .report_message_validation_result(&message_id, &propagation_source, acceptance)
        {
            tracing::debug!("GossipSub validation result did not match a cached message");
        }

        let Ok((source, payload)) = validated else {
            return;
        };
        match payload {
            ValidatedAvailability::Announcement { proof_hash } => {
                self.availability.add(proof_hash, source);
                self.emit(P2pEvent::AvailabilityAnnounced {
                    peer: source,
                    proof_hash,
                })
                .await;
            }
            ValidatedAvailability::Query {
                query_id,
                proof_hash,
            } => {
                let providers = self.availability.providers(proof_hash);
                if !providers.is_empty() {
                    let bytes = availability::response(
                        &self.config.chain_id,
                        query_id,
                        proof_hash,
                        &providers[..providers.len().min(MAX_PROVIDER_HINTS)],
                    );
                    let delay =
                        rand::random_range(QUERY_RESPONSE_MIN_DELAY..=QUERY_RESPONSE_MAX_DELAY);
                    self.delayed_responses.push(
                        async move {
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                            bytes
                        }
                        .boxed(),
                    );
                }
            }
            ValidatedAvailability::Response {
                query_id,
                proof_hash,
                providers,
            } => {
                for provider in &providers {
                    self.availability.add(proof_hash, *provider);
                }
                if self
                    .outstanding_queries
                    .get(&query_id)
                    .is_some_and(|(expected, _)| *expected == proof_hash)
                {
                    self.emit(P2pEvent::ProvidersDiscovered {
                        query_id,
                        proof_hash,
                        providers,
                    })
                    .await;
                }
            }
        }
    }

    async fn handle_exchange(
        &mut self,
        event: request_response::Event<GetProofRequest, GetProofResponse>,
    ) {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => self.handle_inbound_request(peer, request, channel).await,
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    self.handle_outbound_response(peer, request_id, response)
                        .await;
                }
            },
            request_response::Event::OutboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                if let Some(request) = self.outbound_requests.remove(&request_id) {
                    self.in_flight.remove(&(request.peer, request.proof_hash));
                    self.emit(P2pEvent::ProofRequestFailed {
                        request_id: request.application_id,
                        peer,
                        reason: error.to_string(),
                    })
                    .await;
                }
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                tracing::debug!(%peer, %error, "inbound proof request failed");
            }
            request_response::Event::ResponseSent { .. } => {}
        }
    }

    async fn handle_inbound_request(
        &mut self,
        peer: PeerId,
        request: GetProofRequest,
        channel: ResponseChannel<GetProofResponse>,
    ) {
        let proof_hash =
            if self.authorized_peers.contains(&peer) && request.chain_id == self.config.chain_id {
                ProofHash::try_from(request.proof_hash.as_slice()).ok()
            } else {
                None
            };
        let Some(proof_hash) = proof_hash else {
            let _ = self
                .swarm
                .behaviour_mut()
                .proof_exchange
                .send_response(channel, not_found_response(&self.config.chain_id));
            return;
        };

        let request_id = InboundProofRequestId(self.next_inbound_request_id);
        self.next_inbound_request_id += 1;
        self.inbound_requests.insert(
            request_id,
            InboundProofRequest {
                peer,
                proof_hash,
                channel,
            },
        );
        if !self
            .emit(P2pEvent::ProofRequested {
                request_id,
                peer,
                proof_hash,
            })
            .await
        {
            let _ = self.respond_proof(request_id, None);
        }
    }

    async fn handle_outbound_response(
        &mut self,
        peer: PeerId,
        request_id: OutboundRequestId,
        response: GetProofResponse,
    ) {
        let Some(request) = self.outbound_requests.remove(&request_id) else {
            return;
        };
        self.in_flight.remove(&(request.peer, request.proof_hash));
        if peer != request.peer || response.chain_id != self.config.chain_id {
            self.emit_failed(&request, "proof response context mismatch")
                .await;
            return;
        }

        match response.result {
            Some(get_proof_response::Result::Content(content)) => {
                let content = match validate_proof_content(
                    content,
                    request.proof_hash,
                    self.config.max_proof_bytes,
                ) {
                    Ok(content) => content,
                    Err(error) => {
                        self.emit_failed(&request, &error.to_string()).await;
                        return;
                    }
                };
                self.availability.add(request.proof_hash, peer);
                self.emit(P2pEvent::ProofReceived {
                    request_id: request.application_id,
                    peer,
                    content,
                })
                .await;
            }
            Some(get_proof_response::Result::NotFound(_)) => {
                self.availability.remove_provider(request.proof_hash, peer);
                self.emit(P2pEvent::ProofNotFound {
                    request_id: request.application_id,
                    peer,
                    proof_hash: request.proof_hash,
                })
                .await;
            }
            None => {
                self.emit_failed(&request, "proof response has no result")
                    .await;
            }
        }
    }

    async fn emit_failed(&mut self, request: &OutboundProofRequest, reason: &str) {
        self.emit(P2pEvent::ProofRequestFailed {
            request_id: request.application_id,
            peer: request.peer,
            reason: reason.to_owned(),
        })
        .await;
    }

    fn emit(&self, event: P2pEvent) -> impl Future<Output = bool> + Send + 'static {
        let events = self.events.clone();
        async move { events.send(event).await.is_ok() }
    }

    fn expire_queries(&mut self) {
        let timeout = self.config.proof_request_timeout;
        self.outstanding_queries
            .retain(|_, (_, created)| created.elapsed() < timeout);
    }
}

fn not_found_response(chain_id: &str) -> GetProofResponse {
    GetProofResponse {
        chain_id: chain_id.to_owned(),
        result: Some(get_proof_response::Result::NotFound(ProofNotFound {})),
    }
}

fn split_peer_address(mut address: Multiaddr) -> Result<(PeerId, Multiaddr)> {
    match address.pop() {
        Some(Protocol::P2p(peer)) => Ok((peer, address)),
        _ => Err(Error::InvalidConfig(
            "bootnode multiaddr must end with /p2p/<peer-id>".to_owned(),
        )),
    }
}

fn validate_proof_content(
    content: pulsar_verifier_proto::v1::ProofContent,
    expected_hash: ProofHash,
    maximum_bytes: usize,
) -> Result<ProofContent> {
    if content.proof.len() > maximum_bytes {
        return Err(Error::P2pProtocol(
            "proof response exceeds configured limit".to_owned(),
        ));
    }
    let response_hash = ProofHash::try_from(content.proof_hash.as_slice())?;
    if response_hash != expected_hash || ProofHash::digest(&content.proof) != response_hash {
        return Err(Error::P2pProtocol(
            "proof response failed hash binding".to_owned(),
        ));
    }
    Ok(ProofContent {
        proof_hash: response_hash,
        proof: content.proof.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_mismatched_proof_response_without_domain_status() {
        let expected = ProofHash::digest(b"expected");
        let content = pulsar_verifier_proto::v1::ProofContent {
            proof_hash: expected.as_bytes().to_vec(),
            proof: b"different".to_vec(),
        };

        assert!(matches!(
            validate_proof_content(content, expected, 1024),
            Err(Error::P2pProtocol(_))
        ));
    }

    #[test]
    fn rejects_oversized_proof_response() {
        let proof = vec![1_u8; 9];
        let hash = ProofHash::digest(&proof);
        let content = pulsar_verifier_proto::v1::ProofContent {
            proof_hash: hash.as_bytes().to_vec(),
            proof,
        };

        assert!(matches!(
            validate_proof_content(content, hash, 8),
            Err(Error::P2pProtocol(_))
        ));
    }
}
