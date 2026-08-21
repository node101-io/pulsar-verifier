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

use crate::{
    Error, Result,
    config::P2pConfig,
    proof::{Proof, VerificationId},
};

use super::{
    DriverEvent, InboundProofRequestId, ProofRequestId, QueryId,
    availability::{self, AvailabilityIndex, ValidatedAvailability},
    behaviour::{self, PulsarBehaviour, PulsarBehaviourEvent},
};

const MAX_PROVIDER_HINTS: usize = 128;
const QUERY_RESPONSE_MIN_DELAY: u64 = 50;
const QUERY_RESPONSE_MAX_DELAY: u64 = 250;

type CommandReply<T> = oneshot::Sender<Result<T>>;

pub(super) struct DriverParts {
    pub(super) driver: Driver,
    pub(super) client: DriverClient,
    pub(super) events: mpsc::Receiver<DriverEvent>,
    pub(super) ready: oneshot::Receiver<Result<()>>,
}

/// Private mailbox client; the Swarm remains owned by one driver task.
#[derive(Clone)]
pub(super) struct DriverClient {
    commands: mpsc::Sender<DriverCommand>,
}

impl DriverClient {
    /// Dials an already authorized peer using all known transport addresses.
    ///
    /// # Errors
    ///
    /// Returns an error if the driver is unavailable or rejects the dial.
    #[cfg(test)]
    pub(super) async fn dial(&self, peer: PeerId, addresses: Vec<Multiaddr>) -> Result<()> {
        self.call(|reply| DriverCommand::Dial {
            peer,
            addresses,
            reply,
        })
        .await
    }

    /// Announces local ownership of one verification.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver is unavailable or publishing fails.
    pub(super) async fn announce(&self, verification_id: VerificationId) -> Result<()> {
        self.call(|reply| DriverCommand::Announce {
            verification_id,
            reply,
        })
        .await
    }

    /// Removes local ownership after proof content leaves the process cache.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver task is unavailable.
    pub(super) async fn forget_local_proof(&self, verification_id: VerificationId) -> Result<()> {
        self.call(|reply| DriverCommand::ForgetLocalProof {
            verification_id,
            reply,
        })
        .await
    }

    /// Reconciles local ownership after a lagged store event subscription.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver task is unavailable.
    pub(super) async fn replace_local_proofs(
        &self,
        verification_ids: HashSet<VerificationId>,
    ) -> Result<()> {
        self.call(|reply| DriverCommand::ReplaceLocalProofs {
            verification_ids,
            reply,
        })
        .await
    }

    /// Broadcasts a provider lookup for one verification.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver is unavailable or publishing fails.
    pub(super) async fn query_availability(
        &self,
        verification_id: VerificationId,
    ) -> Result<QueryId> {
        self.call(|reply| DriverCommand::QueryAvailability {
            verification_id,
            reply,
        })
        .await
    }

    /// Returns providers currently known by the local ephemeral index.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver task is unavailable.
    pub(super) async fn providers(&self, verification_id: VerificationId) -> Result<Vec<PeerId>> {
        self.call(|reply| DriverCommand::Providers {
            verification_id,
            reply,
        })
        .await
    }

    /// Starts one complete proof request; active-chain gating belongs to the store workflow.
    ///
    /// # Errors
    ///
    /// Returns an error for unauthorized peers, duplicate requests, or driver failure.
    pub(super) async fn request_proof(
        &self,
        peer: PeerId,
        verification_id: VerificationId,
    ) -> Result<ProofRequestId> {
        self.call(|reply| DriverCommand::RequestProof {
            peer,
            verification_id,
            reply,
        })
        .await
    }

    /// Answers one inbound request with a complete proof or `ProofNotFound`.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown token, invalid content, or closed driver.
    pub(super) async fn respond_proof(
        &self,
        request_id: InboundProofRequestId,
        proof: Option<Proof>,
    ) -> Result<()> {
        self.call(|reply| DriverCommand::RespondProof {
            request_id,
            proof,
            reply,
        })
        .await
    }

    /// Atomically replaces active-validator authorization state.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver task is unavailable.
    pub(super) async fn replace_authorized_peers(&self, peers: HashSet<PeerId>) -> Result<()> {
        self.call(|reply| DriverCommand::ReplaceAuthorizedPeers { peers, reply })
            .await
    }

    /// Stops accepting new network work and waits for accepted exchanges to finish.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver is closed or already draining.
    pub(super) async fn drain(&self) -> Result<()> {
        self.call(|reply| DriverCommand::Drain { reply }).await
    }

    async fn call<T>(&self, command: impl FnOnce(CommandReply<T>) -> DriverCommand) -> Result<T> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| Error::P2pDriverClosed)?;
        response.await.map_err(|_| Error::P2pDriverClosed)?
    }
}

enum DriverCommand {
    #[cfg(test)]
    Dial {
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        reply: CommandReply<()>,
    },
    Announce {
        verification_id: VerificationId,
        reply: CommandReply<()>,
    },
    ForgetLocalProof {
        verification_id: VerificationId,
        reply: CommandReply<()>,
    },
    ReplaceLocalProofs {
        verification_ids: HashSet<VerificationId>,
        reply: CommandReply<()>,
    },
    QueryAvailability {
        verification_id: VerificationId,
        reply: CommandReply<QueryId>,
    },
    Providers {
        verification_id: VerificationId,
        reply: CommandReply<Vec<PeerId>>,
    },
    RequestProof {
        peer: PeerId,
        verification_id: VerificationId,
        reply: CommandReply<ProofRequestId>,
    },
    RespondProof {
        request_id: InboundProofRequestId,
        proof: Option<Proof>,
        reply: CommandReply<()>,
    },
    ReplaceAuthorizedPeers {
        peers: HashSet<PeerId>,
        reply: CommandReply<()>,
    },
    Drain {
        reply: CommandReply<()>,
    },
}

enum DriverState {
    Running,
    Draining { reply: CommandReply<()> },
    Drained,
}

struct OutboundProofRequest {
    application_id: ProofRequestId,
    peer: PeerId,
    verification_id: VerificationId,
}

struct InboundProofRequest {
    peer: PeerId,
    verification_id: VerificationId,
    channel: ResponseChannel<GetProofResponse>,
}

/// Single owner of transport state, protocol behaviours and request correlation.
pub(super) struct Driver {
    swarm: Swarm<PulsarBehaviour>,
    config: P2pConfig,
    local_peer_id: PeerId,
    topic: gossipsub::IdentTopic,
    authorized_peers: HashSet<PeerId>,
    availability: AvailabilityIndex,
    outstanding_queries: HashMap<QueryId, (VerificationId, Instant)>,
    outbound_requests: HashMap<OutboundRequestId, OutboundProofRequest>,
    in_flight: HashSet<(PeerId, VerificationId)>,
    inbound_requests: HashMap<InboundProofRequestId, InboundProofRequest>,
    next_proof_request_id: u64,
    next_inbound_request_id: u64,
    commands: mpsc::Receiver<DriverCommand>,
    events: mpsc::Sender<DriverEvent>,
    delayed_responses: FuturesUnordered<futures::future::BoxFuture<'static, Vec<u8>>>,
    pending_listeners: HashSet<libp2p::core::transport::ListenerId>,
    listeners: HashSet<libp2p::core::transport::ListenerId>,
    ready: Option<oneshot::Sender<Result<()>>>,
    state: DriverState,
}

impl Driver {
    /// Builds a driver and its bounded application channels without spawning tasks.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid transports, listeners, bootnodes or behaviour config.
    pub(super) fn build(
        config: P2pConfig,
        identity: identity::Keypair,
        mut authorized_peers: HashSet<PeerId>,
    ) -> Result<DriverParts> {
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
        let mut listeners = HashSet::new();
        for address in &config.listen_addresses {
            let id = swarm.listen_on(address.clone()).map_err(|error| {
                Error::P2pDriver(format!("failed to listen on {address}: {error}"))
            })?;
            pending_listeners.insert(id);
            listeners.insert(id);
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

        Ok(DriverParts {
            driver: Self {
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
                listeners,
                ready: Some(ready_tx),
                state: DriverState::Running,
            },
            client: DriverClient {
                commands: command_tx,
            },
            events: event_rx,
            ready: ready_rx,
        })
    }

    /// Runs until drained ownership closes the mailbox or an unrecoverable failure occurs.
    ///
    /// # Errors
    ///
    /// Returns an error when the command channel closes unexpectedly or a fatal event occurs.
    pub(super) async fn run(mut self) -> Result<()> {
        let mut cleanup = tokio::time::interval(Duration::from_secs(1));
        loop {
            let should_stop = tokio::select! {
                command = self.commands.recv() => {
                    match command {
                        Some(command) => self.handle_command(command),
                        None if matches!(self.state, DriverState::Drained) => true,
                        None => return Err(Error::P2pDriverClosed),
                    }
                }
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await?;
                    false
                },
                Some(response) = self.delayed_responses.next(), if !self.delayed_responses.is_empty() => {
                    if let Err(error) = self.swarm.behaviour_mut().gossipsub.publish(self.topic.clone(), response) {
                        tracing::debug!(%error, "availability response was not published");
                    }
                    false
                }
                _ = cleanup.tick() => {
                    self.expire_queries();
                    false
                },
            };
            self.finish_drain();
            if should_stop {
                return Ok(());
            }
        }
    }

    fn handle_command(&mut self, command: DriverCommand) -> bool {
        match command {
            #[cfg(test)]
            DriverCommand::Dial {
                peer,
                addresses,
                reply,
            } => {
                let _ = reply.send(self.dial(peer, addresses));
            }
            DriverCommand::Announce {
                verification_id,
                reply,
            } => {
                let _ = reply.send(self.announce(verification_id));
            }
            DriverCommand::ForgetLocalProof {
                verification_id,
                reply,
            } => {
                self.availability
                    .remove_provider(verification_id, self.local_peer_id);
                // TODO: Add availability leases and periodic re-announcement so remote
                // peers eventually discard ownership that could not be withdrawn.
                let _ = reply.send(Ok(()));
            }
            DriverCommand::ReplaceLocalProofs {
                verification_ids,
                reply,
            } => {
                self.availability
                    .replace_provider_proofs(self.local_peer_id, &verification_ids);
                let _ = reply.send(Ok(()));
            }
            DriverCommand::QueryAvailability {
                verification_id,
                reply,
            } => {
                let _ = reply.send(self.query_availability(verification_id));
            }
            DriverCommand::Providers {
                verification_id,
                reply,
            } => {
                let _ = reply.send(Ok(self.availability.providers(verification_id)));
            }
            DriverCommand::RequestProof {
                peer,
                verification_id,
                reply,
            } => {
                let _ = reply.send(self.request_proof(peer, verification_id));
            }
            DriverCommand::RespondProof {
                request_id,
                proof,
                reply,
            } => {
                let _ = reply.send(self.respond_proof(request_id, proof));
            }
            DriverCommand::ReplaceAuthorizedPeers { peers, reply } => {
                if self.is_running() {
                    self.replace_authorized_peers(peers);
                    let _ = reply.send(Ok(()));
                } else {
                    let _ = reply.send(Err(Error::P2pDraining));
                }
            }
            DriverCommand::Drain { reply } => match self.state {
                DriverState::Running => {
                    self.begin_drain(reply);
                }
                DriverState::Draining { .. } => {
                    let _ = reply.send(Err(Error::P2pDraining));
                }
                DriverState::Drained => {
                    let _ = reply.send(Ok(()));
                }
            },
        }
        false
    }

    #[cfg(test)]
    fn dial(&mut self, peer: PeerId, addresses: Vec<Multiaddr>) -> Result<()> {
        self.require_running()?;
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

    fn announce(&mut self, verification_id: VerificationId) -> Result<()> {
        self.availability.add(verification_id, self.local_peer_id);
        if !self.is_running() {
            return Ok(());
        }
        let bytes = availability::announcement(&self.config.chain_id, verification_id);
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

    fn query_availability(&mut self, verification_id: VerificationId) -> Result<QueryId> {
        self.require_running()?;
        let query_id = QueryId::random();
        let bytes = availability::query(&self.config.chain_id, query_id, verification_id);
        let published = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(self.topic.clone(), bytes);
        if let Err(error) = published
            && !matches!(error, gossipsub::PublishError::NoPeersSubscribedToTopic)
        {
            return Err(Error::P2pDriver(format!(
                "failed to publish query: {error}"
            )));
        }
        self.outstanding_queries
            .insert(query_id, (verification_id, Instant::now()));
        Ok(query_id)
    }

    fn request_proof(
        &mut self,
        peer: PeerId,
        verification_id: VerificationId,
    ) -> Result<ProofRequestId> {
        self.require_running()?;
        if !self.authorized_peers.contains(&peer) {
            return Err(Error::P2pAuthorization(format!(
                "refusing proof request to unauthorized peer {peer}"
            )));
        }
        if !self.in_flight.insert((peer, verification_id)) {
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
                verification_id: verification_id.as_bytes().to_vec(),
            },
        );
        self.outbound_requests.insert(
            request_id,
            OutboundProofRequest {
                application_id,
                peer,
                verification_id,
            },
        );
        Ok(application_id)
    }

    fn respond_proof(
        &mut self,
        request_id: InboundProofRequestId,
        proof: Option<Proof>,
    ) -> Result<()> {
        let inbound = self.inbound_requests.remove(&request_id).ok_or_else(|| {
            Error::P2pDriver(format!("unknown inbound proof request {request_id:?}"))
        })?;
        let result = match proof {
            Some(proof) => {
                if proof.encoded_len() > self.config.max_proof_bytes {
                    return Err(Error::P2pProtocol(
                        "proof exceeds configured limit".to_owned(),
                    ));
                }
                if proof.verification_id() != inbound.verification_id {
                    return Err(Error::P2pProtocol(
                        "proof response does not match inbound request".to_owned(),
                    ));
                }
                get_proof_response::Result::Proof((&proof).into())
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
    }

    fn begin_drain(&mut self, reply: CommandReply<()>) {
        tracing::info!(
            inbound = self.inbound_requests.len(),
            outbound = self.outbound_requests.len(),
            delayed = self.delayed_responses.len(),
            "p2p drain started"
        );
        for listener in self.listeners.drain() {
            self.pending_listeners.remove(&listener);
            let _ = self.swarm.remove_listener(listener);
        }
        self.outstanding_queries.clear();
        self.state = DriverState::Draining { reply };
    }

    fn finish_drain(&mut self) {
        if !matches!(self.state, DriverState::Draining { .. })
            || !self.inbound_requests.is_empty()
            || !self.outbound_requests.is_empty()
            || !self.in_flight.is_empty()
            || !self.delayed_responses.is_empty()
        {
            return;
        }

        let DriverState::Draining { reply } =
            std::mem::replace(&mut self.state, DriverState::Drained)
        else {
            unreachable!("drain state checked above");
        };
        tracing::info!("p2p driver drained");
        let _ = reply.send(Ok(()));
    }

    fn is_running(&self) -> bool {
        matches!(self.state, DriverState::Running)
    }

    fn require_running(&self) -> Result<()> {
        if self.is_running() {
            Ok(())
        } else {
            Err(Error::P2pDraining)
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<PulsarBehaviourEvent>) -> Result<()> {
        match event {
            SwarmEvent::NewListenAddr {
                listener_id,
                address,
            } => {
                self.listeners.insert(listener_id);
                self.pending_listeners.remove(&listener_id);
                if self.is_running() {
                    self.emit(DriverEvent::Listening { address }).await;
                }
                if self.pending_listeners.is_empty()
                    && let Some(ready) = self.ready.take()
                {
                    let _ = ready.send(Ok(()));
                }
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                self.listeners.remove(&listener_id);
                self.pending_listeners.remove(&listener_id);
                if let Some(ready) = self.ready.take() {
                    let _ = ready.send(Err(Error::P2pDriver(format!(
                        "listener failed during startup: {error}"
                    ))));
                }
                if self.is_running() {
                    return Err(Error::P2pDriver(format!("listener failed: {error}")));
                }
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                if self.is_running() {
                    self.emit(DriverEvent::PeerConnected { peer: peer_id })
                        .await;
                }
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established: 0,
                ..
            } => {
                self.availability.remove_peer(peer_id);
                if self.is_running() {
                    self.emit(DriverEvent::PeerDisconnected { peer: peer_id })
                        .await;
                }
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
        if !self.is_running() {
            return;
        }
        match payload {
            ValidatedAvailability::Announcement { verification_id } => {
                self.availability.add(verification_id, source);
                self.emit(DriverEvent::AvailabilityAnnounced {
                    peer: source,
                    verification_id,
                })
                .await;
            }
            ValidatedAvailability::Query {
                query_id,
                verification_id,
            } => {
                let providers = self.availability.providers(verification_id);
                if !providers.is_empty() {
                    let bytes = availability::response(
                        &self.config.chain_id,
                        query_id,
                        verification_id,
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
                verification_id,
                providers,
            } => {
                for provider in &providers {
                    self.availability.add(verification_id, *provider);
                }
                if self
                    .outstanding_queries
                    .get(&query_id)
                    .is_some_and(|(expected, _)| *expected == verification_id)
                {
                    self.emit(DriverEvent::ProvidersDiscovered {
                        query_id,
                        verification_id,
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
                    self.in_flight
                        .remove(&(request.peer, request.verification_id));
                    self.availability
                        .remove_provider(request.verification_id, request.peer);
                    self.emit(DriverEvent::ProofRequestFailed {
                        request_id: request.application_id,
                        peer,
                        verification_id: request.verification_id,
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
        if !self.is_running() {
            let _ = self
                .swarm
                .behaviour_mut()
                .proof_exchange
                .send_response(channel, not_found_response(&self.config.chain_id));
            return;
        }
        let verification_id =
            if self.authorized_peers.contains(&peer) && request.chain_id == self.config.chain_id {
                VerificationId::try_from(request.verification_id.as_slice()).ok()
            } else {
                None
            };
        let Some(verification_id) = verification_id else {
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
                verification_id,
                channel,
            },
        );
        if !self
            .emit(DriverEvent::ProofRequested {
                request_id,
                peer,
                verification_id,
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
        self.in_flight
            .remove(&(request.peer, request.verification_id));
        if peer != request.peer || response.chain_id != self.config.chain_id {
            self.emit_failed(&request, "proof response context mismatch")
                .await;
            return;
        }

        match response.result {
            Some(get_proof_response::Result::Proof(proof)) => {
                let proof = match validate_proof(
                    proof,
                    request.verification_id,
                    self.config.max_proof_bytes,
                ) {
                    Ok(proof) => proof,
                    Err(error) => {
                        self.emit_failed(&request, &error.to_string()).await;
                        return;
                    }
                };
                self.availability.add(request.verification_id, peer);
                self.emit(DriverEvent::ProofReceived {
                    request_id: request.application_id,
                    peer,
                    verification_id: request.verification_id,
                    proof,
                })
                .await;
            }
            Some(get_proof_response::Result::NotFound(_)) => {
                self.availability
                    .remove_provider(request.verification_id, peer);
                self.emit(DriverEvent::ProofNotFound {
                    request_id: request.application_id,
                    peer,
                    verification_id: request.verification_id,
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
        self.availability
            .remove_provider(request.verification_id, request.peer);
        self.emit(DriverEvent::ProofRequestFailed {
            request_id: request.application_id,
            peer: request.peer,
            verification_id: request.verification_id,
            reason: reason.to_owned(),
        })
        .await;
    }

    fn emit(&self, event: DriverEvent) -> impl Future<Output = bool> + Send + 'static {
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

fn validate_proof(
    wire_proof: pulsar_verifier_proto::v1::Proof,
    expected_id: VerificationId,
    maximum_bytes: usize,
) -> Result<Proof> {
    use prost::Message as _;

    if wire_proof.encoded_len() > maximum_bytes {
        return Err(Error::P2pProtocol(
            "proof response exceeds configured limit".to_owned(),
        ));
    }
    let proof = Proof::try_from(wire_proof)
        .map_err(|error| Error::P2pProtocol(format!("invalid proof response: {error}")))?;
    if proof.verification_id() != expected_id {
        return Err(Error::P2pProtocol(
            "proof response failed verification ID binding".to_owned(),
        ));
    }
    Ok(proof)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::proof::ProofType;

    fn proof(bytes: &'static [u8]) -> Proof {
        Proof {
            proof_type: ProofType::MinaPickles,
            proof: Bytes::from_static(bytes),
            public_inputs: Bytes::from_static(b"inputs"),
            verification_key: Bytes::from_static(b"key"),
        }
    }

    #[test]
    fn rejects_mismatched_proof_response_without_domain_status() {
        let expected = proof(b"expected").verification_id();
        let wire_proof = pulsar_verifier_proto::v1::Proof::from(&proof(b"different"));

        assert!(matches!(
            validate_proof(wire_proof, expected, 1024),
            Err(Error::P2pProtocol(_))
        ));
    }

    #[test]
    fn rejects_oversized_proof_response() {
        let proof = Proof {
            proof_type: ProofType::MinaPickles,
            proof: Bytes::from(vec![1; 9]),
            public_inputs: Bytes::new(),
            verification_key: Bytes::new(),
        };
        let id = proof.verification_id();
        let wire_proof = pulsar_verifier_proto::v1::Proof::from(&proof);

        assert!(matches!(
            validate_proof(wire_proof, id, 8),
            Err(Error::P2pProtocol(_))
        ));
    }

    #[test]
    fn rejects_each_mismatched_proof_component() {
        let original = proof(b"proof");
        let expected = original.verification_id();
        let mut variants = Vec::new();

        let mut changed = original.clone();
        changed.public_inputs = Bytes::from_static(b"other-inputs");
        variants.push(changed);
        let mut changed = original.clone();
        changed.verification_key = Bytes::from_static(b"other-key");
        variants.push(changed);
        let mut changed = original;
        changed.proof_type = ProofType::NoirBarretenberg;
        variants.push(changed);

        for changed in variants {
            assert!(matches!(
                validate_proof(
                    pulsar_verifier_proto::v1::Proof::from(&changed),
                    expected,
                    1024,
                ),
                Err(Error::P2pProtocol(_))
            ));
        }
    }

    #[test]
    fn rejects_unspecified_wire_proof_type() {
        let expected = proof(b"proof").verification_id();
        let wire_proof = pulsar_verifier_proto::v1::Proof {
            proof_type: pulsar_verifier_proto::chain_v1::ProofType::Unspecified.into(),
            proof: b"proof".to_vec(),
            public_inputs: b"inputs".to_vec(),
            verification_key: b"key".to_vec(),
        };

        assert!(matches!(
            validate_proof(wire_proof, expected, 1024),
            Err(Error::P2pProtocol(_))
        ));
    }
}
