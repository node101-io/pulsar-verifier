# Pulsar Verifier

## Current CLI

The current implementation provides the process lifecycle required by the upcoming P2P and RPC components. The verifier runs in the foreground:

```bash
cargo run -- run --config config/default.toml
```

A second terminal can request graceful shutdown through the configured Unix control socket:

```bash
cargo run -- stop --config config/default.toml
```

Both commands use `config/default.toml` when `--config` is omitted. `run` also handles `Ctrl-C` and `SIGTERM` through the same graceful shutdown path. P2P starts when explicitly enabled; RPC and cryptographic verification services are not implemented yet.

## Validator P2P

P2P is disabled in `config/default.toml` so the process lifecycle can run without validator credentials. Copy `config/local.toml.example`, provide an absolute `priv_validator_key.json` path and the local CometBFT RPC URL, then set `p2p.enabled = true` to start the validator network.

The current P2P layer provides:

* QUIC v1 with TCP, Noise, and Yamux fallback
* Validator-set authorization bootstrapped once from CometBFT RPC
* Signed GossipSub availability announcements and provider queries
* Direct request-response transfer for opaque proof bytes
* Runtime authorization replacement ready for a future Pulsar Listener event

P2P startup is fail-closed: the verifier checks the configured chain ID, requires a fully synced CometBFT node, and verifies that its consensus-derived PeerId belongs to the active validator set. Proof-system routing and cryptographic verification are intentionally not connected yet.

## Ephemeral Proof Store

Proof lifecycle state is held in a process-local Moka cache keyed by the canonical BLAKE3 proof hash. One immutable record combines chain-owned metadata with optional proof bytes, allowing either the chain observation or the proof content to arrive first. Store transitions publish bounded events used by P2P to announce newly available content and answer inbound proof requests.

The development defaults allow 512 MiB of weighted proof records and proofs up to 8 MiB. `Verified` and `Wrong` records expire 15 minutes after verification completes. Capacity eviction can remove non-terminal records to preserve the process memory limit. The store is intentionally non-persistent; restarting the verifier discards proof content and lifecycle state.

Pulsar Verifier is a sidecar service responsible for proof propagation and verification within the Pulsar network.

It operates alongside Pulsar consensus nodes and acts as the bridge between the network's proof propagation layer and the blockchain application layer. The verifier receives proofs from peers, validates them, and exposes an API that allows the Pulsar application to query proof status and verification results.

## Motivation

Pulsar relies on externally generated cryptographic proofs to drive state transitions. Before a proof can influence consensus, validators must be able to independently verify that the proof is valid.

The verifier sidecar provides a dedicated execution environment for this responsibility, allowing proof processing to evolve independently from the core blockchain node.

## Responsibilities

The verifier has two primary responsibilities:

### 1. Proof Propagation

The verifier participates in a peer-to-peer network dedicated to proof dissemination.

Through its P2P driver, it:

* Receives proofs from peers
* Validates proof structure and metadata
* Stores and tracks proof lifecycle
* Rebroadcasts proofs across the network
* Maintains proof availability for other validators

### 2. Proof Verification API

The verifier exposes an API that can be consumed by the Pulsar application.

Through this API, the application can:

* Submit newly observed proofs
* Query proof existence
* Retrieve proof metadata
* Check verification status
* Determine whether a proof has been successfully verified

The blockchain application does not perform proof verification itself. Instead, it delegates this responsibility to the verifier and consumes the resulting verification status.

## High-Level Architecture

```text
                     ┌────────────────┐
                     │ Verifier Peers │
                     └───────┬────────┘
                             │
                             │ Proof Propagation
                             ▼
                ┌──────────────────────────┐
                │     Pulsar Verifier      │
                │                          │
                │        P2P Driver        │
                │            │             │
                │            ▼             │
                │    Verification Core     │
                │            ▲             │
                │            │             │
                │        API Server        │
                └────────────┬─────────────┘
                             │
                             │ Verification Queries
                             ▼
                 ┌─────────────────────────┐
                 │     Pulsar ABCI App     │
                 └───────────┬─────────────┘
                             │
                             │ Vote Extensions
                             ▼
                 ┌─────────────────────────┐
                 │     Consensus Layer     │
                 └─────────────────────────┘
```

## Consensus Integration

The verifier is designed to support Pulsar's proof-driven consensus flow.

At a high level:

1. A proof is propagated through the verifier network.
2. Each validator independently receives and verifies the proof using its local verifier instance.
3. The Pulsar application queries the verifier to determine whether the proof is valid.
4. Validators include their opinion on the proof within Vote Extensions.
5. Once a proof receives support from at least two-thirds of the network's voting power, it is considered accepted.
6. State transitions associated with that proof may then be executed on-chain.

By separating proof verification from consensus execution, Pulsar can maintain a lightweight application layer while still allowing validators to reach decentralized agreement on proof validity.

## Design Goals

* Decouple proof verification from consensus execution
* Enable independent evolution of verification logic
* Provide efficient proof propagation across validators
* Support deterministic consensus decisions based on validator votes
* Maintain a simple integration surface for Pulsar nodes
* Allow proof systems to evolve without modifying consensus-critical components

## Future Work

The verifier is intentionally designed as a standalone component. Future iterations may introduce:

* Additional proof systems
* Alternative propagation strategies
* Proof indexing and querying capabilities
* Advanced peer discovery mechanisms
* Metrics and observability tooling

## Status

⚠️ Work in progress.

The process lifecycle CLI, validator-authorized P2P transport, and event-driven ephemeral ProofStore are implemented. RPC services, Pulsar Listener, proof retrieval policy, and the cryptographic verification pipeline are the next implementation stages and may change as the Pulsar ecosystem matures.
