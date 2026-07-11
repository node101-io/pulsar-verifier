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

Both commands use `config/default.toml` when `--config` is omitted. `run` also handles `Ctrl-C` and `SIGTERM` through the same graceful shutdown path. The current scaffold does not yet start P2P, RPC, or proof verification services.

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
* Persistent proof storage
* Proof indexing and querying capabilities
* Advanced peer discovery mechanisms
* Metrics and observability tooling

## Status

⚠️ Work in progress.

The process lifecycle CLI is implemented. The P2P network, RPC services, proof storage, and verification pipeline are the next implementation stages and may change as the Pulsar ecosystem matures.
