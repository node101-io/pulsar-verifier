# Pulsar Verifier

Pulsar Verifier is a validator sidecar for acquiring and independently verifying
zero-knowledge proofs. The Pulsar application asks the local sidecar for completed
verdicts and uses those verdicts while constructing vote extensions; consensus
aggregation remains chain-owned.

## Current Status

Implemented:

- Foreground `run`/`stop` lifecycle with signal handling and a Unix control socket
- Validator-authorized libp2p networking over QUIC with TCP/Noise/Yamux fallback
- Signed GossipSub proof availability and direct composite proof exchange
- Chain-compatible SHA-256 `VerificationId`
- Ephemeral, bounded Moka `ProofStore`
- Chain-owned verification service and `ProofType` proto code generation
- Store lifecycle aligned with `QUEUED`, `VERIFYING`, `COMPLETED`, and `FAILED`
- Tested request/response mapping for the chain verification service contract
- A pinned Noir/Barretenberg 5.2.0 compatibility fixture

Not yet implemented:

- Running gRPC servers
- Verification workers and the production Noir backend
- Pulsar block/event listener and restart reconciliation
- Consumer submission RPC and Cosmos transaction relay
- Automatic P2P retrieval after an on-chain observation

## CLI

The verifier runs in the foreground:

```bash
cargo run -- run --config config/default.toml
```

A second terminal can request graceful shutdown:

```bash
cargo run -- stop --config config/default.toml
```

Both commands default to `config/default.toml`. `run` handles `Ctrl-C`, `SIGTERM`,
and the control socket through the same shutdown path. P2P is disabled in the
development config; `config/local.toml.example` shows the validator credentials,
CometBFT endpoint, and listeners required to enable it.

## Verification Contract

The complete verification input is:

```text
Proof
├── proof_type
├── proof
├── public_inputs
└── verification_key
```

The canonical 32-byte identifier matches the chain implementation:

```text
proof_hash            = SHA256(proof)
public_inputs_hash    = SHA256(public_inputs)
verification_key_hash = SHA256(verification_key)

verification_id = SHA256(
  "pulsar/verification/v1\0" ||
  BE32(proof_type) ||
  proof_hash ||
  public_inputs_hash ||
  verification_key_hash
)
```

The chain recognizes Mina Pickles and Noir Barretenberg proof types. The MVP
sidecar will initially implement only Noir verification. A Mina request can
therefore remain without a completed local verdict; it must never be reported as
cryptographically invalid merely because the backend is unavailable.

The chain-facing result contract deliberately separates:

- `COMPLETED + VALID|INVALID`: a cryptographic verdict
- `FAILED + failure`: an operational backend/runtime failure used for diagnostics
- `UNAVAILABLE`, `QUEUED`, `VERIFYING`: local non-terminal state

Only completed verdicts are returned by the consensus-facing
`GetVerificationResults` method. `GetProofStatuses` exposes local diagnostics.

## Proof Store

Proof state is held in a process-local cache:

```text
Moka<VerificationId, Arc<ProofRecord>>
```

A record merges an optional complete `Proof` with chain-observation metadata.
Either can arrive first. Verification becomes `Queued` only when both are present,
and `begin_verification` provides a single-flight `Queued -> Verifying` claim.
Only `Completed` records start the default 15-minute retention period; failed jobs
remain available for explicit retry policy.

The development defaults bound the cache to 512 MiB and each encoded composite
proof to 8 MiB. Capacity eviction may remove non-terminal records to preserve the
memory ceiling. Restarting the process loses all records and lifecycle state by
design.

## Validator P2P

The crate-private `P2pService` owns the libp2p Driver, the ProofStore/network
Worker, and their task lifecycle. The current network provides:

- QUIC v1 with TCP/Noise/Yamux fallback
- Startup validator authorization from CometBFT
- Signed GossipSub availability announcements and provider queries
- Direct transfer of the complete composite `Proof`
- Ordered drain of accepted proof exchanges during shutdown

Availability is keyed by `VerificationId`. A downloaded proof is accepted only
after recomputing the identifier and only when that ID was already observed
on-chain. BLAKE3 is used solely for GossipSub message deduplication, not proof
identity.

## Noir Compatibility

`tests/fixtures/noir/bb-5.2.0` pins the MVP artifact format:

- Nargo `1.0.0-beta.25`
- Barretenberg `5.2.0`
- UltraHonk, Poseidon2, zero knowledge enabled, IPA disabled

The compatibility test is ignored in normal test runs because it requires an
external `bb` binary and pre-provisioned CRS:

```bash
PULSAR_BB_PATH=/absolute/path/to/bb \
PULSAR_BB_HOME=/absolute/path/to/home \
cargo test --test noir_compatibility -- --ignored
```

The production verifier worker/backend remains a later milestone.
