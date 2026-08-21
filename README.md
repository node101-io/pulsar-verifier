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
- Event-driven verification worker with configurable bounded concurrency and retries
- Loopback-only chain-facing gRPC server with standard health reporting
- Committed `NewBlock` listener with bounded restart and reconnect recovery
- Validator authorization refresh driven by committed `validators_hash` changes
- Loopback consumer submission RPC with proof-to-transaction binding validation
- Store-first signed Cosmos transaction relay through local CometBFT `CheckTx`
- A pinned Noir/Barretenberg 5.2.0 compatibility fixture

Not yet implemented:

- The production Noir backend
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
`CometBFT` endpoint, chain listener, and network listeners required to enable it.

The packaged development config serves the chain-facing verification API on
`127.0.0.1:50051`. Config files that omit `[rpc]` keep the server disabled. This
phase intentionally accepts only literal loopback listeners because the RPC is
plaintext and unauthenticated.

Consumer submission is independently disabled by default. Enabling
`[submission]` opens `127.0.0.1:50052`, requires the Listener and result RPC, and
accepts a complete `Proof` plus an already signed Cosmos `TxRaw`. The sidecar
does not construct, simulate, or sign transactions.

## Consumer Submission

The submission service decodes `TxRaw -> TxBody -> Any -> MsgSubmitProof` and
requires exactly one verification-module message. It recomputes the SHA-256
hashes of the proof, public inputs, and verification key, then checks the proof
type and resulting `VerificationId` against the signed message before producing
any side effect.

After validation, content is written to the `ProofStore` first. The existing
`ProofStored` event automatically produces the P2P availability announcement;
the RPC does not maintain a second announcement path. The signed transaction is
then relayed to the configured local CometBFT endpoint with
`broadcast_tx_sync`.

A successful response means only that `CheckTx` accepted the transaction. It is
still pending chain inclusion, and verification cannot begin until the committed
Listener observes the matching request. Successful relay receipts are cached in
memory for 15 minutes so repeated identical requests do not rebroadcast the same
transaction. The cache and all proof state are lost on restart by design.

## Pulsar Listener

The Listener subscribes only to committed `NewBlock` events. It validates every
`verification.proof_submitted` descriptor before writing its `VerificationId` to
the Store; mempool and `CheckTx` signals never start verification.

Startup and every reconnect subscribe before querying chain state, then reconcile
the latest committed height and the preceding two proof heights through the
chain-owned `ProofsByHeight` query. This fixed three-height window matches the
chain's `H+2` and `H+3` commitment opportunities, so no permanent cursor or local
database is required. Duplicate live and recovered observations are suppressed by
the Store's idempotent transition.

The committed block header's `validators_hash` is the authorization change
signal. When it changes, the complete validator set at that exact height is
fetched and installed atomically in P2P. A failed fetch preserves the previous
allow-list; removal of the local validator clears authorization and shuts the
process down fail-closed. P2P therefore requires the Listener to be enabled.

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
Both methods are available through the generated Tonic service with the chain's
fixed 256 KiB message budget and standard gRPC health reporting.

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

The verification worker subscribes before taking a ready-record snapshot, claims
each ID through the Store's single-flight transition, and runs at most two jobs by
default. Timeout and backend failures remain operational `FAILED` states and never
become cryptographic `INVALID` verdicts. The current production registry is empty;
the deterministic fake verifier exists only in tests until the Noir backend is
implemented.

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
