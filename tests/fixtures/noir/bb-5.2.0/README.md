# Noir Compatibility Fixture

This fixture freezes the MVP Noir artifact contract:

- Nargo `1.0.0-beta.25`
- Barretenberg `5.2.0`
- UltraHonk
- `noir-recursive` verifier target
- Poseidon2 transcript
- zero knowledge enabled
- IPA accumulation disabled

The proof and public inputs are flat sequences of 32-byte big-endian BN254
field elements. The verification key is Barretenberg's raw binary key.

The source circuit asserts that private `x` differs from public `y`; the
fixture was generated with `x = 1` and `y = 2`.

Artifact SHA-256 checksums:

```text
cb48d3112bdb6891f62eea411a8de912faa9f04c0246a549850ea8908383a5a6  proof
9267d3dbed802941483f1afa2a6bc68de5f653128aca9bf1461c5d0a3ad36ed2  public_inputs
cbd42303d4ba5795553bf4642675b3f63bf4c6144622ba113bf0993356632578  vk
```

The ignored integration test requires:

```text
PULSAR_BB_PATH=/absolute/path/to/bb
PULSAR_BB_HOME=/absolute/path/to/home-containing-.bb-crs
```

The CRS must be provisioned before the verifier starts. Runtime CRS downloads
are intentionally outside the sidecar contract.
