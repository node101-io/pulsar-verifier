# Vendored Proto Sources

The chain-owned contracts are copied without modification from
`node101-io/pulsar-chain` commit
`8c2a3e111d4d4d4974055cf930e346baced7c4c9`:

- `pulsarchain/verification/v1/types.proto`
- `pulsarchain/verification/v1/params.proto`
- `pulsarchain/verification/v1/query.proto`
- `pulsar/verifier/v1/verification_service.proto`

Their custom option dependencies are pinned to the same revisions recorded in
that commit's `buf.lock`:

- `amino/amino.proto`: `buf.build/cosmos/cosmos-sdk` commit
  `65fa41963e6a41dd95a35934239029df`
- `gogoproto/gogo.proto`: `buf.build/cosmos/gogo-proto` commit
  `88ef6483f90f478fb938c37dde52ece3`
- `google/protobuf/descriptor.proto`: exported transitively with the pinned
  Gogo proto contract
- `cosmos/base/query/v1beta1/pagination.proto`: Cosmos SDK commit
  `65fa41963e6a41dd95a35934239029df`
- `cosmos_proto/cosmos.proto`: Cosmos Proto commit
  `04467658e59e44bbb22fe568206e1f70`
- `google/api/{annotations,http}.proto`: Google APIs commit
  `c17df5b2beca46928cc87d5656bd5343`

Update these files together whenever the chain contract revision changes.
