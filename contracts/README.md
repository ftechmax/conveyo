# Shared contracts

Conveyo's shared contracts define compatibility across languages.
JSON Schema Draft 2020-12 defines structure; [wire-contract.md](../docs/wire-contract.md)
and [messagedata-uris.md](../docs/messagedata-uris.md) define behavior; fixtures specify
representative values and rejection cases. Resolve disagreements between schemas,
specifications, fixtures, and implementations together.

Envelope version remains `"1"`. Additive optional fields are compatible; breaking
envelope changes require a new version. Breaking application payload changes
require a new message URN. Library API versions are independent of protocol
versions. Contract changes must update specifications, schemas, fixtures, and all
implemented language conformance/interop tests in the same PR. Application
message schemas belong to applications and are not part of this baseline. The
numeric payload fixture tests serialization behavior. Types remain handwritten.

| Implementation/release | Envelope | MessageData formats | Status |
| --- | --- | --- | --- |
| .NET next preview (Stage 1) | `1` | inline base64; existing `pgbin` files/chunks, raw or gzip | Implemented |
| Go | planned `1` | planned same formats | Not implemented; starts in Stage 2 |

Release notes must record API changes and supported envelope/storage formats.
See [.NET next-preview notes](../docs/release-notes.md) for stricter input
validation in this baseline. No Postgres table layout changes are introduced.

## Fixtures and checks

`fixtures/manifest.json` lists every fixture, its schema(s), kind, and expected
structural validity. `envelope-cases.json` defines one minimal template and table-driven mutations
(`set`, `remove`, or `root`); `messageFile` reuses a payload example. The Python
validator and .NET tests expand these cases in memory, without generated fixture
files. Invalid cases must produce an invalid-envelope result (RabbitMQ routes
these to `_error` without retry). Reference cases likewise live in one table.
Nested locator/inline case files describe resolver outcomes: `valid`, `invalid`,
or `namespace` (a well-formed locator targeting another namespace or bucket).
UUID and timestamp formats are checked explicitly. Unknown fields remain valid.
Postgres namespace checks are semantic and run in native tests, separately from
structural schema checks. Reference schemas validate absolute URI structure;
resolvers decide whether a scheme and locator are supported.

The six golden envelopes define expected JSON values; whitespace, property order,
equivalent numeric formatting and escaping do not affect compatibility. .NET golden serialization
and shared fixture reads use this same directory.

Run from the repository root:

```sh
python3 -m venv /tmp/conveyo-contracts
/tmp/conveyo-contracts/bin/pip install -r scripts/requirements-contracts.txt
/tmp/conveyo-contracts/bin/python scripts/validate_contracts.py --contracts contracts
```

The validator uses a local schema registry and the pinned, test-only Python
[jsonschema validator](https://python-jsonschema.readthedocs.io/en/stable/validate/)
with format checks enabled. Schema validation runs only in tests.
.NET tests default to fixtures copied into their output directory for IDE runs;
CI passes `CONVEYO_CONTRACTS_DIR` explicitly and requires all fixtures to exist.
Future Go module tests must remain runnable without the repository contract tree;
repository CI will supply that tree separately for shared conformance.
