# Development and Testing

Use the .NET SDK specified in [`global.json`](../global.json). Run these commands from the repository root.

## Unit and integration tests

```sh
dotnet test Conveyo.sln
bash ./scripts/test-integration.sh
```

The first command excludes integration tests by default and needs no container runtime. An explicit `--filter` overrides that default.

Integration tests provision disposable RabbitMQ and Postgres instances with Testcontainers. No manually started services are needed. The local script requires rootless Podman, sets up its API socket, disables Ryuk for the run, and cleans up the run's containers and any API service it started.

Additional arguments are passed to `dotnet test`; the script keeps the integration category when narrowing a filter:

```sh
bash ./scripts/test-integration.sh --filter 'FullyQualifiedName~Postgres'
bash ./scripts/test-integration.sh --configuration Release --logger trx
```

If the script is killed before cleanup, find leftover containers with `podman ps --all --filter label=conveyo.test-run` and remove the relevant ones with `podman rm --force --volumes <container>`. A private API service and its `conveyo-podman.*` directory under `$XDG_RUNTIME_DIR` may also remain.

With Docker, run all tests as CI does:

```sh
dotnet test Conveyo.sln --configuration Release --filter 'TestCategory!=Integration|TestCategory=Integration'
```

## Protocol fixtures

[`contracts/fixtures/`](../contracts/fixtures/) contains the shared envelope, payload, and AMQP examples used by the [wire contract](wire-contract.md) and .NET tests. Test projects copy the fixtures into their output directories; IDE runs need no extra setup.

## End-to-end smoke tests

The smoke script builds and starts both weather applications and exercises commands, events, MessageData, and retry/error routing:

```sh
docker compose -f .github/compose.smoke.yml -p conveyo-smoke-local up -d
python3 scripts/smoke_test.py --configuration Release --fail-fast --log-dir artifacts/smoke
docker compose -f .github/compose.smoke.yml -p conveyo-smoke-local down --volumes
```

For rootless Podman, replace `docker compose` with `podman-compose` (installed separately). Always run teardown, including after a failed smoke test. These dependencies use disposable credentials and data; teardown deletes the Postgres volume. Ports 5672, 5432, and the producer's default 5033 must be free.

`--skip-build` uses existing application binaries. The default configuration is Debug; CI uses Release.

## Local packages

```sh
bash ./scripts/pack-dev.sh
```

This packs Release builds into `nupkgs/` with a timestamped `0.1.0-dev.*` version. Pass an output directory as the first argument to change the destination. Existing `Conveyo*.nupkg` and `Conveyo*.snupkg` files in that directory are removed before packing.
