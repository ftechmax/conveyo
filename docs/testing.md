# Testing

Install the .NET SDK specified in `global.json` and run tests from the repository
root. Integration tests also require Docker or Podman.

Integration tests use the pinned `Testcontainers.RabbitMq` and
`Testcontainers.PostgreSql` modules. NUnit starts one RabbitMQ container for its
integration namespace and one Postgres container for the storage fixture, waits
for readiness, and disposes them afterwards. Images are pinned in those fixtures;
host ports are allocated dynamically. Tests use unique queues and schemas.
No service connection environment variables or manually started services are needed.
Container startup failures fail the tests.

```sh
# Unit and shared contract tests only (the default):
dotnet test Conveyo.sln
# Integration tests only:
dotnet test Conveyo.sln --filter 'TestCategory=Integration'
# All tests, including container-backed integration tests:
dotnet test Conveyo.sln --filter 'TestCategory!=Integration|TestCategory=Integration'
```

The default filter is set in `tests/Directory.Build.targets`. An explicit
`--filter` overrides it. CI uses the all-tests filter to retain integration coverage.

Docker is discovered automatically. With rootless Podman, start its user socket
and select it for the test command:

```sh
systemctl --user start podman.socket
DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock" \
TESTCONTAINERS_RYUK_DISABLED=true \
dotnet test Conveyo.sln --filter 'TestCategory=Integration'
```

Rootless Podman runs with Ryuk disabled; the fixtures dispose their containers
on teardown, but a forcibly terminated test process can leave containers behind.
These environment variables apply only to this command. CI uses Docker discovery
and keeps Ryuk enabled.

See [Testcontainers configuration](https://dotnet.testcontainers.org/custom_configuration/)
for other runtimes or socket locations. CI runs the .NET tests with Testcontainers;
Compose still provisions services for the separate Python smoke tests.

See [Shared contracts](../contracts/README.md#fixtures-and-checks) for schema and
fixture validation.
