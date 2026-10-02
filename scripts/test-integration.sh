#!/usr/bin/env bash
# Runs the container-backed integration tests against local rootless Podman.
# Usage: scripts/test-integration.sh [dotnet test arguments...]
#
# Nothing is configured globally: the Podman API socket, DOCKER_HOST, and the
# Testcontainers settings apply only to this run. A --filter argument narrows the
# integration tests instead of replacing the integration category.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CATEGORY_FILTER='TestCategory=Integration'
SERVICE_START_ATTEMPTS=50
# The fixtures label their containers with this id so cleanup only touches this run.
RUN_ID="$(date +%Y%m%d%H%M%S)-$$"
RUN_LABEL="conveyo.test-run=$RUN_ID"

service_dir=""
service_pid=""
socket=""

# Environment problems exit with 2 so they are distinguishable from test failures.
fail() {
  printf 'test-integration: %s\n' "$@" >&2
  exit 2
}

socket_responds() {
  podman --remote --url "unix://$1" info >/dev/null 2>&1
}

# Talks to the API socket the tests use once it is known, so cleanup looks at the
# same containers Testcontainers created.
podman_api() {
  if [[ -n "$socket" ]]; then
    podman --remote --url "unix://$socket" "$@"
  else
    podman "$@"
  fi
}

cleanup() {
  local status=$?
  trap - EXIT

  # The fixtures dispose their containers. This only finds some when the test
  # process was interrupted before teardown, because Ryuk is disabled.
  local leftover
  leftover="$(podman_api ps --all --quiet --filter "label=$RUN_LABEL" 2>/dev/null || true)"
  if [[ -n "$leftover" ]]; then
    echo "test-integration: removing containers this run left behind:" >&2
    podman_api ps --all --filter "label=$RUN_LABEL" --format '  {{.Names}} ({{.Image}}, {{.Status}})' >&2 || true
    # shellcheck disable=SC2086 # one container id per word
    podman_api rm --force --volumes $leftover >/dev/null || true
  fi

  if [[ -n "$service_pid" ]]; then
    kill "$service_pid" 2>/dev/null || true
    wait "$service_pid" 2>/dev/null || true
  fi
  if [[ -n "$service_dir" ]]; then
    rm -rf "$service_dir"
  fi

  exit "$status"
}

filter="$CATEGORY_FILTER"
dotnet_args=()
while (($#)); do
  case "$1" in
    --filter)
      (($# >= 2)) || fail "--filter requires an expression."
      filter="($CATEGORY_FILTER)&($2)"
      shift 2
      ;;
    --filter=*)
      filter="($CATEGORY_FILTER)&(${1#--filter=})"
      shift
      ;;
    *)
      dotnet_args+=("$1")
      shift
      ;;
  esac
done

command -v dotnet >/dev/null || fail "dotnet was not found on PATH; install the SDK named in global.json."
command -v podman >/dev/null || fail "podman was not found on PATH; the integration tests need rootless Podman."
if ! podman_error="$(podman info 2>&1 >/dev/null)"; then
  fail "podman is installed but not usable:" "$podman_error"
fi

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

runtime_dir="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
socket="$runtime_dir/podman/podman.sock"
if [[ -S "$socket" ]] && socket_responds "$socket"; then
  echo "test-integration: using the active Podman API socket $socket" >&2
else
  # No user socket is active, so serve the API on a private socket for this run only.
  service_dir="$(mktemp -d "$runtime_dir/conveyo-podman.XXXXXX")" \
    || fail "could not create a directory for the Podman API socket in $runtime_dir."
  socket="$service_dir/podman.sock"
  podman system service --time=0 "unix://$socket" >"$service_dir/service.log" 2>&1 &
  service_pid=$!

  attempt=0
  until socket_responds "$socket"; do
    attempt=$((attempt + 1))
    if ! kill -0 "$service_pid" 2>/dev/null || ((attempt >= SERVICE_START_ATTEMPTS)); then
      fail "the Podman API service did not become available on $socket:" "$(cat "$service_dir/service.log")"
    fi
    sleep 0.2
  done
  echo "test-integration: started a Podman API service for this run on $socket" >&2
fi

DOCKER_HOST="unix://$socket" \
TESTCONTAINERS_RYUK_DISABLED=true \
CONVEYO_TEST_RUN_ID="$RUN_ID" \
  dotnet test "$REPO_ROOT/Conveyo.sln" --filter "$filter" ${dotnet_args[@]+"${dotnet_args[@]}"}
