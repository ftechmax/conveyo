#!/usr/bin/env python3
"""End-to-end smoke test for the Conveyo messaging + MessageData stack.

Starts the example Producer (web app) and Consumer (worker) locally, drives
every example endpoint and asserts the expected log evidence on both sides.

Run from anywhere — paths are resolved relative to this file:

    python3 scripts/smoke_test.py

Requires `dotnet` plus RabbitMQ on 127.0.0.1:5672 and Postgres on
127.0.0.1:5432. For cluster-backed local runs, forward those ports manually
before starting the smoke test.

    # Example manual forwards:
    # kubectl port-forward -n rabbitmq-system svc/rabbitmq 5672:5672
    # kubectl port-forward -n postgres-system svc/postgres 5432:5432
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import pathlib
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Iterator, List, Optional, Sequence

ROOT = pathlib.Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / "examples"
PRODUCER_PROJ = EXAMPLES / "Weather.Producer"
CONSUMER_PROJ = EXAMPLES / "Weather.Consumer"

DEFAULT_PRODUCER_URL = "http://127.0.0.1:5033"
RABBITMQ_PORT = 5672
POSTGRES_PORT = 5432

READY_TIMEOUT = 90
MESSAGE_TIMEOUT = 25
FAILURE_TIMEOUT = 45  # error path goes through 3 retries with exp backoff

# Log patterns emitted by the Conveyo.RabbitMQ framework itself. Keep these in
# line with src/Conveyo.RabbitMQ/LogMessages.cs
RETRY_LOG_FMT = r"Retry {attempt}/{max} for message "
ERROR_ROUTING_LOG_FMT = r"failed after {attempts} attempts; routing to {queue}\."


class SmokeError(RuntimeError):
    """A test assertion failed."""


@dataclass
class ManagedProcess:
    name: str
    proc: subprocess.Popen
    log_path: pathlib.Path

    def alive(self) -> bool:
        return self.proc.poll() is None

    def terminate(self, grace: float = 5.0) -> None:
        if not self.alive():
            return
        with contextlib.suppress(ProcessLookupError):
            self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=grace)
            return
        except subprocess.TimeoutExpired:
            pass
        with contextlib.suppress(ProcessLookupError):
            self.proc.terminate()
        try:
            self.proc.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=grace)


def _spawn(name: str, cmd: Sequence[str], log_path: pathlib.Path, *,
           cwd: Optional[pathlib.Path] = None,
           env: Optional[dict] = None) -> ManagedProcess:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.unlink(missing_ok=True)
    log_file = open(log_path, "wb", buffering=0)
    proc = subprocess.Popen(
        list(cmd),
        cwd=str(cwd) if cwd else None,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return ManagedProcess(name=name, proc=proc, log_path=log_path)


def wait_for_tcp(host: str, port: int, timeout: float, name: str) -> None:
    """Wait until a TCP endpoint accepts connections."""
    deadline = time.monotonic() + timeout
    last_err: Optional[OSError] = None
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            try:
                s.connect((host, port))
                return
            except OSError as e:
                last_err = e
        time.sleep(0.3)
    raise SmokeError(
        f"Timed out waiting for {name} after {timeout:.0f}s "
        f"(last error: {last_err})")


class LogTailer:
    """Polls a growing file and yields lines from a given offset.

    Cheap, side-effect-free. A new tailer is created per assertion so each
    assertion starts looking from "now" rather than from the start of file.
    """

    def __init__(self, path: pathlib.Path):
        self.path = path
        self.offset = path.stat().st_size if path.exists() else 0
        self._buffer = b""

    def wait_for(self, pattern: re.Pattern, timeout: float) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for line in self._read_new_lines():
                if pattern.search(line):
                    return line
            time.sleep(0.15)
        raise SmokeError(
            f"Timed out after {timeout:.0f}s waiting for /{pattern.pattern}/ "
            f"in {self.path}")

    def _read_new_lines(self) -> Iterator[str]:
        if not self.path.exists():
            return
        with open(self.path, "rb") as f:
            f.seek(self.offset)
            chunk = f.read()
            self.offset += len(chunk)
        self._buffer += chunk
        while b"\n" in self._buffer:
            line, self._buffer = self._buffer.split(b"\n", 1)
            yield line.decode("utf-8", errors="replace")


def read_log_tail(path: pathlib.Path, max_lines: int = 40) -> str:
    if not path.exists():
        return "<log file does not exist>"

    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-max_lines:]) or "<log file is empty>"


def wait_for_initial_line(process: ManagedProcess, pattern: re.Pattern,
                          timeout: float) -> str:
    """Wait for `pattern` to appear from the start of the file."""
    deadline = time.monotonic() + timeout
    seen = 0
    while time.monotonic() < deadline:
        path = process.log_path
        if path.exists():
            with open(path, "rb") as f:
                f.seek(seen)
                chunk = f.read()
                seen += len(chunk)
            for line in chunk.decode("utf-8", errors="replace").splitlines():
                if pattern.search(line):
                    return line
        if not process.alive():
            raise SmokeError(
                f"{process.name} exited with code {process.proc.returncode} "
                f"before /{pattern.pattern}/ appeared\n\n"
                f"Last {process.name} log lines:\n"
                f"{read_log_tail(path)}")
        time.sleep(0.2)
    raise SmokeError(
        f"Timed out after {timeout:.0f}s waiting for /{pattern.pattern}/ "
        f"in {process.log_path}")


def http_post_json(url: str, payload: dict, *, timeout: float = 10.0) -> int:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status


def http_post_multipart(url: str, fields: dict, files: dict,
                        *, timeout: float = 15.0) -> int:
    boundary = "----conveyo-smoke-" + uuid.uuid4().hex
    chunks: List[bytes] = []
    for name, value in fields.items():
        chunks.append(f"--{boundary}\r\n".encode())
        chunks.append(
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        chunks.append(str(value).encode("utf-8"))
        chunks.append(b"\r\n")
    for name, (filename, content, content_type) in files.items():
        chunks.append(f"--{boundary}\r\n".encode())
        chunks.append(
            f'Content-Disposition: form-data; name="{name}"; '
            f'filename="{filename}"\r\n'.encode())
        chunks.append(f"Content-Type: {content_type}\r\n\r\n".encode())
        chunks.append(content)
        chunks.append(b"\r\n")
    chunks.append(f"--{boundary}--\r\n".encode())
    body = b"".join(chunks)
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status


def wait_for_http(url: str, timeout: float, process: Optional[ManagedProcess] = None) -> None:
    deadline = time.monotonic() + timeout
    last_err: Optional[Exception] = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                if resp.status < 500:
                    return
        except (urllib.error.URLError, ConnectionError, OSError) as e:
            last_err = e
        if process is not None and not process.alive():
            raise SmokeError(
                f"{process.name} exited with code {process.proc.returncode} "
                f"while waiting for {url}\n\n"
                f"Last {process.name} log lines:\n"
                f"{read_log_tail(process.log_path)}")
        time.sleep(0.5)
    raise SmokeError(
        f"Timed out waiting for {url} after {timeout:.0f}s "
        f"(last error: {last_err})")


@dataclass
class TestResult:
    name: str
    ok: bool
    detail: str = ""
    duration_s: float = 0.0


def _run_case(name: str, body) -> TestResult:
    start = time.monotonic()
    try:
        body()
    except SmokeError as e:
        return TestResult(name=name, ok=False, detail=str(e),
                          duration_s=time.monotonic() - start)
    except Exception as e:  # noqa: BLE001
        return TestResult(name=name, ok=False,
                          detail=f"unexpected: {e!r}",
                          duration_s=time.monotonic() - start)
    return TestResult(name=name, ok=True, duration_s=time.monotonic() - start)


def scenario_submit_observation(producer_url: str, producer_log: pathlib.Path,
                                consumer_log: pathlib.Path) -> None:
    station = str(uuid.uuid4())
    # Tail from "now" so we ignore noise from previous tests / startup.
    ctail = LogTailer(consumer_log)
    ptail = LogTailer(producer_log)

    status = http_post_json(
        f"{producer_url}/stations/observations",
        {
            "StationId": station,
            "Location": "Smoke Town",
            "HumidityPercent": 72,
            "WindSpeedMs": 4.5,
            "PressureHpa": 1011.2,
            "IsPrecipitating": True,
        },
    )
    if status >= 300:
        raise SmokeError(f"Producer returned HTTP {status}")

    ctail.wait_for(
        re.compile(rf"Received SubmitWeatherObservationCommand:.*station={station}"),
        MESSAGE_TIMEOUT)
    ctail.wait_for(
        re.compile(r"Publishing WeatherObservationRecordedEvent"),
        MESSAGE_TIMEOUT)

    ptail.wait_for(
        re.compile(rf"\[observation recorded\] station={station}"),
        MESSAGE_TIMEOUT)


def scenario_upload_readings_string(producer_url: str,
                                    producer_log: pathlib.Path,
                                    consumer_log: pathlib.Path) -> None:
    station = str(uuid.uuid4())
    payload = (
        "ts,temp_c,humidity_pct\n"
        f"2025-01-01T00:00:00Z,12.3,80\n"
        f"2025-01-01T00:05:00Z,12.4,79\n"
        f"# smoke-{station}\n"
    )
    expected_bytes = len(payload.encode("utf-8"))
    ctail = LogTailer(consumer_log)
    ptail = LogTailer(producer_log)

    status = http_post_json(
        f"{producer_url}/stations/samples/readings",
        {
            "StationId": station,
            "Format": "csv",
            "Readings": payload,
        },
    )
    if status >= 300:
        raise SmokeError(f"Producer returned HTTP {status}")

    ctail.wait_for(
        re.compile(rf"Received UploadWeatherReadingsCommand for station {station}"),
        MESSAGE_TIMEOUT)
    # Proves MessageData<string> round-trip + hydration via postgres storage.
    line = ctail.wait_for(
        re.compile(r"Readings \(\d+ chars / (\d+) bytes\): "),
        MESSAGE_TIMEOUT)
    m = re.search(r"/ (\d+) bytes", line)
    if not m or int(m.group(1)) != expected_bytes:
        raise SmokeError(
            f"Readings size mismatch — log line: {line!r}, expected {expected_bytes}B")
    ptail.wait_for(
        re.compile(rf"\[sample archived\] station={station} kind=Readings"),
        MESSAGE_TIMEOUT)


def scenario_upload_radar_bytes(producer_url: str,
                                producer_log: pathlib.Path,
                                consumer_log: pathlib.Path) -> None:
    station = str(uuid.uuid4())
    image_bytes = bytes(range(256)) * 4  # 1024 deterministic bytes
    expected_bytes = len(image_bytes)
    file_name = f"radar-{station}.bin"
    ctail = LogTailer(consumer_log)
    ptail = LogTailer(producer_log)

    status = http_post_multipart(
        f"{producer_url}/stations/samples/radar",
        fields={"StationId": station},
        files={"Image": (file_name, image_bytes, "application/octet-stream")},
    )
    if status >= 300:
        raise SmokeError(f"Producer returned HTTP {status}")

    ctail.wait_for(
        re.compile(rf"Received UploadRadarImageCommand for station {station}"),
        MESSAGE_TIMEOUT)
    line = ctail.wait_for(
        re.compile(r"Image bytes received: (\d+) bytes"),
        MESSAGE_TIMEOUT)
    m = re.search(r"received: (\d+) bytes", line)
    if not m or int(m.group(1)) != expected_bytes:
        raise SmokeError(
            f"Image size mismatch — log line: {line!r}, expected {expected_bytes}B")
    ptail.wait_for(
        re.compile(rf"\[sample archived\] station={station} kind=Radar"),
        MESSAGE_TIMEOUT)


def scenario_upload_satellite_stream(producer_url: str,
                                     producer_log: pathlib.Path,
                                     consumer_log: pathlib.Path) -> None:
    station = str(uuid.uuid4())
    content = (
        f"SAT-FEED smoke {station}\n"
        + ("line of telemetry data\n" * 25)
    )
    expected_bytes = len(content.encode("utf-8"))
    ctail = LogTailer(consumer_log)
    ptail = LogTailer(producer_log)

    status = http_post_json(
        f"{producer_url}/stations/samples/satellite",
        {
            "StationId": station,
            "FeedName": f"sat-{station}.txt",
            "Content": content,
        },
    )
    if status >= 300:
        raise SmokeError(f"Producer returned HTTP {status}")

    ctail.wait_for(
        re.compile(rf"Received UploadSatelliteFeedCommand for station {station}"),
        MESSAGE_TIMEOUT)
    line = ctail.wait_for(
        re.compile(r"Feed stream received: (\d+) bytes"),
        MESSAGE_TIMEOUT)
    m = re.search(r"received: (\d+) bytes", line)
    if not m or int(m.group(1)) != expected_bytes:
        raise SmokeError(
            f"Feed size mismatch — log line: {line!r}, expected {expected_bytes}B")
    ptail.wait_for(
        re.compile(rf"\[sample archived\] station={station} kind=Satellite"),
        MESSAGE_TIMEOUT)


def scenario_failure_routing(producer_url: str,
                             producer_log: pathlib.Path,
                             consumer_log: pathlib.Path) -> None:
    station = str(uuid.uuid4())
    reason = f"smoke-failure-{station}"
    ctail = LogTailer(consumer_log)

    status = http_post_json(
        f"{producer_url}/stations/failures",
        {"StationId": station, "Reason": reason},
    )
    if status >= 300:
        raise SmokeError(f"Producer returned HTTP {status}")

    # The consumer logs the receive, throws, retries (exp backoff: 1s, 2s, 4s)
    # then routes the message to the *_error queue.
    ctail.wait_for(
        re.compile(rf"Received SimulateStationFailureCommand for station {station}"),
        MESSAGE_TIMEOUT)
    for attempt in (1, 2, 3):
        ctail.wait_for(
            re.compile(RETRY_LOG_FMT.format(attempt=attempt, max=3)),
            FAILURE_TIMEOUT)
    ctail.wait_for(
        re.compile(ERROR_ROUTING_LOG_FMT.format(
            attempts=4, queue=re.escape("Weather.Consumer_error"))),
        FAILURE_TIMEOUT)


def build_examples() -> None:
    print("[build] dotnet build examples...", flush=True)
    for proj in (PRODUCER_PROJ / "Weather.Producer.csproj",
                 CONSUMER_PROJ / "Weather.Consumer.csproj"):
        subprocess.check_call(
            ["dotnet", "build", str(proj), "-nologo", "-clp:NoSummary",
             "-v:q", "-c", "Debug"],
            cwd=str(ROOT))


def run(args: argparse.Namespace) -> int:
    log_dir = pathlib.Path(args.log_dir).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_build:
        build_examples()

    producer_log = log_dir / "producer.log"
    consumer_log = log_dir / "consumer.log"
    producer: Optional[ManagedProcess] = None
    consumer: Optional[ManagedProcess] = None

    base_env = os.environ.copy()
    base_env.setdefault("DOTNET_CLI_TELEMETRY_OPTOUT", "1")
    base_env.setdefault("DOTNET_NOLOGO", "1")
    base_env["ASPNETCORE_ENVIRONMENT"] = "Development"
    base_env["DOTNET_ENVIRONMENT"] = "Development"

    producer_env = base_env | {"ASPNETCORE_URLS": args.producer_url}

    try:
        print(f"[services] waiting for RabbitMQ on 127.0.0.1:"
              f"{RABBITMQ_PORT}", flush=True)
        wait_for_tcp("127.0.0.1", RABBITMQ_PORT, 30.0, "RabbitMQ")
        print(f"[services] waiting for Postgres on 127.0.0.1:"
              f"{POSTGRES_PORT}", flush=True)
        wait_for_tcp("127.0.0.1", POSTGRES_PORT, 30.0, "Postgres")

        print("[start] consumer...", flush=True)
        consumer = _spawn(
            "consumer",
            ["dotnet", "run",
             "--project", str(CONSUMER_PROJ),
             "--no-build", "--no-launch-profile",
             "-c", "Debug"],
            consumer_log, cwd=ROOT, env=base_env)
        wait_for_initial_line(
            consumer, re.compile(r"\[SMOKE\] Weather\.Consumer ready"),
            READY_TIMEOUT)
        print("[ready] consumer", flush=True)

        print("[start] producer...", flush=True)
        producer = _spawn(
            "producer",
            ["dotnet", "run",
             "--project", str(PRODUCER_PROJ),
             "--no-build", "--no-launch-profile",
             "-c", "Debug"],
            producer_log, cwd=ROOT, env=producer_env)
        wait_for_initial_line(
            producer, re.compile(r"\[SMOKE\] Weather\.Producer ready"),
            READY_TIMEOUT)
        # Confirm HTTP is live too — the [SMOKE] marker fires from the
        # lifetime hook which runs before Kestrel finishes binding in some
        # edge cases.
        wait_for_http(f"{args.producer_url}/swagger/v1/swagger.json", 30.0, producer)
        print("[ready] producer", flush=True)

        scenarios = [
            ("submit_observation (Send + event round-trip)",
             lambda: scenario_submit_observation(
                 args.producer_url, producer_log, consumer_log)),
            ("upload_readings (MessageData<string>)",
             lambda: scenario_upload_readings_string(
                 args.producer_url, producer_log, consumer_log)),
            ("upload_radar (MessageData<byte[]>)",
             lambda: scenario_upload_radar_bytes(
                 args.producer_url, producer_log, consumer_log)),
            ("upload_satellite (MessageData<Stream>)",
             lambda: scenario_upload_satellite_stream(
                 args.producer_url, producer_log, consumer_log)),
            ("simulate_failure (3 retries + error queue routing)",
             lambda: scenario_failure_routing(
                 args.producer_url, producer_log, consumer_log)),
        ]

        results: List[TestResult] = []
        for name, body in scenarios:
            print(f"[case] {name} ...", flush=True)
            case_result = _run_case(name, body)
            results.append(case_result)
            tag = "ok" if case_result.ok else "FAIL"
            print(f"[case] {name} -> {tag} ({case_result.duration_s:.1f}s)", flush=True)
            if not case_result.ok:
                print(f"       {case_result.detail}", flush=True)
                if args.fail_fast:
                    break

        print()
        print("=" * 60)
        passed = sum(1 for case_result in results if case_result.ok)
        for case_result in results:
            tag = "ok  " if case_result.ok else "FAIL"
            print(f"  [{tag}] {case_result.name} ({case_result.duration_s:.1f}s)")
            if not case_result.ok:
                print(f"         {case_result.detail}")
        print("=" * 60)
        print(f"{passed}/{len(results)} passed", flush=True)
        return 0 if passed == len(results) else 1

    finally:
        print("[teardown]", flush=True)
        for p in (producer, consumer):
            if p is not None:
                p.terminate()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--producer-url", default=DEFAULT_PRODUCER_URL,
                   help="URL the producer should bind to "
                        f"(default: {DEFAULT_PRODUCER_URL})")
    p.add_argument("--log-dir",
                   default=str(ROOT / "scripts" / "_smoke_logs"),
                   help="Where to write process logs")
    p.add_argument("--skip-build", action="store_true",
                   help="Skip `dotnet build` (assume binaries are current)")
    p.add_argument("--fail-fast", action="store_true",
                   help="Stop on first failing scenario")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    try:
        return run(args)
    except SmokeError as e:
        print(f"smoke test aborted: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
