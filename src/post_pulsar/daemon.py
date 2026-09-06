"""Single-instance foreground daemon with sequential durable request execution."""

from __future__ import annotations

import ipaddress
import json
import logging
import os
import secrets
import signal
import socket
import stat
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Protocol

from post_pulsar.control import CONTROL_SCHEMA, ControlApplication, ControlRequest
from post_pulsar.locking import LockLease, LockManager
from post_pulsar.state import RunRequestRecord, StateRepository

RequestExecutor = Callable[[RunRequestRecord], Mapping[str, object]]


class _Server(Protocol):
    server_address: tuple[object, ...]

    def serve_forever(self) -> None: ...
    def shutdown(self) -> None: ...
    def server_close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class EndpointRecord:
    """Non-secret discovery record owned by exactly one daemon incarnation."""

    protocol: str
    address: str
    pid: int
    process_started_at: int
    startup_nonce: str
    capabilities: Mapping[str, object]

    @classmethod
    def read(cls, path: str | Path) -> EndpointRecord:
        source = Path(path)
        from post_pulsar.secure_files import assert_owner_file

        assert_owner_file(source)
        metadata = source.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError("endpoint record is unsafe")
        if metadata.st_nlink != 1:
            raise RuntimeError("endpoint record is unsafe")
        if os.name != "nt" and metadata.st_mode & 0o077:
            raise RuntimeError("endpoint record permissions are too broad")
        value = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or set(value) != {
            "protocol",
            "address",
            "pid",
            "process_started_at",
            "startup_nonce",
            "capabilities",
        }:
            raise RuntimeError("endpoint record schema is invalid")
        return cls(**value)

    def write(self, path: str | Path) -> None:
        from post_pulsar.secure_files import atomic_owner_write

        destination = Path(path)
        payload = (
            json.dumps(asdict(self), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        atomic_owner_write(destination, payload)


class ForegroundDaemon:
    """Own the lifetime instance lock, HTTP thread, and one sequential worker."""

    def __init__(
        self,
        state_directory: str | Path,
        endpoint_record_file: str | Path,
        host: str,
        port: int,
        *,
        control_application: ControlApplication | None = None,
        agent_capability_file: str | Path | None = None,
        recovery: Callable[[], object] | None = None,
        schedule_admission: Callable[[], object] | None = None,
        request_executor: RequestExecutor | None = None,
        poll_seconds: float = 0.05,
        server_factory: Callable[
            [tuple[str, int], type[BaseHTTPRequestHandler]], _Server
        ]
        | None = None,
    ) -> None:
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            raise ValueError(
                "control host must be a literal loopback address"
            ) from None
        if not address.is_loopback or str(address) != host:
            raise ValueError(
                "control host must be a canonical literal loopback address"
            )
        self._state = Path(state_directory)
        self._endpoint = Path(endpoint_record_file)
        self._host = host
        self._port = port
        self._control = control_application
        self._capability_file = (
            None if agent_capability_file is None else str(Path(agent_capability_file))
        )
        self._recovery = recovery
        self._schedule_admission = schedule_admission
        self._executor = request_executor
        self._poll = poll_seconds
        self._server_factory = server_factory or _make_http_server
        self._locks = LockManager(self._state)
        self._lease: LockLease | None = None
        self._server: _Server | None = None
        self._http_thread: threading.Thread | None = None
        self._worker_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._claim_gate = threading.Lock()
        self._nonce = secrets.token_hex(32)
        self._worker_token = f"daemon-{self._nonce[:16]}"
        self._started_at = _process_start_identity(os.getpid())

    def start(self) -> None:
        """Recover first, then expose admission and worker processing."""
        if self._lease is not None:
            raise RuntimeError("daemon is already started")
        lease = self._locks.acquire_instance()
        self._lease = lease
        try:
            self._stop.clear()
            if self._recovery is not None:
                self._invoke_callback("recovery", self._recovery)
            if self._schedule_admission is not None:
                self._invoke_callback("schedule", self._schedule_admission)
            if self._endpoint.exists():
                existing = EndpointRecord.read(self._endpoint)
                if _record_process_is_live(existing):
                    raise RuntimeError("endpoint record is owned by a live process")
            handler = _handler_for(self._control)
            self._server = self._server_factory((self._host, self._port), handler)
            bound_host, bound_port = self._server.server_address[:2]
            record = EndpointRecord(
                protocol=CONTROL_SCHEMA,
                address=f"{bound_host}:{bound_port}",
                pid=os.getpid(),
                process_started_at=self._started_at,
                startup_nonce=self._nonce,
                capabilities={
                    "control_api_major": 1,
                    "agent_capability_file": self._capability_file,
                },
            )
            record.write(self._endpoint)
            self._http_thread = threading.Thread(
                target=self._server.serve_forever,
                name="post-pulsar-control",
                daemon=False,
            )
            self._http_thread.start()
            self._worker_thread = threading.Thread(
                target=self._worker, name="post-pulsar-publisher", daemon=False
            )
            self._worker_thread.start()
        except BaseException:
            self._cleanup_runtime()
            raise

    def run_forever(self) -> None:
        """Start and block until SIGINT/SIGTERM requests an orderly stop."""
        self.start()
        stopped = threading.Event()
        previous: dict[int, object] = {}

        def request_stop(_signum: int, _frame: object) -> None:
            stopped.set()

        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, request_stop)
        try:
            while not stopped.wait(0.1) and not self._stop.is_set():
                continue
        finally:
            self.stop()
            for signum, handler in previous.items():
                signal.signal(signum, handler)  # type: ignore[arg-type]

    def stop(self) -> None:
        """Stop admission, then wait for the current sequential safe boundary."""
        if self._lease is None:
            return
        with self._claim_gate:
            self._stop.set()
        if self._server is not None:
            self._server.shutdown()
        if self._http_thread is not None:
            self._http_thread.join()
        if self._worker_thread is not None:
            self._worker_thread.join()
        self._cleanup_runtime()

    def _worker(self) -> None:
        try:
            self._worker_loop()
        except Exception:
            logging.getLogger(__name__).error("publisher_worker_stopped")
        finally:
            # A persistence failure cannot leave a healthy-looking HTTP service
            # accepting work with no publisher to process it.
            self._stop.set()
            if self._server is not None:
                self._server.shutdown()

    def dispatch_if_running(self, callback: Callable[[], object]) -> bool:
        """Commit one start decision under the same gate used by shutdown."""
        with self._claim_gate:
            if self._stop.is_set():
                return False
        callback()
        return True

    def _invoke_callback(self, phase: str, callback: Callable[[], object]) -> None:
        try:
            self.dispatch_if_running(callback)
        except Exception:
            with StateRepository.open_existing(
                self._state / "post_pulsar.sqlite3"
            ) as repository:
                repository.record_callback_failure(phase=phase)

    def _worker_loop(self) -> None:
        database = self._state / "post_pulsar.sqlite3"
        while not self._stop.is_set():
            if self._schedule_admission is not None:
                self._invoke_callback("schedule", self._schedule_admission)
            if self._stop.is_set():
                break
            if self._executor is None or not database.exists():
                self._stop.wait(self._poll)
                continue
            with StateRepository.open_existing(database) as repository:
                with self._claim_gate:
                    if self._stop.is_set():
                        break
                    request = repository.claim_next_run_request(self._worker_token)
                if request is None:
                    self._stop.wait(self._poll)
                    continue
                try:
                    result = self._executor(request)
                except Exception:
                    repository.complete_run_request(
                        request.request_id,
                        worker_token=self._worker_token,
                        result={"code": "request_failed"},
                        failed=True,
                    )
                else:
                    repository.complete_run_request(
                        request.request_id,
                        worker_token=self._worker_token,
                        result=result,
                    )

    def _cleanup_runtime(self) -> None:
        if self._server is not None:
            self._server.server_close()
            self._server = None
        try:
            record = EndpointRecord.read(self._endpoint)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
            record = None
        if (
            record is not None
            and record.pid == os.getpid()
            and record.process_started_at == self._started_at
            and secrets.compare_digest(record.startup_nonce, self._nonce)
        ):
            self._endpoint.unlink(missing_ok=True)
            _fsync_parent(self._endpoint)
        if self._lease is not None:
            self._lease.release()
            self._lease = None


def _handler_for(
    application: ControlApplication | None,
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "POST-PULSAR"
        sys_version = ""

        def do_GET(self) -> None:
            self._serve()

        def do_POST(self) -> None:
            self._serve()

        def do_PATCH(self) -> None:
            self._serve()

        def do_DELETE(self) -> None:
            self._serve()

        def do_OPTIONS(self) -> None:
            self.send_response(405)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, _format: str, *_args: object) -> None:
            return

        def _serve(self) -> None:
            length_text = self.headers.get("Content-Length", "0")
            try:
                length = int(length_text)
            except ValueError:
                length = -1
            if length < 0 or length > 1048576:
                self.send_error(413)
                return
            body = self.rfile.read(length)
            if application is None:
                payload = b'{"schema":"post-pulsar.control/v1","ok":false,"error":{"code":"unavailable"}}\n'
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            response = application.handle(
                ControlRequest(
                    self.command, self.path, dict(self.headers.items()), body
                )
            )
            self.send_response(response.status)
            for name, value in response.headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(response.body)))
            self.end_headers()
            self.wfile.write(response.body)

    return Handler


def _make_http_server(
    address: tuple[str, int], handler: type[BaseHTTPRequestHandler]
) -> _Server:
    if ipaddress.ip_address(address[0]).version == 6:

        class IPv6HTTPServer(HTTPServer):
            address_family = socket.AF_INET6

        return IPv6HTTPServer(address, handler, bind_and_activate=True)
    return HTTPServer(address, handler, bind_and_activate=True)


def _process_start_identity(pid: int) -> int:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="ascii").split()
        return int(fields[21])
    except (OSError, ValueError, IndexError):
        if pid == os.getpid():
            return time.time_ns()
        return -1


def _record_process_is_live(record: EndpointRecord) -> bool:
    try:
        os.kill(record.pid, 0)
    except (OSError, ValueError):
        return False
    identity = _process_start_identity(record.pid)
    return identity < 0 or identity == record.process_started_at


def _fsync_parent(path: Path) -> None:
    from post_pulsar.secure_files import fsync_directory

    fsync_directory(path.parent)


__all__ = ["EndpointRecord", "ForegroundDaemon"]
