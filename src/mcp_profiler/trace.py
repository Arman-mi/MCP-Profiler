"""Trace record schema and writer.

A trace is newline-delimited JSON. The first record of each run is a header:

  {"kind": "header", "run_id", "server", "version", "argv", "command", "pid",
   "t_wall_ns", "t_wall_iso", "t_mono_ns", "capture_payloads", "payload_cap"}

t_wall_ns and t_mono_ns are read back to back at startup, so any t_mono_ns in the
run converts to wall clock as t_wall_ns + (t - header.t_mono_ns).

Then one record per observed message, in the field order of MESSAGE_FIELDS:

  run_id     uuid4 per proxy invocation
  server     --name, default basename of the server command
  seq        counter per run, both directions, taken when the message is read
  t_mono_ns  time.monotonic_ns() when the message's final byte was read
  direction  "c2s" (client to server) or "s2c" (server to client)
  kind       "request" | "response" | "notification" | "parse_error"
  rpc_id     the message's id (null for notifications and parse errors)
  method     requests and notifications only
  is_error   responses only: whether it carries a JSON-RPC `error`
  bytes      raw line length, newline included
  tool_name  params.name of a tools/call request
  ref_id     params.requestId of a notifications/cancelled: the request it cancels
  proxy_ns   monotonic time from reading the message to having forwarded it

With capture_payloads, records also carry "payload" (params, result or error as
JSON text; the raw line for a parse error), cut to payload_cap bytes, and
"payload_truncated".

seq and t_mono_ns increase together; file order can differ slightly when one
direction is blocked on a full pipe, so sort by seq.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections import deque
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any

from mcp_profiler import __version__

DEFAULT_DIR = "traces"
DEFAULT_PAYLOAD_CAP = 2048

# The writer thread wakes this often to write what has accumulated. Waking it per
# record instead (a blocking queue) cost ~100 us per message in futex wakeups and
# GIL handoffs, far more than the record itself.
FLUSH_INTERVAL_S = 0.1

# How long close() waits for the writer thread to finish flushing.
CLOSE_TIMEOUT_S = 10.0

MESSAGE_FIELDS = (
    "run_id", "server", "seq", "t_mono_ns", "direction", "kind", "rpc_id",
    "method", "is_error", "bytes", "tool_name", "ref_id", "proxy_ns",
)  # fmt: skip


def new_run_id() -> str:
    return str(uuid.uuid4())


def default_path(run_id: str) -> str:
    return os.path.join(DEFAULT_DIR, f"{run_id}.jsonl")


def _valid_id(value: Any) -> bool:
    # MCP ids are strings or integers. bool is an int subclass, so rule it out.
    return isinstance(value, (str, int)) and not isinstance(value, bool)


def classify(msg: Any) -> tuple[str, Any, str | None, bool | None] | None:
    """Return (kind, rpc_id, method, is_error) for a parsed JSON-RPC 2.0 message,
    or None if it is not one."""
    if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
        return None
    method = msg.get("method")
    if method is not None:
        if not isinstance(method, str):
            return None
        if "id" not in msg:
            return "notification", None, method, None
        if not _valid_id(msg["id"]):
            return None
        return "request", msg["id"], method, None
    if "id" not in msg or ("result" in msg) == ("error" in msg):
        return None
    rpc_id = msg["id"]
    # A null id is allowed on an error response to a request whose id was unreadable.
    if not (_valid_id(rpc_id) or (rpc_id is None and "error" in msg)):
        return None
    return "response", rpc_id, None, "error" in msg


def _truncate(text: str, cap: int) -> tuple[str, bool]:
    data = text.encode("utf-8", "replace")
    if len(data) <= cap:
        return text, False
    # Cut at a byte boundary, dropping a split multi-byte character.
    return data[:cap].decode("utf-8", "ignore"), True


class TraceWriter:
    """Appends trace records to a file from a background thread.

    record() runs on the event loop: it parses a copy of the line, serializes the
    record, and appends bytes to an unbounded deque. It never signals the thread,
    so recording costs no cross-thread wakeup. The thread wakes every
    FLUSH_INTERVAL_S (and on close), joins whatever is pending and writes it,
    holding the GIL as little as possible. Writes use O_APPEND and end on record
    boundaries, so several proxies can share one trace file without interleaving
    records.
    """

    def __init__(
        self,
        path: str,
        *,
        run_id: str,
        server: str,
        argv: list[str],
        command: list[str],
        capture_payloads: bool = False,
        payload_cap: int = DEFAULT_PAYLOAD_CAP,
    ) -> None:
        self.path = path
        self.run_id = run_id
        self.server = server
        self.capture_payloads = capture_payloads
        self.payload_cap = payload_cap
        self.records = 0  # messages recorded, header excluded
        self.record_errors = 0  # bugs in record(); counted rather than breaking forwarding
        self.write_errors = 0
        self.first_error: Exception | None = None
        self._seq = 0
        self._closed = False

        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

        # Read back to back: this pair is what aligns monotonic times to wall clock.
        t_mono_ns = time.monotonic_ns()
        t_wall_ns = time.time_ns()
        header = {
            "kind": "header",
            "run_id": run_id,
            "server": server,
            "version": __version__,
            "argv": argv,
            "command": command,
            "pid": os.getpid(),
            "t_wall_ns": t_wall_ns,
            "t_wall_iso": datetime.fromtimestamp(t_wall_ns / 1e9, timezone.utc).isoformat(),
            "t_mono_ns": t_mono_ns,
            "capture_payloads": capture_payloads,
            "payload_cap": payload_cap,
        }
        try:
            # Synchronous, before any message can be recorded: it is always first.
            self._write(self._encode(header))
        except OSError:
            os.close(self._fd)
            raise

        # deque.append and popleft are atomic, so no lock is needed between threads.
        self._pending: deque[bytes] = deque()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="mcp-profiler-trace", daemon=True)
        self._thread.start()

    def next_seq(self) -> int:
        seq = self._seq
        self._seq += 1
        return seq

    def record(self, direction: str, line: bytes, seq: int, t_mono_ns: int, proxy_ns: int) -> None:
        """Record one message. `line` is only read, never modified or re-encoded.
        Never raises: it runs inside the forwarding pumps."""
        try:
            self._pending.append(self._encode(self._make_record(direction, line, seq, t_mono_ns, proxy_ns)))
            self.records += 1
        except Exception as e:
            self.record_errors += 1
            self.first_error = self.first_error or e

    def _make_record(self, direction: str, line: bytes, seq: int, t_mono_ns: int, proxy_ns: int) -> dict[str, Any]:
        msg: Any = None
        info = None
        try:
            msg = json.loads(line)
            info = classify(msg)
        except (ValueError, RecursionError):  # ValueError covers bad JSON and bad UTF-8
            pass
        if info is None:
            kind, rpc_id, method, is_error = "parse_error", None, None, None
        else:
            kind, rpc_id, method, is_error = info

        tool_name = ref_id = None
        if method == "tools/call" and kind == "request":
            params = msg.get("params")
            if isinstance(params, dict) and isinstance(params.get("name"), str):
                tool_name = params["name"]
        elif method == "notifications/cancelled":
            params = msg.get("params")
            if isinstance(params, dict) and _valid_id(params.get("requestId")):
                ref_id = params["requestId"]

        rec = {
            "run_id": self.run_id,
            "server": self.server,
            "seq": seq,
            "t_mono_ns": t_mono_ns,
            "direction": direction,
            "kind": kind,
            "rpc_id": rpc_id,
            "method": method,
            "is_error": is_error,
            "bytes": len(line),
            "tool_name": tool_name,
            "ref_id": ref_id,
            "proxy_ns": proxy_ns,
        }
        if self.capture_payloads:
            rec["payload"], rec["payload_truncated"] = self._payload(kind, msg, line)
        return rec

    def _payload(self, kind: str, msg: Any, line: bytes) -> tuple[str | None, bool]:
        if kind == "parse_error":
            return _truncate(line.decode("utf-8", "replace"), self.payload_cap)
        for key in ("params", "result", "error"):
            if key in msg:
                return _truncate(json.dumps(msg[key], ensure_ascii=False, separators=(",", ":")), self.payload_cap)
        return None, False

    @staticmethod
    def _encode(rec: dict[str, Any]) -> bytes:
        return json.dumps(rec, separators=(",", ":")).encode() + b"\n"

    def _write(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            view = view[os.write(self._fd, view) :]

    def _run(self) -> None:
        while True:
            stopping = self._stop.wait(FLUSH_INTERVAL_S)
            self._flush()
            if stopping:
                return

    def _flush(self) -> None:
        batch = []
        with suppress(IndexError):
            while True:
                batch.append(self._pending.popleft())
        if not batch:
            return
        try:
            self._write(b"".join(batch))
        except OSError as e:
            self.write_errors += 1
            self.first_error = self.first_error or e

    def close(self) -> bool:
        """Flush every queued record and close the file. Idempotent. Returns False
        if the writer thread did not finish within CLOSE_TIMEOUT_S."""
        if self._closed:
            return True
        self._closed = True
        self._stop.set()
        self._thread.join(CLOSE_TIMEOUT_S)
        flushed = not self._thread.is_alive()
        if flushed:
            with suppress(OSError):
                os.close(self._fd)
        return flushed
