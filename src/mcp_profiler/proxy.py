"""Transparent stdio proxy between an MCP client and a child MCP server.

Three concurrent pumps:
  our stdin    -> child stdin   (traced as c2s)
  child stdout -> our stdout    (traced as s2c)
  child stderr -> log (tagged, via a writer thread so the event loop never blocks)

Bytes are forwarded exactly as read, one line at a time, and every write is
drained to the kernel before the next read. Each line is recorded only after it
has been forwarded, so parsing never delays it. Nothing here writes to stdout
except the stdout pump (see CLAUDE.md).
"""

from __future__ import annotations

import asyncio
import os
import queue
import select
import signal
import stat
import sys
import threading
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress

from mcp_profiler.trace import TraceWriter

# Lines up to this size are forwarded as one write. Longer lines are not an
# error: they are forwarded in chunks of about this size as they arrive.
LINE_LIMIT = 16 * 1024 * 1024

# After forwarding SIGINT/SIGTERM, how long the child gets before SIGKILL.
SIGNAL_GRACE_S = 2.0

# After the child exits, how long to wait for its pipes to finish draining.
DRAIN_TIMEOUT_S = 1.0

SERVER_TAG = b"[server] "
PROXY_TAG = b"[mcp-profiler] "


def _write_all(fd: int, data: bytes) -> None:
    """Blocking write that also copes with fds another component made non-blocking."""
    view = memoryview(data)
    while view:
        try:
            n = os.write(fd, view)
        except BlockingIOError:
            select.select([], [fd], [])
            continue
        view = view[n:]


class LogSink:
    """Line-oriented log writer running on its own thread.

    Callers on the event loop only enqueue; file I/O happens on the thread, so a
    slow disk or a stalled stderr reader can never delay protocol traffic.
    """

    def __init__(self, path: str | None) -> None:
        if path is None:
            self._fd = sys.stderr.fileno()
            self._owns_fd = False
        else:
            self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            self._owns_fd = True
        self._queue: queue.SimpleQueue[bytes | None] = queue.SimpleQueue()
        self._thread = threading.Thread(target=self._run, name="mcp-profiler-log", daemon=True)
        self._thread.start()

    def write(self, data: bytes) -> None:
        self._queue.put(data)

    def diag(self, msg: str) -> None:
        self._queue.put(PROXY_TAG + msg.encode("utf-8", "replace") + b"\n")

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                return
            with suppress(OSError):
                _write_all(self._fd, item)

    def close(self, timeout: float = 2.0) -> None:
        self._queue.put(None)
        self._thread.join(timeout)
        if self._owns_fd:
            with suppress(OSError):
                os.close(self._fd)


def _pollable(fd: int) -> bool:
    """Whether asyncio's pipe transports can drive `fd` (epoll rejects regular
    files and devices such as /dev/null)."""
    mode = os.fstat(fd).st_mode
    return stat.S_ISFIFO(mode) or stat.S_ISSOCK(mode) or os.isatty(fd)


class _FdWriter:
    """Writer for a stdout that is not pollable (regular file, /dev/null)."""

    def __init__(self, fd: int) -> None:
        self._fd = fd

    def write(self, data: bytes) -> None:
        _write_all(self._fd, data)

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        pass


async def _open_stdin(loop: asyncio.AbstractEventLoop) -> asyncio.StreamReader:
    reader = asyncio.StreamReader(limit=LINE_LIMIT)
    if not _pollable(0):
        # e.g. `< input.jsonl` or `< /dev/null`: feed the reader from a thread.
        def feed() -> None:
            with suppress(RuntimeError):  # loop already closed at shutdown
                while chunk := os.read(0, 65536):
                    loop.call_soon_threadsafe(reader.feed_data, chunk)
                loop.call_soon_threadsafe(reader.feed_eof)

        threading.Thread(target=feed, name="mcp-profiler-stdin", daemon=True).start()
    else:
        pipe = os.fdopen(0, "rb", buffering=0, closefd=False)
        await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), pipe)
    return reader


async def _open_stdout(loop: asyncio.AbstractEventLoop) -> asyncio.StreamWriter | _FdWriter:
    if not _pollable(1):
        return _FdWriter(1)
    pipe = os.fdopen(1, "wb", buffering=0, closefd=False)
    transport, protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, pipe)
    # high=0: drain() returns only once every byte has been handed to the kernel.
    transport.set_write_buffer_limits(high=0)
    return asyncio.StreamWriter(transport, protocol, None, loop)


async def _read_chunk(reader: asyncio.StreamReader) -> tuple[bytes, bool]:
    """Return (chunk, eof). A chunk is one full line, or a piece of an over-long line."""
    try:
        return await reader.readuntil(b"\n"), False
    except asyncio.IncompleteReadError as e:
        return e.partial, True
    except asyncio.LimitOverrunError as e:
        return await reader.readexactly(max(e.consumed, 1)), False


async def _pump(
    reader: asyncio.StreamReader,
    write: Callable[[bytes], Awaitable[None]],
    trace: TraceWriter | None,
    direction: str,
) -> None:
    """Forward chunks as they arrive; record one trace event per complete line.

    An over-long line is forwarded piece by piece. Its pieces are also kept, so
    the trace can parse it whole once its final piece has been forwarded; its
    timestamp is that of the final piece and its proxy_ns the sum over pieces.
    """
    pieces: list[bytes] = []
    proxy_ns = 0
    while True:
        chunk, eof = await _read_chunk(reader)
        t_read = time.monotonic_ns()
        complete = chunk.endswith(b"\n") or (eof and bool(chunk or pieces))
        if trace is not None and complete:
            seq = trace.next_seq()  # taken with t_read, so seq order is time order
        if chunk:
            await write(chunk)
            proxy_ns += time.monotonic_ns() - t_read
        if trace is not None:
            if complete:
                line = b"".join((*pieces, chunk)) if pieces else chunk
                pieces.clear()
                trace.record(direction, line, seq, t_read, proxy_ns)
                proxy_ns = 0
            elif chunk:
                pieces.append(chunk)
        if eof:
            return


def _exit_status(returncode: int) -> int:
    return 128 - returncode if returncode < 0 else returncode


async def run(command: list[str], log_path: str | None = None, trace: TraceWriter | None = None) -> int:
    """Run `command` behind the proxy. Returns the child's raw returncode
    (negative for death by signal), or 126/127 if it could not be started.
    `trace` is flushed and closed before returning, however the run ends."""
    log = LogSink(log_path)
    saved_blocking = {fd: os.get_blocking(fd) for fd in (0, 1)}
    if trace is not None:
        log.diag(f"tracing to {trace.path} (run {trace.run_id})")
    try:
        return await _run(command, log, trace)
    finally:
        for fd, blocking in saved_blocking.items():
            with suppress(OSError):
                os.set_blocking(fd, blocking)
        if trace is not None:
            _close_trace(trace, log)
        log.close()


def _close_trace(trace: TraceWriter, log: LogSink) -> None:
    if not trace.close():
        log.diag(f"trace: writer did not finish flushing {trace.path}; records may be missing")
    if trace.record_errors:
        log.diag(f"trace: {trace.record_errors} messages could not be recorded, first: {trace.first_error!r}")
    if trace.write_errors:
        log.diag(f"trace: {trace.write_errors} failed writes to {trace.path}, first: {trace.first_error!r}")
    log.diag(f"trace: {trace.records} messages recorded to {trace.path}")


async def _run(command: list[str], log: LogSink, trace: TraceWriter | None) -> int:
    loop = asyncio.get_running_loop()
    proc: asyncio.subprocess.Process | None = None
    early_signal: signal.Signals | None = None
    signals_seen = 0

    def signal_child(sig: signal.Signals) -> None:
        # The child leads its own group (pgid == pid); signal the whole group,
        # as a client tearing down the server directly would.
        assert proc is not None
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(proc.pid, sig)

    def kill_child(reason: str) -> None:
        if proc is not None and proc.returncode is None:
            log.diag(f"{reason}; sending SIGKILL to process group {proc.pid}")
            signal_child(signal.SIGKILL)

    def on_signal(sig: signal.Signals) -> None:
        nonlocal signals_seen, early_signal
        if proc is None:
            early_signal = sig  # arrived while spawning; forwarded right after
            return
        signals_seen += 1
        if proc.returncode is not None:
            return
        if signals_seen > 1:
            kill_child(f"received {sig.name} again")
            return
        log.diag(f"received {sig.name}; forwarding to process group {proc.pid}")
        signal_child(sig)
        loop.call_later(SIGNAL_GRACE_S, kill_child, f"server still alive {SIGNAL_GRACE_S}s after {sig.name}")

    # Installed before spawning: the child gets no terminal signals of its own
    # (see start_new_session below), so we must never miss one.
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, on_signal, sig)
    try:
        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=LINE_LIMIT,
                # Own session and process group: terminal Ctrl-C and a client's
                # killpg() reach only us, and we forward exactly once. Sharing our
                # group would deliver every group-wide signal to the server twice.
                start_new_session=True,
            )
        except OSError as e:
            log.diag(f"failed to start {command[0]!r}: {e}")
            return 126 if isinstance(e, PermissionError) else 127
        log.diag(f"started pid {proc.pid}: {command!r}")
        if early_signal is not None:
            on_signal(early_signal)
        return await _serve(proc, log, loop, trace)
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        kill_child("proxy shutting down with server still running")


async def _serve(
    proc: asyncio.subprocess.Process,
    log: LogSink,
    loop: asyncio.AbstractEventLoop,
    trace: TraceWriter | None,
) -> int:
    """Pump traffic until the child exits; return its returncode."""
    assert proc.stdin and proc.stdout and proc.stderr
    child_in = proc.stdin
    child_out = proc.stdout
    child_err = proc.stderr
    child_in.transport.set_write_buffer_limits(high=0)  # type: ignore[attr-defined]
    client_gone = False
    tasks: list[asyncio.Task[None]] = []

    def close_child_stdin() -> None:
        with suppress(OSError):
            child_in.close()

    async def to_child(data: bytes) -> None:
        child_in.write(data)
        await child_in.drain()

    async def to_client(data: bytes) -> None:
        nonlocal client_gone
        if client_gone:
            return  # keep draining the child so it never blocks on a full pipe
        try:
            client_out.write(data)
            await client_out.drain()
        except (BrokenPipeError, ConnectionResetError):
            client_gone = True
            log.diag("client closed our stdout; closing server stdin")
            upstream.cancel()
            close_child_stdin()

    async def upstream_pump() -> None:
        try:
            await _pump(client_in, to_child, trace, "c2s")
            log.diag("client stdin EOF; closing server stdin")
        except (BrokenPipeError, ConnectionResetError):
            log.diag("server stdin closed; no longer forwarding client input")
        finally:
            close_child_stdin()

    async def stderr_pump() -> None:
        at_line_start = True
        while True:
            chunk, eof = await _read_chunk(child_err)
            if chunk:
                log.write(SERVER_TAG + chunk if at_line_start else chunk)
                at_line_start = chunk.endswith(b"\n")
            if eof:
                if not at_line_start:
                    log.write(b"\n")
                return

    client_out = await _open_stdout(loop)
    try:
        # stderr first: it needs no client stream, and must drain from the start.
        errstream = asyncio.create_task(stderr_pump(), name="child-stderr->log")
        tasks.append(errstream)
        client_in = await _open_stdin(loop)
        upstream = asyncio.create_task(upstream_pump(), name="stdin->child")
        downstream = asyncio.create_task(_pump(child_out, to_client, trace, "s2c"), name="child->stdout")
        tasks += [upstream, downstream]

        returncode = await proc.wait()
        log.diag(f"server exited with status {_exit_status(returncode)}")
        # The child is gone; let already-written output drain, then stop.
        _, pending = await asyncio.wait({downstream, errstream}, timeout=DRAIN_TIMEOUT_S)
        if pending:
            log.diag("server pipes still open after exit (inherited by a grandchild?); closing")
        return returncode
    finally:
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for task, result in zip(tasks, results):
            if isinstance(result, Exception):
                log.diag(f"{task.get_name()} failed: {result!r}")
        with suppress(OSError):
            client_out.close()
