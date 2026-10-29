"""End-to-end tests of the passthrough proxy, run as a real subprocess."""

import os
import signal
import subprocess
import sys
import time

import pytest

from mcp_profiler.proxy import LINE_LIMIT

PROXY = [sys.executable, "-m", "mcp_profiler.cli"]


def run_proxy(server, stdin_bytes=b"", log=None, timeout=30, trace=os.devnull):
    argv = [*PROXY, *(["--log", str(log)] if log else []), "--trace", str(trace), "--", *server]
    return subprocess.run(argv, input=stdin_bytes, capture_output=True, timeout=timeout)


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A zombie is dead for our purposes.
    with open(f"/proc/{pid}/stat") as f:
        return f.read().split(") ", 1)[1][0] != "Z"


def test_forwards_bytes_unchanged(tmp_path):
    payload = b"".join(
        [
            b'{"z":1,"a":2.50,"id":7,"jsonrpc":"2.0"}\n',
            b'{"t":"\\u00e9\\ud83d\\ude00 caf\xc3\xa9","n":1e3}\n',
            b"not json\r\n",
            b"\n",
            b'{"big":"' + b"x" * (LINE_LIMIT + 12345) + b'"}\n',
            b'{"tail":"no trailing newline"}',
        ]
    )
    proc = run_proxy(["cat"], payload, log=tmp_path / "p.log")
    assert proc.returncode == 0
    assert proc.stdout == payload


def test_server_stderr_goes_to_log_never_stdout(tmp_path):
    log = tmp_path / "p.log"
    server = ["sh", "-c", 'echo "noise on stderr" >&2; printf partial >&2; cat']
    proc = run_proxy(server, b'{"id":1}\n', log=log)
    assert proc.stdout == b'{"id":1}\n'
    assert proc.stderr == b""
    text = log.read_text()
    assert "[server] noise on stderr\n" in text
    assert "[server] partial\n" in text


def test_without_log_server_stderr_goes_to_our_stderr():
    proc = run_proxy(["sh", "-c", "echo oops >&2"])
    assert proc.stdout == b""
    assert b"[server] oops" in proc.stderr


@pytest.mark.parametrize("code", [0, 3, 42])
def test_exit_code_propagates(code):
    assert run_proxy(["sh", "-c", f"exit {code}"]).returncode == code


def test_death_by_signal_propagates():
    assert run_proxy(["sh", "-c", "kill -TERM $$"]).returncode == -signal.SIGTERM


def test_missing_command_exits_127(tmp_path):
    log = tmp_path / "p.log"
    assert run_proxy(["/nonexistent/server"], log=log).returncode == 127
    assert "failed to start" in log.read_text()


def _start_proxy_with_idle_client(server, log, ready="started pid", trace=os.devnull):
    """Start the proxy with a stdin pipe we keep open, like an idle client."""
    r, w = os.pipe()
    proc = subprocess.Popen(
        [*PROXY, "--log", str(log), "--trace", str(trace), "--", *server],
        stdin=r,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    os.close(r)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if log.exists() and ready in log.read_text():
            break
        time.sleep(0.02)
    child_pid = int(log.read_text().split("started pid ", 1)[1].split(":", 1)[0])
    return proc, w, child_pid


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
def test_signal_is_forwarded_and_leaves_no_orphan(tmp_path, sig):
    log = tmp_path / "p.log"
    proc, w, child = _start_proxy_with_idle_client(["sleep", "60"], log)
    try:
        proc.send_signal(sig)
        proc.wait(timeout=10)
        assert proc.returncode == -sig
        assert not alive(child)
        assert f"received {sig.name}" in log.read_text()
    finally:
        os.close(w)


def test_ctrl_c_to_process_group_leaves_no_orphan(tmp_path):
    log = tmp_path / "p.log"
    proc, w, child = _start_proxy_with_idle_client(["sleep", "60"], log)
    try:
        os.killpg(proc.pid, signal.SIGINT)  # what a terminal does on Ctrl-C
        proc.wait(timeout=10)
        assert not alive(child)
        # The child is in its own group, so it is signalled once, by the proxy.
        assert log.read_text().count("received SIGINT") == 1
    finally:
        os.close(w)


def test_stubborn_server_is_killed(tmp_path):
    log = tmp_path / "p.log"
    stubborn = [
        sys.executable,
        "-c",
        "import signal, sys, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "print('ready', file=sys.stderr, flush=True); time.sleep(60)",
    ]
    proc, w, child = _start_proxy_with_idle_client(stubborn, log, ready="[server] ready")
    try:
        start = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
        assert time.monotonic() - start < 5
        assert not alive(child)
        assert "sending SIGKILL" in log.read_text()
    finally:
        os.close(w)


def test_client_eof_closes_server_stdin(tmp_path):
    log = tmp_path / "p.log"
    proc = run_proxy(["cat"], b"x\n", log=log)
    assert proc.returncode == 0
    assert "client stdin EOF" in log.read_text()
