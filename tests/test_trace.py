"""Trace recording: classification, record schema, and end-to-end traces."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from mcp_profiler import __version__
from mcp_profiler.proxy import LINE_LIMIT
from mcp_profiler.trace import MESSAGE_FIELDS, TraceWriter, classify

ROOT = Path(__file__).resolve().parent.parent
PROXY = [sys.executable, "-m", "mcp_profiler.cli"]
DRIVER = [sys.executable, str(ROOT / "fixtures" / "driver.py")]
SERVER = [sys.executable, str(ROOT / "fixtures" / "echo_server.py")]


def read_trace(path):
    header, *records = [json.loads(line) for line in Path(path).read_bytes().splitlines()]
    return header, sorted(records, key=lambda r: r["seq"])


def run_traced(tmp_path, server, stdin_bytes=b"", *opts):
    trace = tmp_path / "t.jsonl"
    argv = [*PROXY, "--log", str(tmp_path / "p.log"), "--trace", str(trace), *opts, "--", *server]
    proc = subprocess.run(argv, input=stdin_bytes, capture_output=True, timeout=60)
    return proc, trace


@pytest.mark.parametrize(
    "msg, expected",
    [
        ({"jsonrpc": "2.0", "id": 1, "method": "initialize"}, ("request", 1, "initialize", None)),
        ({"jsonrpc": "2.0", "id": "x", "method": "ping", "params": {}}, ("request", "x", "ping", None)),
        ({"jsonrpc": "2.0", "method": "notifications/cancelled"}, ("notification", None, "notifications/cancelled", None)),  # noqa: E501
        ({"jsonrpc": "2.0", "id": 1, "result": {}}, ("response", 1, None, False)),
        ({"jsonrpc": "2.0", "id": 1, "error": {"code": -1}}, ("response", 1, None, True)),
        ({"jsonrpc": "2.0", "id": None, "error": {"code": -32700}}, ("response", None, None, True)),
    ],
)
def test_classify_valid(msg, expected):
    assert classify(msg) == expected


@pytest.mark.parametrize(
    "msg",
    [
        [{"jsonrpc": "2.0", "id": 1, "method": "ping"}],  # batch
        "string",
        {"id": 1, "method": "ping"},  # no jsonrpc
        {"jsonrpc": "1.0", "id": 1, "method": "ping"},
        {"jsonrpc": "2.0", "id": True, "method": "ping"},
        {"jsonrpc": "2.0", "id": 1.5, "method": "ping"},
        {"jsonrpc": "2.0", "id": None, "method": "ping"},
        {"jsonrpc": "2.0", "id": 1, "method": 5},
        {"jsonrpc": "2.0", "id": 1},  # neither result nor error
        {"jsonrpc": "2.0", "id": 1, "result": {}, "error": {}},
        {"jsonrpc": "2.0", "result": {}},  # response without id
        {"jsonrpc": "2.0", "id": None, "result": {}},
    ],
)
def test_classify_rejects(msg):
    assert classify(msg) is None


def make_writer(tmp_path, **kw):
    return TraceWriter(str(tmp_path / "t.jsonl"), run_id="r", server="s", argv=["p"], command=["c"], **kw)


def test_writer_record_schema_and_parse_errors(tmp_path):
    w = make_writer(tmp_path)
    lines = [
        b'{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"echo","arguments":{}}}\n',
        b'{"jsonrpc":"2.0","id":7,"result":{"content":[]}}\n',
        b"not json\n",
        b"\xff\xfe\n",  # invalid UTF-8
        b"[" * 100_000 + b"\n",  # nesting deep enough to hit RecursionError
        b"\n",
        b'{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":7,"reason":"x"}}\n',
    ]
    for i, line in enumerate(lines):
        w.record("c2s", line, w.next_seq(), 1000 + i, 5)
    assert w.close()
    header, records = read_trace(tmp_path / "t.jsonl")
    assert header["kind"] == "header" and header["version"] == __version__
    assert all(tuple(r) == MESSAGE_FIELDS for r in records)  # field order, no payload keys
    req, resp, *errors, cancel = records
    assert (req["kind"], req["rpc_id"], req["method"], req["tool_name"]) == ("request", 7, "tools/call", "echo")
    assert (cancel["kind"], cancel["rpc_id"], cancel["ref_id"]) == ("notification", None, 7)
    assert req["ref_id"] is None
    assert (resp["kind"], resp["is_error"], resp["method"]) == ("response", False, None)
    assert [r["kind"] for r in errors] == ["parse_error"] * 4
    assert [r["bytes"] for r in records] == [len(line) for line in lines]
    assert [r["seq"] for r in records] == list(range(len(lines)))
    assert w.records == len(lines) and w.record_errors == 0


def test_writer_payload_capture_truncates(tmp_path):
    w = make_writer(tmp_path, capture_payloads=True, payload_cap=16)
    w.record("s2c", b'{"jsonrpc":"2.0","id":1,"result":{"text":"\xc3\xa9\xc3\xa9\xc3\xa9\xc3\xa9\xc3\xa9"}}\n', 0, 0, 0)
    w.record("c2s", b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n', 1, 0, 0)
    w.record("c2s", b"garbage\n", 2, 0, 0)
    w.close()
    _, (resp, note, bad) = read_trace(tmp_path / "t.jsonl")
    # The 16-byte cap falls inside the 4th e-acute; the partial character is dropped.
    assert resp["payload"] == '{"text":"ééé' and resp["payload_truncated"] is True
    assert len(resp["payload"].encode()) <= 16
    assert note["payload"] is None and note["payload_truncated"] is False
    assert bad["payload"] == "garbage\n" and bad["payload_truncated"] is False


def test_end_to_end_trace_and_bytes_unchanged(tmp_path):
    payload = b"".join(
        [
            b'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"z":1,"a":1e3}}\n',
            b'{"jsonrpc":"2.0","method":"notifications/initialized"}\r\n',
            b"not json\n",
            b'{"jsonrpc":"2.0","id":2,"method":"x","params":{"big":"' + b"x" * (LINE_LIMIT + 10) + b'"}}\n',
            b'{"jsonrpc":"2.0","id":3,"method":"tail"}',  # no trailing newline
        ]
    )
    proc, trace = run_traced(tmp_path, ["cat"], payload, "--name", "loop")
    assert proc.returncode == 0
    assert proc.stdout == payload
    header, records = read_trace(trace)
    assert header["server"] == "loop" and header["command"] == ["cat"]
    assert header["run_id"] == records[0]["run_id"]
    for direction in ("c2s", "s2c"):
        rs = [r for r in records if r["direction"] == direction]
        assert [r["kind"] for r in rs] == ["request", "notification", "parse_error", "request", "request"]
        # The over-long line was forwarded in pieces but recorded as one message.
        assert sum(r["bytes"] for r in rs) == len(payload)
        assert rs[3]["method"] == "x" and rs[3]["bytes"] > LINE_LIMIT
        assert all(r["proxy_ns"] > 0 for r in rs)
    assert all(a["t_mono_ns"] <= b["t_mono_ns"] for a, b in zip(records, records[1:]))
    assert all(r["t_mono_ns"] >= header["t_mono_ns"] for r in records)
    assert "trace: 10 messages recorded" in (tmp_path / "p.log").read_text()


def test_default_trace_path_and_name(tmp_path):
    argv = [*PROXY, "--log", os.devnull, "--", "/bin/cat"]
    proc = subprocess.run(argv, input=b"x\n", capture_output=True, cwd=tmp_path, timeout=30)
    assert proc.returncode == 0
    (path,) = (tmp_path / "traces").iterdir()
    header, records = read_trace(path)
    assert path.name == f"{header['run_id']}.jsonl"
    assert header["server"] == "cat"
    assert len(records) == 2


def test_trace_parent_dirs_are_created(tmp_path):
    trace = tmp_path / "a" / "b" / "t.jsonl"
    proc = subprocess.run([*PROXY, "--trace", str(trace), "--", "true"], capture_output=True, timeout=30)
    assert proc.returncode == 0
    assert read_trace(trace)[0]["kind"] == "header"


def test_unwritable_trace_fails_before_starting_server(tmp_path):
    marker = tmp_path / "started"
    blocked = tmp_path / "blocked"
    blocked.write_text("")  # a file where a directory is needed
    proc = subprocess.run(
        [*PROXY, "--trace", str(blocked / "t.jsonl"), "--", "touch", str(marker)],
        capture_output=True,
        timeout=30,
    )
    assert proc.returncode == 2
    assert b"cannot open trace file" in proc.stderr
    assert proc.stdout == b""
    assert not marker.exists()


def test_trace_flushed_on_sigterm(tmp_path):
    trace = tmp_path / "t.jsonl"
    log = tmp_path / "p.log"
    r, w = os.pipe()
    proc = subprocess.Popen(
        [*PROXY, "--log", str(log), "--trace", str(trace), "--", "cat"],
        stdin=r,
        stdout=subprocess.PIPE,
        start_new_session=True,
    )
    os.close(r)
    try:
        n = 50
        os.write(w, b"".join(b'{"jsonrpc":"2.0","method":"n%d"}\n' % i for i in range(n)))
        got = b""
        while got.count(b"\n") < n:
            got += proc.stdout.read1(65536)
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
    finally:
        os.close(w)
    _, records = read_trace(trace)
    assert [r["method"] for r in records if r["direction"] == "c2s"] == [f"n{i}" for i in range(n)]
    assert len(records) == 2 * n


def test_driver_trace_correlates(tmp_path):
    trace = tmp_path / "t.jsonl"
    for mode in ([], ["--concurrent"]):
        proc = subprocess.run(
            [*DRIVER, *mode, "--calls", "fast", "slow:0.2", "echo:hi", "slow:0.1", "--",
             *PROXY, "--log", str(tmp_path / "p.log"), "--trace", str(trace), "--", *SERVER],
            capture_output=True,
            text=True,
            timeout=60,
        )  # fmt: skip
        assert proc.returncode == 0, proc.stderr
    lines = [json.loads(line) for line in trace.read_bytes().splitlines()]
    headers = [r for r in lines if r["kind"] == "header"]
    assert len(headers) == 2  # two runs appended to one file
    for header in headers:
        records = sorted((r for r in lines if r.get("seq") is not None and r["run_id"] == header["run_id"]),
                         key=lambda r: r["seq"])  # fmt: skip
        assert records[0]["method"] == "initialize" and records[0]["direction"] == "c2s"
        assert not [r for r in records if r["kind"] == "parse_error"]
        assert any(r["method"] == "notifications/initialized" for r in records)
        # Ids are unique per direction: a c2s request is answered by an s2c response.
        for req_dir, resp_dir in (("c2s", "s2c"), ("s2c", "c2s")):
            reqs = {r["rpc_id"]: r for r in records if r["kind"] == "request" and r["direction"] == req_dir}
            resps = {r["rpc_id"]: r for r in records if r["kind"] == "response" and r["direction"] == resp_dir}
            assert reqs.keys() == resps.keys()
            assert all(reqs[i]["seq"] < resps[i]["seq"] for i in reqs)
        calls = [r["tool_name"] for r in records if r["method"] == "tools/call"]
        assert sorted(calls) == ["echo", "fast_tool", "slow_tool", "slow_tool"]
