"""Correlator and report tests on hand-written traces, plus the CLI end to end.

Times in the fixtures are in milliseconds for readability; `rec` converts.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from mcp_profiler.analyze import Trace, build_report, correlate, format_report, load, percentile

ROOT = Path(__file__).resolve().parent.parent
PROXY = [sys.executable, "-m", "mcp_profiler.cli"]
MS = 1_000_000


def rec(t_ms, direction, kind, rpc_id=None, method=None, *, run="r1", server="srv", tool=None,
        is_error=None, ref=None, nbytes=100, proxy_ns=1000):  # fmt: skip
    return {
        "run_id": run, "server": server, "seq": 0, "t_mono_ns": int(t_ms * MS), "direction": direction,
        "kind": kind, "rpc_id": rpc_id, "method": method,
        "is_error": (bool(is_error) if kind == "response" else None), "bytes": nbytes,
        "tool_name": tool, "ref_id": ref, "proxy_ns": proxy_ns,
    }  # fmt: skip


def call(t_ms, rpc_id, tool, **kw):
    return rec(t_ms, "c2s", "request", rpc_id, "tools/call", tool=tool, **kw)


def resp(t_ms, rpc_id, direction="s2c", **kw):
    return rec(t_ms, direction, "response", rpc_id, **kw)


def spans_by_id(records):
    return {(s.direction, s.rpc_id): s for s in correlate(sorted(records, key=lambda r: r["t_mono_ns"])).spans}


def report(records, headers=()):
    records = sorted(records, key=lambda r: r["t_mono_ns"])
    trace = Trace(list(headers), records)
    return build_report(trace, correlate(records))


def test_interleaved_concurrent_requests():
    spans = spans_by_id([
        call(0, 1, "a", nbytes=50),
        call(10, 2, "b"),
        call(15, 3, "c"),
        resp(20, 2, nbytes=700),
        resp(30, 1, is_error=True),
        resp(40, 3),
    ])  # fmt: skip
    a, b, c = spans["c2s", 1], spans["c2s", 2], spans["c2s", 3]
    assert (a.duration_ns, b.duration_ns, c.duration_ns) == (30 * MS, 10 * MS, 25 * MS)
    assert (a.status, b.status, c.status) == ("error", "ok", "ok")
    assert (a.inflight_at_start, b.inflight_at_start, c.inflight_at_start) == (0, 1, 2)
    assert (a.request_bytes, b.response_bytes) == (50, 700)
    assert b.forwarded_ns == 20 * MS + 1000


def test_cancelled_request_and_late_response():
    records = [
        call(0, 1, "slow"),
        rec(50, "c2s", "notification", method="notifications/cancelled", ref=1),
        resp(60, 1),  # the server answered anyway: attached, not unmatched
        call(70, 2, "fast"),
        resp(75, 2),
    ]
    corr = correlate(records)
    slow, fast = corr.spans
    assert (slow.status, slow.duration_ns, slow.late_response, slow.response_bytes) == ("cancelled", 50 * MS, True, 100)
    assert fast.status == "ok" and fast.inflight_at_start == 0
    assert not corr.anomalies


def test_cancel_falls_back_to_captured_payload():
    cancel = rec(5, "c2s", "notification", method="notifications/cancelled")
    del cancel["ref_id"]  # trace from before ref_id existed
    cancel["payload"] = '{"requestId":"x","reason":"timeout"}'
    (span,) = correlate([call(0, "x", "t"), cancel]).spans
    assert span.status == "cancelled"


def test_cancel_edge_cases_are_anomalies():
    corr = correlate([
        call(0, 1, "t"),
        resp(5, 1),
        rec(6, "c2s", "notification", method="notifications/cancelled", ref=1),  # lost the race
        rec(7, "c2s", "notification", method="notifications/cancelled"),  # no id recorded
    ])  # fmt: skip
    assert corr.spans[0].status == "ok"
    assert corr.anomalies == {"cancel_for_closed_request": 1, "cancel_without_request_id": 1}


def test_orphaned_request_runs_to_end_of_its_run():
    corr = correlate([
        call(0, 1, "hangs"),
        call(10, 2, "ok"),
        resp(20, 2),
        rec(90, "c2s", "notification", method="notifications/initialized"),
        rec(500, "c2s", "notification", method="x", run="other"),  # another run ends later
    ])  # fmt: skip
    hangs = corr.spans[0]
    assert hangs.status == "orphaned" and hangs.end_ns == 90 * MS
    assert hangs.response_bytes is None
    assert corr.anomalies["orphaned"] == 1


def test_server_initiated_request():
    # During a tool call the server asks the client to sample an LLM.
    corr = correlate([
        call(0, 1, "agentic"),
        rec(10, "s2c", "request", 0, "sampling/createMessage"),
        resp(400, 0, direction="c2s"),
        resp(410, 1),
    ])  # fmt: skip
    tool, sampling = corr.spans
    assert (sampling.direction, sampling.method, sampling.status) == ("s2c", "sampling/createMessage", "ok")
    assert sampling.duration_ns == 390 * MS
    assert sampling.label == "(s2c) sampling/createMessage"
    assert tool.duration_ns == 410 * MS
    assert not corr.anomalies
    # The client's response is not a client request: no gap, no extra in-flight call.
    assert corr.gaps == []


def test_duplicate_ids_across_directions():
    # Both sides use id 1 at the same time; each response closes its own direction.
    spans = spans_by_id([
        call(0, 1, "t"),
        rec(5, "s2c", "request", 1, "roots/list"),
        resp(8, 1, direction="c2s"),
        resp(30, 1),
    ])  # fmt: skip
    assert spans["c2s", 1].end_ns == 30 * MS and spans["c2s", 1].tool_name == "t"
    assert spans["s2c", 1].end_ns == 8 * MS and spans["s2c", 1].method == "roots/list"


def test_duplicate_ids_same_direction_match_earliest_first():
    corr = correlate([call(0, 7, "first"), call(1, 7, "second"), resp(10, 7), resp(20, 7)])
    first, second = corr.spans
    assert (first.end_ns, second.end_ns) == (10 * MS, 20 * MS)
    assert corr.anomalies == {"duplicate_open_id": 1}


def test_same_ids_in_different_runs_are_separate():
    spans = correlate([
        call(0, 1, "a", run="r1"),
        call(1, 1, "b", run="r2", server="other"),
        resp(5, 1, run="r2", server="other"),
        resp(9, 1, run="r1"),
    ]).spans  # fmt: skip
    assert {s.tool_name: s.duration_ns for s in spans} == {"a": 9 * MS, "b": 4 * MS}
    assert spans[1].inflight_at_start == 0  # in-flight counts are per run


def test_unmatched_response_and_parse_error():
    corr = correlate([resp(1, 99), rec(2, "s2c", "parse_error")])
    assert corr.spans == [] and corr.anomalies == {"unmatched_response": 1, "parse_error": 1}


def test_client_gaps():
    corr = correlate([
        call(0, 1, "a"),
        resp(10, 1, proxy_ns=2 * MS),
        rec(11, "c2s", "notification", method="notifications/progress"),  # not a request
        call(50, 2, "b"),  # idle gap: 12 ms (forwarded) -> 50 ms
        call(55, 3, "c"),
        resp(60, 2),
        call(70, 4, "d"),  # gap while c is still in flight
        resp(80, 3),
        resp(90, 4),
    ])  # fmt: skip
    idle, overlapped = corr.gaps
    assert (idle.start_ns, idle.end_ns, idle.inflight, idle.next_label) == (12 * MS, 50 * MS, 0, "b")
    assert (overlapped.duration_ns, overlapped.inflight) == (10 * MS - 1000, 1)


def test_report_wall_clock_concurrency_and_chains():
    rep = report([
        rec(0, "c2s", "request", 0, "initialize"),
        resp(5, 0),
        # Sequential chain of three calls with 10 ms client gaps.
        call(10, 1, "read"), resp(20, 1, proxy_ns=0),
        call(30, 2, "read"), resp(40, 2, proxy_ns=0),
        call(50, 3, "grep"), resp(80, 3, proxy_ns=0),
        # 20 ms of think time, then two calls in parallel.
        call(100, 4, "read"), call(100, 5, "read"),
        resp(110, 4), resp(120, 5),
    ])  # fmt: skip
    wc = rep["wall_clock"]
    assert wc["total_ns"] == 120 * MS
    assert wc["busy_ns"] == (5 + 10 + 10 + 30 + 20) * MS
    assert wc["idle_ns"] == 45 * MS
    assert wc["servers"]["srv"]["waiting_ns"] == wc["busy_ns"]

    read = next(t for t in rep["tools"] if t["name"] == "read")
    assert (read["count"], read["max_ns"], read["error_rate"]) == (4, 20 * MS, 0.0)

    c = rep["concurrency"]
    assert c["max_inflight"] == 2
    assert (c["tool_calls"], c["tool_calls_overlapped"]) == (5, 1)
    assert c["time_at_level_ns"] == {0: 45 * MS, 1: 65 * MS, 2: 10 * MS}

    # initialize is not a tool call; the parallel pair breaks the chain after it starts.
    (chain,) = rep["sequential_chains"]
    assert chain["tools"] == ["read", "read", "grep", "read"]
    assert chain["gap_ns"] == (10 + 10 + 20) * MS
    assert chain["calls_ns"] == (10 + 10 + 30 + 10) * MS
    assert chain["potential_saving_ns"] == chain["wall_ns"] - 30 * MS

    text = format_report(rep)
    for heading in ("1. Wall clock", "2. Per tool", "3. Concurrency", "4. Sequential chains", "5. Instrument"):
        assert heading in text


def test_chains_do_not_span_unrelated_sessions():
    # Two sessions one after the other in the same file; dead time between them.
    rep = report([
        call(0, 1, "a", run="r1"), resp(10, 1, run="r1"),
        call(20, 2, "a", run="r1"), resp(30, 2, run="r1"),
        call(5000, 1, "a", run="r2"), resp(5010, 1, run="r2"),
        call(5020, 2, "a", run="r2"), resp(5030, 2, run="r2"),
    ])  # fmt: skip
    assert [c["calls"] for c in rep["sequential_chains"]] == [2, 2]
    assert rep["wall_clock"]["total_ns"] == 60 * MS  # dead time between runs excluded


def test_clock_mismatch_warning():
    headers = [
        {"kind": "header", "run_id": "r1", "t_wall_ns": 10**18, "t_mono_ns": 0},
        {"kind": "header", "run_id": "r2", "t_wall_ns": 10**18 + 5 * 10**9, "t_mono_ns": 0},
    ]
    assert report([call(0, 1, "a"), resp(1, 1)], headers)["warnings"]


def test_percentile():
    assert percentile([], 50) is None
    assert percentile([5], 95) == 5
    assert percentile([1, 2, 3, 4], 50) == 2.5
    assert percentile(list(range(101)), 95) == 95


def test_load_merges_files_and_skips_malformed_lines(tmp_path):
    a, b = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    a.write_text("\n".join(json.dumps(r) for r in [{"kind": "header", "run_id": "r1"}, call(0, 1, "x"), resp(30, 1)]))
    b.write_text(json.dumps(call(10, 1, "y", run="r2")) + "\n" + '{"truncated by SIGKI\n')
    trace = load([str(a), str(b)])
    assert [r["t_mono_ns"] for r in trace.records] == [0, 10 * MS, 30 * MS]
    assert trace.malformed_lines == 1 and len(trace.headers) == 1


def test_cli_on_real_proxy_trace(tmp_path):
    trace = tmp_path / "t.jsonl"
    driver = [sys.executable, str(ROOT / "fixtures" / "driver.py")]
    server = [sys.executable, str(ROOT / "fixtures" / "echo_server.py")]
    subprocess.run(
        [*driver, "--calls", "fast", "fast", "slow:0.1", "--",
         *PROXY, "--log", str(tmp_path / "p.log"), "--trace", str(trace), "--", *server],
        check=True, capture_output=True, timeout=60,
    )  # fmt: skip
    text = subprocess.run([*PROXY, "analyze", str(trace)], check=True, capture_output=True, text=True).stdout
    assert "fast_tool" in text and "slow_tool" in text and "3 calls" in text

    rep = json.loads(subprocess.run([*PROXY, "analyze", "--json", str(trace)], check=True, capture_output=True).stdout)
    assert rep["concurrency"]["max_inflight"] == 1
    (chain,) = rep["sequential_chains"]
    assert chain["tools"] == ["fast_tool", "fast_tool", "slow_tool"]
    assert all(s["status"] == "ok" for s in rep["span_list"])
    assert rep["anomalies"] == {}


@pytest.mark.parametrize("argv", [["analyze"], ["analyze", "/nonexistent.jsonl"]])
def test_cli_errors(argv):
    proc = subprocess.run([*PROXY, *argv], capture_output=True, text=True)
    assert proc.returncode != 0 and proc.stdout == ""
