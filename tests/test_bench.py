"""Load generator: classification on synthetic levels, and the CLI against the fixture server."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from mcp_profiler.bench import Level, classify

ROOT = Path(__file__).resolve().parent.parent
CLI = [sys.executable, "-m", "mcp_profiler.cli"]
SERVER = [sys.executable, str(ROOT / "fixtures" / "echo_server.py")]
MS = 1_000_000


def levels(*rows):
    """rows: (concurrency, p50_ms, calls_per_s)"""
    out = [
        Level(concurrency=c, calls=16, rounds=1, wall_ns=0, p50_ns=p50 * MS, p95_ns=p50 * MS, max_ns=int(p50 * MS),
              throughput=tput, errors=0, first_error=None)
        for c, p50, tput in rows
    ]  # fmt: skip
    for lv in out:
        lv.speedup = lv.throughput / out[0].throughput
    return out


def test_classify_serialized():
    cl = classify(levels((1, 200, 5), (4, 800, 5), (16, 3200, 5.1)))
    assert cl["verdict"] == "serialized"
    assert cl["p50_growth"] == 16
    assert "200.0 -> 3200.0 ms" in cl["evidence"][1]


def test_classify_concurrent():
    cl = classify(levels((1, 200, 5), (4, 205, 19), (16, 215, 70)))
    assert cl["verdict"] == "concurrent"
    assert cl["scaling_efficiency"] == pytest.approx(70 / 80)


def test_classify_partial_reports_peak_parallelism():
    # Throughput peaks at C=4 and falls off: effective parallelism is the peak.
    cl = classify(levels((1, 200, 5), (4, 230, 17), (16, 900, 15)))
    assert cl["verdict"] == "partially serialized"
    assert cl["effective_parallelism"] == pytest.approx(3.4)
    assert cl["peak_concurrency"] == 4
    assert "about 3.4" in cl["summary"]


def test_classify_relative_to_lowest_level():
    # Starting at C=2, ideal scaling to C=16 is 8x, not 16x.
    cl = classify(levels((2, 200, 10), (16, 210, 75)))
    assert cl["verdict"] == "concurrent"


def test_classify_notes_fast_calls_and_errors():
    lv = levels((1, 0.5, 2000), (16, 6, 2600))
    lv[1].errors, lv[1].first_error = 3, "JSON-RPC error: boom"
    cl = classify(lv)
    assert cl["verdict"] == "serialized"
    assert any("per-message overhead" in n for n in cl["notes"])
    assert any("3 calls failed" in n and "boom" in n for n in cl["notes"])


def test_classify_needs_two_levels():
    assert classify(levels((1, 200, 5)))["verdict"] == "unknown"


def bench(*args, server=SERVER):
    return subprocess.run([*CLI, "bench", *args, "--", *server], capture_output=True, text=True, timeout=60)


SLOW = ["--tool", "slow_tool", "--tool-args", '{"seconds": 0.1}', "--concurrency", "1,4", "--calls-per-level", "4"]


@pytest.mark.parametrize(("flags", "verdict"), [(["--blocking"], "serialized"), ([], "concurrent")])
def test_fixture_server_classification(flags, verdict):
    r = bench(*SLOW, "--json", server=[*SERVER, *flags])
    assert r.returncode == 0, r.stderr
    rep = json.loads(r.stdout)
    assert rep["classification"]["verdict"] == verdict
    assert rep["server"] == "echo-server"
    assert [(lv["concurrency"], lv["calls"], lv["errors"]) for lv in rep["levels"]] == [(1, 4, 0), (4, 4, 0)]
    # Warmup rounds are not measured, and every call slept 0.1 s.
    assert all(lv["p50_ns"] >= 100 * MS for lv in rep["levels"])


def test_through_proxy_with_trace(tmp_path):
    trace = tmp_path / "t.jsonl"
    proxy = [*CLI, "--log", str(tmp_path / "p.log"), "--trace", str(trace), "--", *SERVER]
    r = bench(*SLOW, "--warmup", "0", server=proxy)
    assert r.returncode == 0, r.stderr
    assert "Verdict: CONCURRENT." in r.stdout
    assert "p50 ms" in r.stdout
    # Probe + 4 calls at C=1 + 4 calls at C=4, all recorded by the proxy.
    recs = [json.loads(line) for line in trace.read_text().splitlines()]
    assert sum(r.get("tool_name") == "slow_tool" for r in recs) == 9


def test_rounds_are_whole():
    r = bench("--tool", "fast_tool", "--concurrency", "1,3", "--calls-per-level", "4", "--json")
    assert r.returncode == 0, r.stderr
    assert [(lv["calls"], lv["rounds"]) for lv in json.loads(r.stdout)["levels"]] == [(4, 4), (6, 2)]


def test_unknown_tool_lists_tools():
    r = bench("--tool", "nope")
    assert r.returncode == 1
    assert "has no tool 'nope'; it has: echo, fast_tool, slow_tool" in r.stderr
    assert r.stdout == ""


def test_bad_tool_args_fail_on_probe():
    r = bench("--tool", "slow_tool", "--tool-args", '{"x": 1}')
    assert r.returncode == 1
    assert "probe call to slow_tool failed: tool returned isError" in r.stderr


def test_server_that_exits():
    r = bench("--tool", "x", server=[sys.executable, "-c", "pass"])
    assert r.returncode == 1
    assert "server closed its stdout" in r.stderr


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--tool", "x", "--tool-args", "[1]", "--", "true"], "must be a JSON object"),
        (["--tool", "x", "--concurrency", "0,2", "--", "true"], "integers >= 1"),
        (["--tool", "x", "--calls-per-level", "0", "--", "true"], "--calls-per-level must be >= 1"),
        (["--tool", "x"], "missing server command"),
    ],
)
def test_cli_errors(args, message):
    r = subprocess.run([*CLI, "bench", *args], capture_output=True, text=True, timeout=30)
    assert r.returncode == 2
    assert message in r.stderr
