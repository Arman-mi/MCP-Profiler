"""Offline analysis of recorded traces.

  mcp-profiler analyze [--json] [--min-chain N] <trace.jsonl> [more.jsonl...]

Records from all files are merged and sorted by t_mono_ns. CLOCK_MONOTONIC is
shared by every process on one boot, so runs recorded on the same machine line
up; runs from different boots or machines do not, and are flagged. Wall clock is
counted only while at least one run is alive (first to last record), so the dead
time between unrelated sessions in one file is not reported as idle.

Correlation turns messages into spans. A request is keyed by (run_id, direction,
rpc_id): ids are unique only per direction per session. Its response travels the
other way. Duplicate open ids are matched earliest first. A notifications/cancelled
closes the request it names (status "cancelled"); a response that still arrives
for it is attached as a late response. A request still open at the end of its run
is "orphaned" and runs to that run's last record.

All times are as seen at the proxy: a span runs from reading the request to
reading the response.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
from typing import Any

# Runs whose wall-minus-monotonic offsets differ by more than this were not
# recorded on the same boot, so their monotonic times can't be compared.
CLOCK_MISMATCH_NS = 1_000_000_000

OPPOSITE = {"c2s": "s2c", "s2c": "c2s"}
COMPLETED = ("ok", "error")


@dataclass
class Span:
    run_id: str
    server: str
    direction: str  # of the request: "c2s" for client calls, "s2c" for server-initiated
    rpc_id: Any
    method: str
    tool_name: str | None
    start_ns: int
    end_ns: int  # response read, cancellation, or (orphaned) last record of the run
    status: str  # "ok" | "error" | "cancelled" | "orphaned"
    inflight_at_start: int  # other open requests, same run and direction
    request_bytes: int
    response_bytes: int | None = None
    # When the response had been forwarded on (end_ns + its proxy_ns).
    forwarded_ns: int | None = None
    late_response: bool = False  # a response arrived after the cancellation

    @property
    def duration_ns(self) -> int:
        return self.end_ns - self.start_ns

    @property
    def label(self) -> str:
        if self.tool_name is not None:
            return self.tool_name
        return self.method if self.direction == "c2s" else f"(s2c) {self.method}"


@dataclass
class Gap:
    """From a response leaving the proxy toward the client to the client's next
    request on the same run. With nothing in flight it approximates think time."""

    run_id: str
    server: str
    start_ns: int
    end_ns: int
    inflight: int  # c2s requests open throughout the gap
    next_label: str

    @property
    def duration_ns(self) -> int:
        return self.end_ns - self.start_ns


@dataclass
class Correlation:
    spans: list[Span] = field(default_factory=list)
    gaps: list[Gap] = field(default_factory=list)
    anomalies: Counter[str] = field(default_factory=Counter)


@dataclass
class Trace:
    headers: list[dict[str, Any]]
    records: list[dict[str, Any]]
    malformed_lines: int = 0


def load(paths: list[str]) -> Trace:
    headers: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    malformed = 0
    for path in paths:
        with open(path, "rb") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    malformed += 1  # e.g. a last line cut short by SIGKILL
                    continue
                if not isinstance(rec, dict):
                    malformed += 1
                elif rec.get("kind") == "header":
                    headers.append(rec)
                elif isinstance(rec.get("t_mono_ns"), int) and rec.get("direction") in OPPOSITE:
                    records.append(rec)
                else:
                    malformed += 1
    records.sort(key=lambda r: (r["t_mono_ns"], r.get("seq", 0)))
    return Trace(headers, records, malformed)


def _cancelled_id(rec: dict[str, Any]) -> Any:
    if rec.get("ref_id") is not None:
        return rec["ref_id"]
    # Traces recorded before ref_id existed: fall back to a captured payload.
    try:
        params = json.loads(rec.get("payload") or "")
    except ValueError:
        return None
    return params.get("requestId") if isinstance(params, dict) else None


def correlate(records: list[dict[str, Any]]) -> Correlation:
    """Turn time-ordered records into spans and client gaps."""
    out = Correlation()
    open_: defaultdict[tuple, deque[Span]] = defaultdict(deque)
    cancelled: defaultdict[tuple, deque[Span]] = defaultdict(deque)
    inflight: Counter[tuple[str, str]] = Counter()  # (run_id, direction) -> open requests
    last_t: dict[str, int] = {}
    # Per run: when the latest s2c response was forwarded, if it is more recent
    # than the latest c2s request. The next c2s request closes a gap from there.
    gap_from: dict[str, int | None] = {}

    def close(span: Span) -> None:
        inflight[span.run_id, span.direction] -= 1

    for rec in records:
        run, d, kind = rec.get("run_id", ""), rec["direction"], rec.get("kind")
        t = rec["t_mono_ns"]
        last_t[run] = t
        server = rec.get("server") or "?"
        if kind == "request":
            key = (run, d, rec.get("rpc_id"))
            if open_[key]:
                out.anomalies["duplicate_open_id"] += 1
            span = Span(
                run_id=run, server=server, direction=d, rpc_id=rec.get("rpc_id"),
                method=rec.get("method") or "?", tool_name=rec.get("tool_name"),
                start_ns=t, end_ns=t, status="orphaned",
                inflight_at_start=inflight[run, d], request_bytes=rec.get("bytes", 0),
            )  # fmt: skip
            open_[key].append(span)
            inflight[run, d] += 1
            out.spans.append(span)
            if d == "c2s":
                start = gap_from.get(run)
                if start is not None:
                    out.gaps.append(Gap(run, server, start, max(t, start), inflight[run, d] - 1, span.label))
                gap_from[run] = None
        elif kind == "response":
            key = (run, OPPOSITE[d], rec.get("rpc_id"))
            if open_[key]:
                span = open_[key].popleft()
                span.status = "error" if rec.get("is_error") else "ok"
                span.end_ns = t
                span.forwarded_ns = t + rec.get("proxy_ns", 0)
                span.response_bytes = rec.get("bytes", 0)
                close(span)
                if d == "s2c":
                    gap_from[run] = span.forwarded_ns
            elif cancelled[key]:
                span = cancelled[key].popleft()
                span.late_response = True
                span.response_bytes = rec.get("bytes", 0)
            else:
                out.anomalies["unmatched_response"] += 1
        elif kind == "notification" and rec.get("method") == "notifications/cancelled":
            ref = _cancelled_id(rec)
            key = (run, d, ref)  # sent by the requester, in the request's direction
            if ref is None:
                out.anomalies["cancel_without_request_id"] += 1
            elif open_[key]:
                span = open_[key].popleft()
                span.status = "cancelled"
                span.end_ns = t
                close(span)
                cancelled[key].append(span)
            else:
                out.anomalies["cancel_for_closed_request"] += 1  # benign race with the response
        elif kind == "parse_error":
            out.anomalies["parse_error"] += 1

    for spans in open_.values():
        for span in spans:
            span.end_ns = last_t[span.run_id]
    out.anomalies["orphaned"] = sum(s.status == "orphaned" for s in out.spans)
    out.anomalies = Counter({k: v for k, v in out.anomalies.items() if v})
    return out


def percentile(values: list[int], p: float) -> float | None:
    """Linear interpolation between closest ranks, like numpy's default."""
    if not values:
        return None
    xs = sorted(values)
    k = (len(xs) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _union_ns(intervals: list[tuple[int, int]]) -> int:
    total = 0
    cur_start = cur_end = None
    for a, b in sorted(intervals):
        if cur_end is None or a > cur_end:
            if cur_end is not None:
                total += cur_end - cur_start
            cur_start, cur_end = a, b
        else:
            cur_end = max(cur_end, b)
    if cur_end is not None:
        total += cur_end - cur_start
    return total


def _sweep(spans: list[Span], total_ns: int) -> tuple[dict[int, int], dict[int, int]]:
    """Over c2s spans of all runs: time at each in-flight level (level 0 is the
    rest of `total_ns`), and the number of other requests in flight when each
    span started (by index into `spans`)."""
    events = []
    for i, s in enumerate(spans):
        events.append((s.start_ns, 1, i))
        events.append((s.end_ns, 0, i))  # at equal times, ends sort before starts
    events.sort()
    level_ns: Counter[int] = Counter()
    at_start: dict[int, int] = {}
    level, prev = 0, None
    for t, is_start, i in events:
        if level:
            level_ns[level] += t - prev
        prev = t
        if is_start:
            at_start[i] = level
            level += 1
        else:
            level -= 1
    level_ns[0] = total_ns - sum(level_ns.values())
    return dict(sorted(level_ns.items())), at_start


def build_report(trace: Trace, corr: Correlation, min_chain: int = 2) -> dict[str, Any]:
    recs = trace.records
    alive: dict[str, list[int]] = {}  # run_id -> [first, last] record time
    for r in recs:
        alive.setdefault(r.get("run_id", ""), [r["t_mono_ns"], 0])[1] = r["t_mono_ns"]
    total = _union_ns([(a, b) for a, b in alive.values()])
    calls = [s for s in corr.spans if s.direction == "c2s"]

    def frac(ns: float) -> float:
        return ns / total if total else 0.0

    # 1. Wall clock.
    busy = _union_ns([(s.start_ns, s.end_ns) for s in calls])
    by_server: defaultdict[str, list[tuple[int, int]]] = defaultdict(list)
    for s in calls:
        by_server[s.server].append((s.start_ns, s.end_ns))
    idle_gaps = [g for g in corr.gaps if g.inflight == 0]
    wall = {
        "total_ns": total,
        "busy_ns": busy,
        "busy_fraction": frac(busy),
        "idle_ns": total - busy,
        "idle_fraction": frac(total - busy),
        "client_gaps": len(corr.gaps),
        "client_gap_idle_ns": sum(g.duration_ns for g in idle_gaps),
        "client_gap_overlapped_ns": sum(g.duration_ns for g in corr.gaps if g.inflight > 0),
        "servers": {
            name: {"waiting_ns": (w := _union_ns(iv)), "waiting_fraction": frac(w)}
            for name, iv in sorted(by_server.items())
        },
    }

    # 2. Per tool (tool name for tools/call, method otherwise).
    groups: defaultdict[tuple[str, str], list[Span]] = defaultdict(list)
    for s in corr.spans:
        groups[s.server, s.label].append(s)
    tools = []
    for (server, label), spans in sorted(groups.items(), key=lambda kv: -sum(s.duration_ns for s in kv[1])):
        done = [s.duration_ns for s in spans if s.status in COMPLETED]
        status = Counter(s.status for s in spans)
        tools.append({
            "server": server, "name": label, "count": len(spans),
            **{k: status[k] for k in ("ok", "error", "cancelled", "orphaned")},
            "p50_ns": percentile(done, 50), "p95_ns": percentile(done, 95), "max_ns": max(done, default=None),
            "total_ns": sum(s.duration_ns for s in spans),
            "error_rate": status["error"] / len(done) if done else None,
        })  # fmt: skip

    # 3. Concurrency, across all servers.
    level_ns, at_start = _sweep(calls, total)
    tool_idx = [i for i, s in enumerate(calls) if s.method == "tools/call"]
    overlapped = sum(at_start[i] > 0 for i in tool_idx)
    concurrency = {
        "max_inflight": max([0, *(n + 1 for n in at_start.values())]),
        "time_at_level_ns": level_ns,
        "tool_calls": len(tool_idx),
        "tool_calls_overlapped": overlapped,
        "overlapped_fraction": overlapped / len(tool_idx) if tool_idx else None,
    }

    # 4. Sequential chains: consecutive tool calls, each issued with nothing in
    # flight, on runs that were both alive in between (so two unrelated sessions
    # in one file never chain together).
    def linked(a: Span, b: Span) -> bool:
        return alive[b.run_id][0] <= a.end_ns and alive[a.run_id][1] >= b.start_ns

    chains = []
    run: list[int] = []
    for i in [*tool_idx, None]:
        if i is not None and run and at_start[i] == 0 and linked(calls[run[-1]], calls[i]):
            run.append(i)
            continue
        if len(run) >= min_chain:
            chains.append(_chain([calls[j] for j in run]))
        run = [i] if i is not None else []
    chains.sort(key=lambda c: -c["potential_saving_ns"])

    # 5. Instrument overhead.
    anomalies = dict(corr.anomalies)
    if trace.malformed_lines:
        anomalies["malformed_lines"] = trace.malformed_lines

    proxy = [r.get("proxy_ns", 0) for r in recs]
    overhead = {
        "messages": len(proxy),
        "proxy_ns_total": sum(proxy),
        "fraction_of_wall": frac(sum(proxy)),
        "proxy_ns_p50": percentile(proxy, 50),
        "proxy_ns_p99": percentile(proxy, 99),
    }

    return {
        "runs": _runs(trace),
        "messages": len(recs),
        "spans": len(corr.spans),
        "wall_clock": wall,
        "tools": tools,
        "concurrency": concurrency,
        "sequential_chains": chains,
        "overhead": overhead,
        "anomalies": anomalies,
        "warnings": _clock_warnings(trace),
    }


def _chain(spans: list[Span]) -> dict[str, Any]:
    gaps = sum(b.start_ns - (a.forwarded_ns or a.end_ns) for a, b in zip(spans, spans[1:]))
    wall = spans[-1].end_ns - spans[0].start_ns
    return {
        "calls": len(spans),
        "start_ns": spans[0].start_ns,
        "wall_ns": wall,
        "calls_ns": sum(s.duration_ns for s in spans),
        "gap_ns": gaps,
        # If the calls were independent and issued together, the chain would take
        # as long as its slowest call.
        "potential_saving_ns": wall - max(s.duration_ns for s in spans),
        "servers": sorted({s.server for s in spans}),
        "tools": [s.tool_name for s in spans],
    }


def _runs(trace: Trace) -> list[dict[str, Any]]:
    counts = Counter(r.get("run_id") for r in trace.records)
    seen = {h["run_id"]: h for h in trace.headers}
    runs = [
        {"run_id": rid, "server": h.get("server"), "started": h.get("t_wall_iso"), "messages": counts[rid]}
        for rid, h in seen.items()
    ]
    for rid, n in counts.items():
        if rid not in seen:  # header missing, e.g. the file was cut at the front
            runs.append({"run_id": rid, "server": None, "started": None, "messages": n})
    return runs


def _clock_warnings(trace: Trace) -> list[str]:
    offsets = [h["t_wall_ns"] - h["t_mono_ns"] for h in trace.headers if "t_wall_ns" in h and "t_mono_ns" in h]
    if offsets and max(offsets) - min(offsets) > CLOCK_MISMATCH_NS:
        return ["runs were recorded on different boots or machines; their monotonic times are not comparable"]
    return []


# ---- human-readable output ----


def _ms(ns: float | None) -> str:
    return "-" if ns is None else f"{ns / 1e6:.1f}"


def _dur(ns: float) -> str:
    return f"{ns / 1e9:.3f} s" if ns >= 1e9 else f"{ns / 1e6:.1f} ms"


def _pct(x: float | None) -> str:
    return "-" if x is None else f"{100 * x:.1f}%"


def format_report(rep: dict[str, Any]) -> str:
    out: list[str] = []
    w = out.append
    w(f"{len(rep['runs'])} run(s), {rep['messages']} messages, {rep['spans']} spans")
    for r in rep["runs"]:
        w(f"  run {r['run_id'][:8]}  {r['server'] or '?':<20} {r['messages']:>6} msgs  started {r['started'] or '?'}")
    for msg in rep["warnings"]:
        w(f"  WARNING: {msg}")

    wc = rep["wall_clock"]
    w("\n1. Wall clock")
    w(f"  total                       {_dur(wc['total_ns']):>12}")
    w(f"  >=1 request in flight       {_dur(wc['busy_ns']):>12}  {_pct(wc['busy_fraction']):>6}")
    w(f"  idle, nothing in flight     {_dur(wc['idle_ns']):>12}  {_pct(wc['idle_fraction']):>6}")
    w(f"  client gaps: {wc['client_gaps']}, idle {_dur(wc['client_gap_idle_ns'])} (think time), "
      f"overlapping other requests {_dur(wc['client_gap_overlapped_ns'])}")  # fmt: skip
    w("  waiting on each server:")
    for name, s in wc["servers"].items():
        w(f"    {name:<25} {_dur(s['waiting_ns']):>12}  {_pct(s['waiting_fraction']):>6}")

    w("\n2. Per tool (durations in ms, completed calls only)")
    w(f"  {'server':<16} {'tool / method':<26} {'n':>5} {'err':>4} {'canc':>4} {'orph':>4}"
      f" {'p50':>8} {'p95':>8} {'max':>8} {'total':>9} {'err%':>6}")  # fmt: skip
    for t in rep["tools"]:
        w(f"  {t['server'][:16]:<16} {t['name'][:26]:<26} {t['count']:>5} {t['error']:>4} {t['cancelled']:>4}"
          f" {t['orphaned']:>4} {_ms(t['p50_ns']):>8} {_ms(t['p95_ns']):>8} {_ms(t['max_ns']):>8}"
          f" {_ms(t['total_ns']):>9} {_pct(t['error_rate']):>6}")  # fmt: skip

    c = rep["concurrency"]
    w("\n3. Concurrency (client requests, all servers)")
    w(f"  max in flight: {c['max_inflight']}")
    if c["tool_calls"]:
        w(f"  tool calls issued while another request was in flight: "
          f"{c['tool_calls_overlapped']}/{c['tool_calls']} ({_pct(c['overlapped_fraction'])})")  # fmt: skip
    w("  time at each in-flight level:")
    total = rep["wall_clock"]["total_ns"] or 1
    for label, ns in _buckets(c["time_at_level_ns"]):
        w(f"    {label:>7}  {_dur(ns):>12}  {_pct(ns / total):>6}  {'#' * round(40 * ns / total)}")

    chains = rep["sequential_chains"]
    w(f"\n4. Sequential chains ({len(chains)} found): tool calls each issued only after the previous response")
    if not chains:
        w("  none")
    for i, ch in enumerate(chains[:10], 1):
        tools = Counter(ch["tools"])
        names = ", ".join(f"{n} x{k}" if k > 1 else str(n) for n, k in tools.most_common())
        w(f"  #{i} {ch['calls']} calls on {', '.join(ch['servers'])}: wall {_dur(ch['wall_ns'])}, "
          f"calls {_dur(ch['calls_ns'])}, gaps {_dur(ch['gap_ns'])}")  # fmt: skip
        w(f"     up to {_dur(ch['potential_saving_ns'])} saved if independent and issued together  [{names}]")
    if len(chains) > 10:
        w(f"  ... {len(chains) - 10} more (see --json)")

    o = rep["overhead"]
    w("\n5. Instrument overhead")
    w(f"  proxy_ns total {_dur(o['proxy_ns_total'])} over {o['messages']} messages"
      f" = {_pct(o['fraction_of_wall'])} of wall clock  (per message p50 {_us(o['proxy_ns_p50'])}, p99 {_us(o['proxy_ns_p99'])})")  # fmt: skip
    w("  in-process time only: the extra pipe hop and event-loop wakeup are not included (see README)")

    if rep["anomalies"]:
        w("\nAnomalies: " + ", ".join(f"{k}={v}" for k, v in sorted(rep["anomalies"].items())))
    return "\n".join(out) + "\n"


def _buckets(level_ns: dict[int, int]) -> list[tuple[str, int]]:
    """Levels 0-2 one by one, then power-of-two ranges: 3-4, 5-8, 9-16, ..."""
    out: dict[str, int] = {}
    for level, ns in level_ns.items():
        if level <= 2:
            label = str(level)
        else:
            hi = 1 << (level - 1).bit_length()
            label = f"{hi // 2 + 1}-{hi}"
        out[label] = out.get(label, 0) + ns
    return list(out.items())


def _us(ns: float | None) -> str:
    return "-" if ns is None else f"{ns / 1e3:.1f} us"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mcp-profiler analyze",
        description="Correlate traces into spans and report where wall-clock time went.",
    )
    parser.add_argument("traces", nargs="+", metavar="TRACE", help="trace .jsonl files, merged by monotonic time")
    parser.add_argument("--json", action="store_true", help="machine-readable report, including every span and gap")
    parser.add_argument(
        "--min-chain", type=int, default=2, metavar="N",
        help="shortest sequential chain to report (default: %(default)s)",
    )  # fmt: skip
    return parser


def main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    try:
        trace = load(args.traces)
    except OSError as e:
        print(f"mcp-profiler analyze: {e}", file=sys.stderr)
        return 1
    corr = correlate(trace.records)
    report = build_report(trace, corr, max(args.min_chain, 2))
    if args.json:
        report["span_list"] = [{**asdict(s), "duration_ns": s.duration_ns} for s in corr.spans]
        report["gap_list"] = [{**asdict(g), "duration_ns": g.duration_ns} for g in corr.gaps]
        json.dump(report, sys.stdout, indent=1)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(format_report(report))
    return 0
