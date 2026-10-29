"""Load generator: does an MCP server serialize concurrent requests?

  mcp-profiler bench --tool NAME [options] -- <server command...>

Starts the server and connects to it as a minimal MCP client over stdio. For each
concurrency level C it issues C identical tools/call requests at once, waits for
all of them (one round), and repeats. The first --warmup rounds of a level are
discarded; then it runs ceil(calls / C) measured rounds. Rounds are whole, so a
level may measure a few more calls than --calls-per-level.

A call's latency runs from just before its request is written to the server's
stdin until its response line is read. A level's throughput is its measured calls
over the summed wall time of its measured rounds.

The client speaks just enough MCP for this (initialize, tools/list, tools/call,
answering pings) with the stdlib, so the package keeps no runtime dependencies.
The server command can itself be `mcp-profiler [opts] -- <server...>` to record a
trace of the run.

bench is the client here, not a proxy: its report goes to stdout. The server's
stderr is passed through to ours.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import shlex
import signal
import sys
import time
from contextlib import suppress
from dataclasses import asdict, dataclass
from typing import Any

from mcp_profiler import __version__
from mcp_profiler.analyze import percentile
from mcp_profiler.proxy import LINE_LIMIT

DEFAULT_LEVELS = "1,2,4,8,16"
DEFAULT_CALLS = 32
PROTOCOL_VERSION = "2025-11-25"

# A call with no response after this long ends the run.
CALL_TIMEOUT_S = 60.0

# After closing the server's stdin, how long it gets to exit before each of
# SIGTERM and SIGKILL.
SHUTDOWN_GRACE_S = 2.0

# Classification, on throughput relative to the lowest level. Peak speedup below
# SERIALIZED_MAX_SPEEDUP is flat throughput. Reaching CONCURRENT_MIN_EFFICIENCY of
# the ideal speedup at the top level is linear scaling.
SERIALIZED_MAX_SPEEDUP = 1.5
CONCURRENT_MIN_EFFICIENCY = 0.7

# Below this p50 at the lowest level, the numbers mostly measure per-message
# overhead rather than the tool.
FAST_CALL_NS = 5_000_000


class BenchError(Exception):
    """Ends the run with a message and exit status 1."""


class Client:
    """Minimal MCP client over a child's stdin/stdout.

    One reader task resolves each request's future with (response, time read).
    Server-initiated requests get an answer: we declare no client capabilities,
    so only ping is supported. Notifications are ignored.
    """

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self._proc = proc
        self._next_id = 1
        self._pending: dict[int, asyncio.Future[tuple[dict[str, Any], int]]] = {}
        self._dead: BenchError | None = None
        self._reader = asyncio.create_task(self._read(), name="bench-reader")

    @classmethod
    async def start(cls, command: list[str]) -> Client:
        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                limit=LINE_LIMIT,
                # Own process group, so close() can take down a wrapper's children
                # too (npx, uvx, or the proxy).
                start_new_session=True,
            )
        except OSError as e:
            raise BenchError(f"failed to start {command[0]!r}: {e}") from e
        return cls(proc)

    def _write(self, msg: dict[str, Any]) -> None:
        assert self._proc.stdin
        self._proc.stdin.write(json.dumps(msg, separators=(",", ":")).encode() + b"\n")

    async def request(self, method: str, params: dict[str, Any]) -> tuple[dict[str, Any], int]:
        """Send a request; return (response, latency_ns)."""
        if self._dead:
            raise self._dead
        rid = self._next_id
        self._next_id += 1
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        t0 = time.monotonic_ns()
        try:
            self._write({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
            await self._proc.stdin.drain()  # type: ignore[union-attr]
            resp, t1 = await asyncio.wait_for(fut, CALL_TIMEOUT_S)
        except TimeoutError:
            raise BenchError(f"no response to {method} after {CALL_TIMEOUT_S:g} s") from None
        except (BrokenPipeError, ConnectionResetError):
            raise BenchError("server closed its stdin") from None
        finally:
            self._pending.pop(rid, None)
        return resp, t1 - t0

    async def _read(self) -> None:
        assert self._proc.stdout
        dead = BenchError("server closed its stdout")
        try:
            while line := await self._proc.stdout.readline():
                t = time.monotonic_ns()
                try:
                    msg = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(msg, dict):
                    continue
                if "method" in msg:
                    if "id" in msg:
                        self._answer(msg)
                    continue
                rid = msg.get("id")
                fut = self._pending.pop(rid, None) if isinstance(rid, int) else None
                if fut is not None and not fut.done():
                    fut.set_result((msg, t))
        except ValueError:
            dead = BenchError(f"server sent a line longer than {LINE_LIMIT} bytes")
        finally:
            self._dead = dead
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(dead)

    def _answer(self, msg: dict[str, Any]) -> None:
        # No drain: awaiting here would stop the reader while the server may be
        # blocked writing to us.
        reply: dict[str, Any] = {"jsonrpc": "2.0", "id": msg["id"]}
        if msg["method"] == "ping":
            reply["result"] = {}
        else:
            reply["error"] = {"code": -32601, "message": f"Method not found: {msg['method']}"}
        with suppress(OSError, RuntimeError):
            self._write(reply)

    async def initialize(self) -> dict[str, Any]:
        resp, _ = await self.request("initialize", {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "mcp-profiler-bench", "version": __version__},
        })  # fmt: skip
        if not isinstance(resp.get("result"), dict):
            raise BenchError(f"initialize failed: {_error_text(resp)}")
        self._write({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return resp["result"]

    async def list_tools(self) -> list[str]:
        names: list[str] = []
        params: dict[str, Any] = {}
        while True:
            resp, _ = await self.request("tools/list", params)
            result = resp.get("result")
            if not isinstance(result, dict):
                raise BenchError(f"tools/list failed: {_error_text(resp)}")
            names += [t["name"] for t in result.get("tools", []) if isinstance(t, dict) and "name" in t]
            if not result.get("nextCursor"):
                return names
            params = {"cursor": result["nextCursor"]}

    async def call_tool(self, name: str, args: dict[str, Any]) -> tuple[int, str | None]:
        """Return (latency_ns, error). A JSON-RPC error and a result with
        isError: true both count as errors."""
        resp, ns = await self.request("tools/call", {"name": name, "arguments": args})
        return ns, _call_error(resp)

    async def close(self) -> None:
        proc = self._proc
        with suppress(OSError):
            proc.stdin.close()  # type: ignore[union-attr]
        for sig in (None, signal.SIGTERM, signal.SIGKILL):
            if sig is not None:
                with suppress(ProcessLookupError, PermissionError):
                    os.killpg(proc.pid, sig)
            with suppress(TimeoutError):
                await asyncio.wait_for(proc.wait(), SHUTDOWN_GRACE_S)
                break
        self._reader.cancel()
        with suppress(asyncio.CancelledError):
            await self._reader


def _error_text(resp: dict[str, Any]) -> str:
    err = resp.get("error")
    if isinstance(err, dict):
        return f"{err.get('message')} (code {err.get('code')})"
    return json.dumps(resp)[:500]


def _call_error(resp: dict[str, Any]) -> str | None:
    if "error" in resp:
        return f"JSON-RPC error: {_error_text(resp)}"
    result = resp.get("result")
    if not isinstance(result, dict):
        return f"malformed response: {json.dumps(resp)[:500]}"
    if result.get("isError"):
        content = result.get("content")
        texts = [c.get("text", "") for c in content if isinstance(c, dict)] if isinstance(content, list) else []
        return "tool returned isError: " + (" ".join(texts)[:500] or "(no text)")
    return None


@dataclass
class Level:
    concurrency: int
    calls: int
    rounds: int
    wall_ns: int
    p50_ns: float
    p95_ns: float
    max_ns: int
    throughput: float  # calls per second
    errors: int
    first_error: str | None
    speedup: float = 1.0  # throughput relative to the lowest level


async def run_level(client: Client, tool: str, args: dict[str, Any], c: int, calls: int, warmup: int) -> Level:
    rounds = math.ceil(calls / c)
    latencies: list[int] = []
    wall = errors = 0
    first_error = None
    for r in range(warmup + rounds):
        t0 = time.monotonic_ns()
        outs = await asyncio.gather(*(client.call_tool(tool, args) for _ in range(c)))
        t1 = time.monotonic_ns()
        if r < warmup:
            continue
        wall += t1 - t0
        for ns, err in outs:
            latencies.append(ns)
            if err:
                errors += 1
                first_error = first_error or err
    return Level(
        concurrency=c, calls=len(latencies), rounds=rounds, wall_ns=wall,
        p50_ns=percentile(latencies, 50), p95_ns=percentile(latencies, 95),  # type: ignore[arg-type]
        max_ns=max(latencies), throughput=len(latencies) / (wall / 1e9),
        errors=errors, first_error=first_error,
    )  # fmt: skip


async def bench(
    command: list[str], tool: str, args: dict[str, Any], levels: list[int], calls: int, warmup: int
) -> dict[str, Any]:
    client = await Client.start(command)
    try:
        init = await client.initialize()
        tools = await client.list_tools()
        if tool not in tools:
            raise BenchError(f"server has no tool {tool!r}; it has: {', '.join(sorted(tools)) or '(none)'}")
        # One call up front: bad --tool-args fail here, not 300 times over.
        _, err = await client.call_tool(tool, args)
        if err:
            raise BenchError(f"probe call to {tool} failed: {err}")
        results = []
        for c in levels:
            results.append(await run_level(client, tool, args, c, calls, warmup))
    finally:
        await client.close()

    for lv in results:
        lv.speedup = lv.throughput / results[0].throughput
    info = init.get("serverInfo") if isinstance(init.get("serverInfo"), dict) else {}
    return {
        "command": command,
        "server": info.get("name"),
        "server_version": info.get("version"),
        "protocol_version": init.get("protocolVersion"),
        "tool": tool,
        "tool_args": args,
        "calls_per_level": calls,
        "warmup_rounds": warmup,
        "levels": [asdict(lv) for lv in results],
        "classification": classify(results),
    }


def classify(levels: list[Level]) -> dict[str, Any]:
    """Classify from the lowest level to the highest. `speedup` must be set."""
    if len(levels) < 2:
        return {"verdict": "unknown", "summary": "classifying needs at least two concurrency levels", "evidence": []}
    base, top = levels[0], levels[-1]
    peak = max(levels, key=lambda lv: lv.speedup)
    ideal = top.concurrency / base.concurrency
    efficiency = top.speedup / ideal
    p50_growth = top.p50_ns / base.p50_ns
    evidence = [
        f"concurrency C={base.concurrency} -> C={top.concurrency} ({ideal:g}x)",
        f"p50 latency {_ms(base.p50_ns)} -> {_ms(top.p50_ns)} ms ({p50_growth:.2f}x)",
        f"throughput {base.throughput:.1f} -> {top.throughput:.1f} calls/s "
        f"({top.speedup:.2f}x, {100 * efficiency:.0f}% of the ideal {ideal:g}x)",
    ]
    if peak.speedup < SERIALIZED_MAX_SPEEDUP:
        verdict = "serialized"
        summary = (
            "the server handles one call at a time. Extra concurrent calls only wait in line: "
            "latency grows with C while throughput stays flat."
        )
    elif efficiency >= CONCURRENT_MIN_EFFICIENCY:
        verdict = "concurrent"
        summary = (
            "concurrent calls overlap. Latency stays roughly flat as C rises "
            "and throughput scales with C."
        )
    else:
        verdict = "partially serialized"
        summary = (
            f"some calls overlap, but throughput stops scaling. Effective parallelism is about "
            f"{peak.speedup:.1f}: throughput peaks at C={peak.concurrency}, "
            f"{peak.speedup:.1f}x that at C={base.concurrency}."
        )
    notes = []
    if base.p50_ns < FAST_CALL_NS:
        notes.append(
            f"a call takes only {_ms(base.p50_ns)} ms at C={base.concurrency}, so this mostly measures "
            "per-message overhead (pipes, JSON, the server's event loop) rather than the tool itself"
        )
    errors = sum(lv.errors for lv in levels)
    if errors:
        first = next(lv.first_error for lv in levels if lv.first_error)
        notes.append(f"{errors} calls failed and are included in the latencies; first: {first}")
    return {
        "verdict": verdict,
        "summary": summary,
        "evidence": evidence,
        "notes": notes,
        "effective_parallelism": peak.speedup,
        "peak_concurrency": peak.concurrency,
        "scaling_efficiency": efficiency,
        "p50_growth": p50_growth,
    }


def _ms(ns: float) -> str:
    return f"{ns / 1e6:.1f}"


def format_report(rep: dict[str, Any]) -> str:
    out: list[str] = []
    w = out.append
    w(f"bench {rep['tool']} {json.dumps(rep['tool_args'])}")
    w(f"  command: {shlex.join(rep['command'])}")
    server = " ".join(str(x) for x in (rep["server"] or "?", rep["server_version"]) if x)
    w(f"  server: {server}, protocol {rep['protocol_version']}")
    w(f"  {rep['calls_per_level']} calls per level, first {rep['warmup_rounds']} round(s) of each level discarded")
    w("")
    w(f"  {'C':>4} {'calls':>6} {'p50 ms':>9} {'p95 ms':>9} {'max ms':>9} {'calls/s':>9} {'speedup':>8} {'errors':>7}")
    for lv in rep["levels"]:
        w(f"  {lv['concurrency']:>4} {lv['calls']:>6} {_ms(lv['p50_ns']):>9} {_ms(lv['p95_ns']):>9}"
          f" {_ms(lv['max_ns']):>9} {lv['throughput']:>9.1f} {lv['speedup']:>7.2f}x {lv['errors']:>7}")  # fmt: skip
    cl = rep["classification"]
    w("")
    w(f"Verdict: {cl['verdict'].upper()}. {cl['summary'][0].upper()}{cl['summary'][1:]}")
    for line in cl["evidence"]:
        w(f"  {line}")
    for note in cl.get("notes", []):
        w(f"  note: {note}")
    return "\n".join(out) + "\n"


def _levels(text: str) -> list[int]:
    try:
        levels = sorted({int(x) for x in text.split(",") if x.strip()})
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a comma-separated list of integers: {text!r}") from None
    if not levels or levels[0] < 1:
        raise argparse.ArgumentTypeError("levels must be integers >= 1")
    return levels


def _tool_args(text: str) -> dict[str, Any]:
    try:
        args = json.loads(text)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"invalid JSON: {e}") from None
    if not isinstance(args, dict):
        raise argparse.ArgumentTypeError("must be a JSON object")
    return args


def _nonneg(text: str) -> int:
    n = int(text)
    if n < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return n


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mcp-profiler bench",
        usage="mcp-profiler bench --tool NAME [options] -- <server command...>",
        description="Call one tool at rising concurrency and report whether the server serializes calls.",
    )
    parser.add_argument("--tool", required=True, help="tool to call")
    parser.add_argument("--tool-args", type=_tool_args, default={}, metavar="JSON",
                        help="tool arguments as a JSON object (default: {})")  # fmt: skip
    parser.add_argument("--concurrency", type=_levels, default=_levels(DEFAULT_LEVELS), metavar="LIST",
                        help=f"comma-separated concurrency levels (default: {DEFAULT_LEVELS})")  # fmt: skip
    parser.add_argument("--calls-per-level", type=int, default=DEFAULT_CALLS, metavar="N",
                        help="calls to measure at each level, rounded up to whole rounds (default: %(default)s)")  # fmt: skip
    parser.add_argument("--warmup", type=_nonneg, default=1, metavar="N",
                        help="rounds discarded at the start of each level (default: %(default)s)")  # fmt: skip
    parser.add_argument("--json", action="store_true", help="machine-readable report")
    return parser


def main(argv: list[str], command: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not command:
        parser.error("missing server command after --")
    if args.calls_per_level < 1:
        parser.error("--calls-per-level must be >= 1")
    try:
        rep = asyncio.run(
            bench(command, args.tool, args.tool_args, args.concurrency, args.calls_per_level, args.warmup)
        )
    except BenchError as e:
        print(f"mcp-profiler bench: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    if args.json:
        json.dump(rep, sys.stdout, indent=1)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(format_report(rep))
    return 0
