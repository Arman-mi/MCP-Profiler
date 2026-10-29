"""Minimal MCP client that drives a stdio server and prints per-call latency.

Usage:
  python fixtures/driver.py [--concurrent] [--calls SPEC ...] -- <command> [args...]

Call specs:
  fast            fast_tool()
  slow:SECONDS    slow_tool(seconds=SECONDS)
  echo:TEXT       echo(text=TEXT)

The server's stderr is passed through to this script's stderr.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from typing import Any

from mcp import Client, StdioServerParameters

DEFAULT_CALLS = ["fast", "echo:hello", "slow:0.5", "slow:0.5", "fast"]


def parse_call(spec: str) -> tuple[str, dict[str, Any]]:
    kind, _, arg = spec.partition(":")
    if kind == "fast":
        return "fast_tool", {}
    if kind == "slow":
        return "slow_tool", {"seconds": float(arg)}
    if kind == "echo":
        return "echo", {"text": arg}
    raise argparse.ArgumentTypeError(f"unknown call spec: {spec!r}")


def describe(name: str, args: dict[str, Any]) -> str:
    inner = ", ".join(f"{k}={v!r}" for k, v in args.items())
    return f"{name}({inner})"


def result_text(result: Any) -> str:
    parts = [getattr(block, "text", f"<{type(block).__name__}>") for block in result.content]
    text = " ".join(parts)
    return f"ERROR {text}" if result.is_error else text


async def timed_call(client: Client, idx: int, name: str, args: dict[str, Any]) -> tuple[int, str, float, str]:
    start = time.perf_counter()
    result = await client.call_tool(name, args)
    elapsed_ms = (time.perf_counter() - start) * 1000
    return idx, describe(name, args), elapsed_ms, result_text(result)


async def run(command: list[str], calls: list[tuple[str, dict[str, Any]]], concurrent: bool) -> None:
    params = StdioServerParameters(command=command[0], args=command[1:], env=dict(os.environ))

    t0 = time.perf_counter()
    # mode="legacy" forces the classic initialize handshake.
    async with Client(params, mode="legacy") as client:
        init_ms = (time.perf_counter() - t0) * 1000
        info = client.server_info
        print(f"server: {info.name if info else '?'}  protocol: {client.protocol_version}")
        print(f"initialize: {init_ms:8.1f} ms")

        t = time.perf_counter()
        tools = await client.list_tools()
        print(f"tools/list: {(time.perf_counter() - t) * 1000:8.1f} ms  -> {', '.join(sorted(x.name for x in tools.tools))}")

        print(f"calls ({'concurrent' if concurrent else 'sequential'}):")
        t = time.perf_counter()
        if concurrent:
            rows = await asyncio.gather(*(timed_call(client, i, n, a) for i, (n, a) in enumerate(calls, 1)))
        else:
            rows = [await timed_call(client, i, n, a) for i, (n, a) in enumerate(calls, 1)]
        total_ms = (time.perf_counter() - t) * 1000

        for idx, desc, ms, text in rows:
            print(f"  #{idx:<2} {desc:<28} {ms:8.1f} ms  -> {text!r}")
        print(f"total calls: {total_ms:8.1f} ms")


def main() -> None:
    argv = sys.argv[1:]
    if "--" not in argv:
        sys.exit("usage: driver.py [--concurrent] [--calls SPEC ...] -- <command> [args...]")
    split = argv.index("--")
    own, command = argv[:split], argv[split + 1 :]
    if not command:
        sys.exit("driver.py: missing server command after --")

    parser = argparse.ArgumentParser(description="Drive a stdio MCP server and time tool calls.")
    parser.add_argument("--concurrent", action="store_true", help="issue all calls at once via asyncio.gather")
    parser.add_argument("--calls", nargs="+", type=parse_call, metavar="SPEC", help="fast | slow:SECONDS | echo:TEXT")
    args = parser.parse_args(own)
    calls = args.calls or [parse_call(s) for s in DEFAULT_CALLS]

    try:
        asyncio.run(run(command, calls, args.concurrent))
    except KeyboardInterrupt:
        print("driver: interrupted", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
