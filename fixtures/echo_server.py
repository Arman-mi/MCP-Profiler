"""Minimal MCP server over stdio, used as a profiling target.

Tools:
  fast_tool()            returns immediately
  slow_tool(seconds)     sleeps for `seconds`, then returns
  echo(text)             returns `text`

--blocking makes slow_tool call time.sleep() inside the coroutine, which stalls
the server's event loop: concurrent calls then run one after another instead of
overlapping. Without it, slow_tool awaits asyncio.sleep() and concurrent calls
overlap. All tools are `async def` on purpose: the SDK runs sync tools in a
worker thread, which would hide the blocking.

Diagnostics go to stderr only; stdout is the protocol channel.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time

from mcp.server.mcpserver import MCPServer

BLOCKING = False

mcp = MCPServer("echo-server")


def log(msg: str) -> None:
    print(f"echo_server: {msg}", file=sys.stderr, flush=True)


@mcp.tool()
async def fast_tool() -> str:
    """Return immediately."""
    log("fast_tool")
    return "fast"


@mcp.tool()
async def slow_tool(seconds: float) -> str:
    """Sleep for `seconds` before returning."""
    log(f"slow_tool start seconds={seconds} blocking={BLOCKING}")
    if BLOCKING:
        time.sleep(seconds)  # deliberately blocks the event loop
    else:
        await asyncio.sleep(seconds)
    log(f"slow_tool done seconds={seconds}")
    return f"slept {seconds}s"


@mcp.tool()
async def echo(text: str) -> str:
    """Return the input unchanged."""
    log(f"echo len={len(text)}")
    return text


def main() -> None:
    global BLOCKING
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--blocking",
        action="store_true",
        help="slow_tool uses time.sleep() and blocks the event loop",
    )
    args = parser.parse_args()
    BLOCKING = args.blocking
    log(f"starting (blocking={BLOCKING})")
    mcp.run("stdio")


if __name__ == "__main__":
    main()
