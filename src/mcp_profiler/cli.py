"""Entry point: mcp-profiler [--log FILE] -- <command> [args...]

stdout is the MCP protocol channel once the proxy runs, so every diagnostic
in this module goes to stderr.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys

from mcp_profiler import __version__, proxy

USAGE = "mcp-profiler [--log FILE] -- <command> [args...]"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mcp-profiler",
        usage=USAGE,
        description=(
            "Transparent stdio MCP proxy. Launches the command as a child process "
            "and forwards its stdin/stdout unchanged."
        ),
    )
    parser.add_argument(
        "--log",
        metavar="FILE",
        help="append the server's stderr and proxy diagnostics here (default: stderr)",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def split_argv(argv: list[str]) -> tuple[list[str], list[str]]:
    """Split at the first `--`: options before it, server command after it."""
    if "--" not in argv:
        return argv, []
    i = argv.index("--")
    return argv[:i], argv[i + 1 :]


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    own_args, server_cmd = split_argv(argv)

    parser = build_parser()
    args = parser.parse_args(own_args)

    if not server_cmd:
        parser.error("missing server command; usage: " + USAGE)

    returncode = asyncio.run(proxy.run(server_cmd, args.log))

    if returncode < 0:
        # The server died from a signal: die from the same one, so our parent
        # sees exactly what it would have seen without the proxy.
        sig = -returncode
        if sig not in (signal.SIGKILL, signal.SIGSTOP):
            signal.signal(sig, signal.SIG_DFL)
        os.kill(os.getpid(), sig)
        return 128 + sig
    return returncode


if __name__ == "__main__":
    sys.exit(main())
