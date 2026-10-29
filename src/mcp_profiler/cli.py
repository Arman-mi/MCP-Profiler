"""Entry points:
  mcp-profiler [options] -- <command> [args...]     run the proxy
  mcp-profiler analyze [options] <trace.jsonl>...   offline analysis (see analyze.py)

stdout is the MCP protocol channel once the proxy runs, so every diagnostic
in this module goes to stderr.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys

from mcp_profiler import __version__, analyze, proxy, trace

USAGE = "mcp-profiler [options] -- <command> [args...]  |  mcp-profiler analyze <trace.jsonl>..."


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mcp-profiler",
        usage=USAGE,
        description=(
            "Transparent stdio MCP proxy. Launches the command as a child process, "
            "forwards its stdin/stdout unchanged, and records a trace of every message."
        ),
    )
    parser.add_argument(
        "--log",
        metavar="FILE",
        help="append the server's stderr and proxy diagnostics here (default: stderr)",
    )
    parser.add_argument(
        "--trace",
        metavar="FILE",
        help=f"append trace records here (default: ./{trace.DEFAULT_DIR}/<run_id>.jsonl)",
    )
    parser.add_argument(
        "--name",
        help="server name recorded in the trace (default: basename of the command)",
    )
    parser.add_argument(
        "--capture-payloads",
        action="store_true",
        help="also record params/result/error, truncated; may contain user data",
    )
    parser.add_argument(
        "--payload-cap",
        type=int,
        default=trace.DEFAULT_PAYLOAD_CAP,
        metavar="BYTES",
        help="truncate captured payloads to this many bytes (default: %(default)s)",
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
    if argv[:1] == ["analyze"]:
        # A server named "analyze" still works: `mcp-profiler -- analyze`.
        return analyze.main(argv[1:])
    own_args, server_cmd = split_argv(argv)

    parser = build_parser()
    args = parser.parse_args(own_args)

    if not server_cmd:
        parser.error("missing server command; usage: " + USAGE)
    if args.payload_cap < 0:
        parser.error("--payload-cap must be >= 0")

    run_id = trace.new_run_id()
    trace_path = args.trace or trace.default_path(run_id)
    try:
        # Opened before the server starts: a bad trace path fails fast, here.
        writer = trace.TraceWriter(
            trace_path,
            run_id=run_id,
            server=args.name or os.path.basename(server_cmd[0]),
            argv=[sys.argv[0], *argv],
            command=server_cmd,
            capture_payloads=args.capture_payloads,
            payload_cap=args.payload_cap,
        )
    except OSError as e:
        parser.exit(2, f"mcp-profiler: cannot open trace file {trace_path!r}: {e}\n")

    returncode = asyncio.run(proxy.run(server_cmd, args.log, writer))

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
