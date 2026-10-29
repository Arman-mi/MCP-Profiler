# mcp-profiler

A transparent proxy that sits between an MCP client and a stdio MCP server,
timestamps every JSON-RPC message in both directions, and writes a trace you can
analyze offline to see where wall-clock time goes in an agent run.

The proxy forwards original bytes unchanged and has no runtime dependencies
(Python 3.11+ stdlib only).

> **Status:** stage 1, a pure passthrough proxy. No traces are recorded yet.

## Install

```sh
# from a checkout, for development
uv sync

# as a standalone tool
uv tool install .
```

## Usage

Put `mcp-profiler` in front of the server command your MCP client would normally
launch, separated by `--`:

```sh
mcp-profiler [--log FILE] -- <command> [args...]
```

Example MCP client config:

```json
{
  "mcpServers": {
    "filesystem": {
      "command": "mcp-profiler",
      "args": ["--log", "/tmp/fs-proxy.log", "--", "npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
    }
  }
}
```

Options:

| Option | Meaning |
| --- | --- |
| `--log FILE` | Append the server's stderr (tagged `[server]`) and proxy diagnostics (tagged `[mcp-profiler]`) here instead of stderr |
| `--version` | Show version and exit |

The proxy never writes anything but forwarded protocol bytes to stdout.

Behavior:

- Messages are forwarded line by line, byte for byte, and each write is flushed
  before the next read. Lines longer than 16 MiB are forwarded in pieces.
- When the client closes stdin, the proxy closes the server's stdin and exits
  when the server does. It exits with the server's exit code, or dies from the
  same signal that killed the server.
- The server runs in its own process group. SIGINT/SIGTERM sent to the proxy are
  forwarded to that group once. If the server is still alive 2 s later, or a
  second signal arrives, the group is SIGKILLed.

## Development

```sh
uv sync            # creates .venv with dev dependencies (pytest, mcp SDK for fixtures)
uv run pytest
```

Test fixtures:

```sh
# MCP server with fast_tool, slow_tool(seconds), echo(text). With --blocking,
# slow_tool blocks the event loop, so concurrent calls run one after another.
python fixtures/echo_server.py [--blocking]

# Client: initialize, list tools, run calls sequentially or concurrently, print latency.
python fixtures/driver.py [--concurrent] [--calls fast slow:0.5 echo:hi ...] -- <server command...>

# The same run through the proxy.
python fixtures/driver.py -- mcp-profiler --log /tmp/p.log -- python fixtures/echo_server.py
```

See `CLAUDE.md` for the invariants the proxy must respect.
