# mcp-profiler

A transparent proxy that sits between an MCP client and a stdio MCP server,
timestamps every JSON-RPC message in both directions, and writes a trace you can
analyze offline to see where wall-clock time goes in an agent run.

The proxy forwards original bytes unchanged and has no runtime dependencies
(Python 3.11+ stdlib only).

> **Status:** scaffolding only. The CLI parses arguments, but the proxy is not
> implemented yet.

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
mcp-profiler [options] -- <server command...>
```

Example MCP client config:

```json
{
  "mcpServers": {
    "filesystem": {
      "command": "mcp-profiler",
      "args": ["-o", "traces/fs.jsonl", "--", "npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
    }
  }
}
```

Options:

| Option | Meaning |
| --- | --- |
| `-o, --output PATH` | Trace file to write |
| `--log-file PATH` | Write proxy diagnostics to a file instead of stderr |
| `--version` | Show version and exit |

The proxy never writes anything but forwarded protocol bytes to stdout.

## Development

```sh
uv sync            # creates .venv with dev dependencies (pytest, mcp SDK for fixtures)
uv run pytest
```

See `CLAUDE.md` for the invariants the proxy must respect.
