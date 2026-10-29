# mcp-profiler

A transparent proxy that sits between an MCP client and a stdio MCP server,
timestamps every JSON-RPC message in both directions, and writes a trace you can
analyze offline to see where wall-clock time goes in an agent run.

The proxy forwards original bytes unchanged and has no runtime dependencies
(Python 3.11+ stdlib only).

> **Status:** stage 3. The proxy records a trace of every message, and
> `mcp-profiler analyze` turns traces into a report.

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
mcp-profiler [options] -- <command> [args...]
```

Example MCP client config:

```json
{
  "mcpServers": {
    "filesystem": {
      "command": "mcp-profiler",
      "args": ["--log", "/tmp/fs-proxy.log", "--trace", "/tmp/fs.jsonl", "--", "npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
    }
  }
}
```

Options:

| Option | Meaning |
| --- | --- |
| `--log FILE` | Append the server's stderr (tagged `[server]`) and proxy diagnostics (tagged `[mcp-profiler]`) here instead of stderr |
| `--trace FILE` | Append trace records here. Default: `./traces/<run_id>.jsonl`, relative to the proxy's working directory. MCP clients often launch servers from `/`, so set this in client configs |
| `--name NAME` | Server name recorded in the trace. Default: basename of the command |
| `--capture-payloads` | Also record each message's `params`/`result`/`error` as JSON text, truncated. Off by default: payloads can contain user data and bloat traces |
| `--payload-cap BYTES` | Truncation limit for captured payloads (default 2048) |
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

## Trace format

Newline-delimited JSON. Each run starts with a header, followed by one record per
message seen in either direction:

```json
{"kind":"header","run_id":"8a60…","server":"echo-server","version":"0.1.0","argv":[…],"command":["python","fixtures/echo_server.py"],"pid":15183,"t_wall_ns":1793240551067511557,"t_wall_iso":"2026-10-29T02:22:31.067512+00:00","t_mono_ns":3767725890065,"capture_payloads":false,"payload_cap":2048}
{"run_id":"8a60…","server":"echo-server","seq":5,"t_mono_ns":3768608409076,"direction":"c2s","kind":"request","rpc_id":3,"method":"tools/call","is_error":null,"bytes":92,"tool_name":"fast_tool","proxy_ns":56994}
```

| Field | Meaning |
| --- | --- |
| `seq` | Per-run counter over both directions, taken when the message is read. Sort by it: file order can differ slightly |
| `t_mono_ns` | `time.monotonic_ns()` when the message's last byte was read. Wall clock = `header.t_wall_ns + (t_mono_ns - header.t_mono_ns)` |
| `direction` | `c2s` (client to server) or `s2c` |
| `kind` | `request`, `response`, `notification`, or `parse_error` (not valid JSON or not a JSON-RPC 2.0 message; forwarded anyway) |
| `rpc_id` | Message id. Ids are unique per direction only: match a `c2s` request with an `s2c` response |
| `method` | Requests and notifications only |
| `is_error` | Responses only: the response carries a JSON-RPC `error`. A tool result with `isError: true` is a successful response |
| `bytes` | Raw line length including the newline |
| `tool_name` | `params.name` of a `tools/call` request |
| `ref_id` | `params.requestId` of a `notifications/cancelled`: the request it cancels |
| `proxy_ns` | Time from reading the message to having written it on. This is the proxy's in-process cost only (see below) |

Records are written by a background thread every 100 ms and on exit, including
on SIGINT/SIGTERM and when the server dies. A trace file can hold several runs:
each record carries its `run_id`.

**What `proxy_ns` does not include.** Messages are parsed and recorded *after*
they are forwarded, so parsing never delays the message itself. It can still
delay the next message: recording takes ~8 µs for a typical message and ~30 ms for
a 16 MiB one. Also invisible from inside the proxy: the extra pipe hop and the
event-loop wakeup. On a WSL2 dev box, a ping round trip took ~75 µs direct and
~290 µs through the proxy, with tracing on or off. That is ~105 µs per message,
against a `proxy_ns` median of ~40 µs.

## Analyze

```sh
mcp-profiler analyze [--json] [--min-chain N] <trace.jsonl> [more.jsonl...]
```

Reads one or more traces, merges them by monotonic time, and pairs each request
with its response. Requests are keyed by run, direction and id, and duplicate open
ids are matched earliest first. Each pair becomes a span with a status:

- `ok` or `error`: a response arrived.
- `cancelled`: closed by a `notifications/cancelled`. A response that arrives
  anyway is attached to the span as a late response.
- `orphaned`: still open when its run's trace ends.

The report has five sections:

1. **Wall clock.** Total time, time with at least one request in flight, idle
   time, client gaps, and the share of time spent waiting on each server. A client
   gap runs from a response being forwarded to the client until that client's next
   request. With nothing else in flight, it approximates agent or LLM think time.
2. **Per tool.** Count, p50/p95/max duration over completed calls, and the error
   rate. Errors are JSON-RPC errors only; a tool result with `isError: true` is
   not recorded in the trace.
3. **Concurrency.** Time spent at each in-flight level, the maximum concurrency,
   and how many tool calls were issued while another request was in flight. This
   shows whether the client parallelizes at all.
4. **Sequential chains.** Runs of tool calls where each was issued only after the
   previous response, with nothing else in flight. Each chain shows its total
   client-gap time and how much time it would save if its calls were independent
   and issued together. That saving is an upper bound: a call may depend on the
   previous result.
5. **Instrument overhead.** Total `proxy_ns` as a fraction of wall clock. This
   understates the proxy's real cost; see "What `proxy_ns` does not include" above.

Wall clock counts only time when at least one run is alive (from its first to its
last record). Unrelated sessions in one file therefore don't add their dead time
in between as idle, and their calls never chain together. Runs from different
boots or machines can't be aligned and produce a warning. `--json` also includes
every span and gap.

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
python fixtures/driver.py -- mcp-profiler --log /tmp/p.log --trace /tmp/t.jsonl -- python fixtures/echo_server.py
```

See `CLAUDE.md` for the invariants the proxy must respect.
