# mcp-profiler

A transparent proxy that sits between an MCP client and a stdio MCP server,
timestamps every JSON-RPC message in both directions, and writes a trace you can
analyze offline to see where wall-clock time goes in an agent run. It also ships
a load generator that measures whether a server handles concurrent calls
concurrently.

The proxy forwards original bytes unchanged and has no runtime dependencies
(Python 3.11+ stdlib only).

> **Status:** stage 4. The proxy records a trace of every message,
> `mcp-profiler analyze` turns traces into a report, and `mcp-profiler bench`
> measures whether a server serializes concurrent calls. Experiment results are in
> [Results.md](Results.md).

## What it can do

Three commands, one instrument:

| Command | What it answers |
| --- | --- |
| `mcp-profiler -- <server>` | Records every message between a real client and a real server, with timestamps, while you work normally |
| `mcp-profiler analyze` | From those recordings: how much of the run was spent waiting on tools vs. idle (agent/LLM think time), which tools are slow (p50/p95/max), whether the client ever runs tool calls in parallel, and which chains of one-at-a-time calls could have overlapped |
| `mcp-profiler bench` | Under synthetic load: does this server process concurrent calls concurrently, one at a time, or somewhere in between? |

Typical uses:

- **MCP server authors:** `bench` shows whether a blocking call in an async handler
  makes the server process calls one at a time. Cheap to run in CI as a
  regression check.
- **Agent builders:** `analyze` shows whether your agent issues independent tool
  calls in parallel or one after another, and what that costs.
- **Deciding what not to optimize:** if the report says 90% of the run was client
  think time, speeding up the MCP server won't help.
- **Startup and slow calls:** `initialize` time shows cold-start cost
  (`npx`/`uvx` servers can take seconds), and per-tool percentiles show the slow tool.
- **Wire-level debugging:** with `--capture-payloads` a trace is a timestamped log
  of every message; hung calls show up as orphaned spans.

### What it can and can't see

| Kind of tool | Visible? |
| --- | --- |
| Local stdio MCP server, local work (filesystem, git, time) | Yes |
| Local stdio MCP server that calls the network (e.g. an API wrapper run via `npx`/`uvx`) | Yes. Network time is inside the server, so it is part of the span, but not broken out |
| Remote MCP server configured by URL (Streamable HTTP / SSE) | Not directly: v1 is stdio only. Use a local stdio-to-HTTP bridge such as `mcp-remote` and put the proxy in front of it |
| A client's built-in tools (e.g. Claude Code's Bash, Read, Edit) | No. They are not MCP and never cross a pipe |
| LLM time | Only indirectly, as client gaps between tool calls. A gap also includes client overhead and any time a human spends approving a prompt |

Spans are black boxes: the profiler tells you a call took 800 ms, not which line
of the server was slow. Use a profiler inside the server (py-spy, clinic.js) for
that.

## How it works

```
              stdin                         stdin
  MCP client ───────▶  mcp-profiler  ───────▶  MCP server
             ◀───────   (the proxy)  ◀───────
              stdout        │               stdout
                            │ server stderr ──▶ --log file or stderr
                            ▼
                      trace.jsonl  ──▶  mcp-profiler analyze  ──▶  report
```

**Recording.** The client launches `mcp-profiler`, which launches the real server
as a child process in its own process group. At startup the proxy writes a trace
header with a monotonic and a wall-clock reading taken back to back, so every
later monotonic timestamp can be converted to a date. Three asyncio pumps run on
one event-loop thread: client to server, server to client, and server stderr to
the log. For each line a pump:

1. reads up to the newline and takes a monotonic timestamp and a sequence number,
2. writes the exact bytes on and waits until the kernel has them,
3. only then parses a copy of the line and queues a trace record (direction, kind,
   id, method, tool name, size, and `proxy_ns`, the proxy's own time for it).

Forwarding before parsing means measurement never delays the message, and the
bytes arrive exactly as sent. A background thread writes queued records to the
trace file every 100 ms and on exit. A separate thread writes the server's stderr,
so a slow disk can never stall protocol traffic. Messages are never reordered,
batched or held back.

**Analysis.** `analyze` merges traces by monotonic time, which all processes on
one machine share, so traces from several servers line up. It then walks the
timeline once and pairs each request with its response by `(run, direction, id)`,
producing spans (`ok`, `error`, `cancelled`, `orphaned`) and client gaps. The
report is computed from those: unions of span intervals for busy and idle time,
a sweep over span starts and ends for concurrency, and runs of calls issued with
nothing in flight for sequential chains.

**Bench.** `bench` is the client instead of a bystander. A minimal built-in MCP
client on a single thread starts the server, initializes, and at each
concurrency level C writes C `tools/call` requests back to back without waiting,
then matches responses by id as they come back. Several requests outstanding on
one pipe is normal MCP; how the server handles the overlap is what gets measured.
A single-threaded asyncio or Node server can overlap calls whose handlers await
I/O, but a handler that blocks its thread makes every other call wait.

**What the numbers mean.** All times are as seen at the proxy (or at bench's
client). A span is the server's processing plus two pipe hops; a client gap is
the client's time plus pipe hops. See "What `proxy_ns` does not include" below
for the proxy's own cost.

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

To profile a remote server, wrap a stdio bridge the same way, e.g.
`"args": ["--trace", "/tmp/remote.jsonl", "--", "npx", "-y", "mcp-remote", "https://example.com/mcp"]`.
Spans then include the network round trip and the bridge's own overhead.

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
- Anything that is not valid JSON-RPC is forwarded anyway and recorded as a
  `parse_error`.
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

## Bench

```sh
mcp-profiler bench --tool NAME [--tool-args JSON] [--concurrency 1,2,4,8,16] \
    [--calls-per-level 32] [--warmup 1] [--json] -- <server command...>
```

Starts the server, connects to it as an MCP client, and calls one tool at rising
concurrency to see whether the server serializes calls. At each level C it issues
C identical calls at once, waits for all of them, and repeats. The first
`--warmup` rounds of each level are discarded, then it runs `ceil(calls / C)`
measured rounds. One probe call before the first level checks the tool and its
arguments.

Per level it reports p50/p95/max latency, throughput (measured calls over the
time of the measured rounds), speedup over the lowest level, and errors. A
JSON-RPC error and a result with `isError: true` both count as errors. Then it
classifies the server, comparing the highest level with the lowest:

- **serialized**: throughput never reaches 1.5x the lowest level's. Latency grows
  with C instead.
- **concurrent**: throughput at the highest level reaches at least 70% of ideal
  linear scaling, and latency stays roughly flat.
- **partially serialized**: in between. The report gives the effective
  parallelism: peak throughput divided by throughput at the lowest level.

"Serialized" describes behavior, not a protocol violation: MCP does not require
servers to process requests concurrently. It does mean parallel tool calls from
an agent queue up behind each other.

If a call takes under 5 ms, the report notes that it mostly measures per-message
overhead (pipes, JSON, the server's event loop) rather than the tool itself.
Speedups for such fast tools vary noticeably between runs; the verdict is more
stable than the numbers.

Bench speaks MCP with its own minimal stdlib client (initialize, tools/list,
tools/call, ping), so it adds no dependencies. Latency runs from writing the
request to reading the response. Its report goes to stdout. To record a trace of
the load, put the proxy in front of the server:

```sh
mcp-profiler bench --tool slow_tool --tool-args '{"seconds": 0.2}' \
    -- mcp-profiler --trace /tmp/bench.jsonl -- python fixtures/echo_server.py --blocking
```

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
