# Results

Stage 4 experiment: does `mcp-profiler bench` classify servers correctly, and how
do real stdio MCP servers handle concurrent tool calls?

Run on 2026-10-29 with the code at commit `3e1f27b` plus the stage 4 changes
(`bench.py`).

## Setup

| | |
| --- | --- |
| Machine | WSL2, Linux 6.18 |
| Python | 3.11, `mcp` SDK 2.3.0 (fixture and the Python servers) |
| Node | 20.18.0 |
| Levels | C = 1, 2, 4, 8, 16 |
| Warmup | 1 round per level, discarded |

At each level C, bench writes C identical `tools/call` requests back to back on
the server's stdin, waits for all C responses, and repeats. Bench is a single
thread; the requests are concurrent only in the sense that several are
outstanding on the pipe at once, as MCP allows. Latency runs from writing a
request to reading its response. Throughput is measured calls over the summed
time of the measured rounds. Speedup is throughput relative to C=1.

Classification rules: **serialized** if throughput never reaches 1.5x that at
C=1; **concurrent** if throughput at C=16 reaches at least 70% of the ideal 16x;
otherwise **partially serialized**, with effective parallelism = peak throughput /
throughput at C=1.

## 1. Validation against the fixture server

`fixtures/echo_server.py`, tool `slow_tool` with `{"seconds": 0.2}`, 32 calls
per level. Both modes are single-threaded asyncio. In the default mode the tool
awaits `asyncio.sleep`, so calls can overlap. With `--blocking` it calls
`time.sleep`, which stalls the event loop.

### `--blocking`: SERIALIZED (expected: serialized)

| C | calls | p50 ms | p95 ms | max ms | calls/s | speedup |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 32 | 205.6 | 206.8 | 207.5 | 4.9 | 1.00x |
| 2 | 32 | 409.6 | 412.2 | 413.0 | 4.9 | 1.00x |
| 4 | 32 | 817.1 | 821.8 | 823.4 | 4.9 | 1.00x |
| 8 | 32 | 1623.7 | 1639.5 | 1641.9 | 4.9 | 1.01x |
| 16 | 32 | 3263.5 | 3276.6 | 3278.0 | 4.9 | 1.00x |

p50 205.6 → 3263.5 ms (15.87x); throughput flat at 4.9 calls/s (1.00x, 6% of
the ideal 16x).

p50 grows by about the full C rather than C/2, as a first-in-first-out queue would
give. While the loop is blocked the server cannot write any response, so every
call in a round finishes only when the whole round's sleeps have run.

### Default (async): CONCURRENT (expected: concurrent)

| C | calls | p50 ms | p95 ms | max ms | calls/s | speedup |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 32 | 206.0 | 207.2 | 208.1 | 4.8 | 1.00x |
| 2 | 32 | 208.2 | 209.9 | 210.1 | 9.5 | 1.97x |
| 4 | 32 | 211.9 | 215.9 | 216.7 | 18.5 | 3.82x |
| 8 | 32 | 219.3 | 225.5 | 226.2 | 35.3 | 7.29x |
| 16 | 32 | 214.4 | 233.6 | 237.3 | 69.7 | 14.39x |

p50 206.0 → 214.4 ms (1.04x); throughput 4.8 → 69.7 calls/s (14.39x, 90% of the
ideal 16x).

### Through the proxy

The async fixture again, with the proxy recording a trace in between
(C = 1, 4, 16; 16 calls per level):

| C | p50 ms | calls/s | speedup |
| ---: | ---: | ---: | ---: |
| 1 | 207.1 | 4.8 | 1.00x |
| 4 | 211.4 | 18.6 | 3.85x |
| 16 | 230.7 | 63.1 | 13.09x |

Still CONCURRENT (82% of ideal). `mcp-profiler analyze` on the trace agreed:
`slow_tool` p50 213.5 ms over 70 calls, max 16 requests in flight. `initialize`
took 749 ms, which is the server's cold start.

The automated tests (`tests/test_bench.py`) repeat both fixture checks at smaller
scale on every run.

## 2. Real servers

Three servers from npm and PyPI, each called with a read-only tool, 64 calls
per level. Nothing was written to disk. The filesystem server was rooted in a
scratch directory holding one 1 KB text file. The git server ran on a throwaway
clone of this repository.

| Server | Version | Tool and arguments |
| --- | --- | --- |
| `@modelcontextprotocol/server-filesystem` (npm, Node) | 2.1.0 | `read_text_file` on a 1050-byte file |
| `mcp-server-git` (PyPI, Python) | 2026.10.28 | `git_log` with `max_count: 10` |
| `mcp-server-time` (PyPI, Python) | 2026.10.28 | `get_current_time` with `timezone: "UTC"` |

### Classification

| Server | p50 at C=1 → C=16 | Throughput at C=1 → C=16 | Speedup at C=16 | Verdict | Run 2 |
| --- | --- | --- | --- | --- | --- |
| filesystem | 1.2 → 6.8 ms | 729 → 2035 calls/s | 2.79x | **Partially serialized**, effective parallelism ≈ 2.8 | Partially serialized, ≈ 4.5 |
| git | 17.1 → 165.4 ms | 54.8 → 55.2 calls/s | 1.01x | **Serialized** | Serialized, 0.92x |
| time | 2.4 → 28.5 ms | 404 → 450 calls/s | 1.12x | **Serialized** | Serialized, 1.00x |

Each server was benched twice. The verdicts matched between runs; the
filesystem server's speedups did not (see caveats).

### Per-level detail (run 1)

**filesystem / `read_text_file`**

| C | p50 ms | p95 ms | max ms | calls/s | speedup |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.2 | 1.8 | 2.2 | 728.6 | 1.00x |
| 2 | 1.5 | 2.6 | 2.9 | 1093.4 | 1.50x |
| 4 | 2.1 | 3.7 | 3.9 | 1538.5 | 2.11x |
| 8 | 3.7 | 6.1 | 6.2 | 1720.2 | 2.36x |
| 16 | 6.8 | 8.1 | 8.5 | 2035.0 | 2.79x |

Run 2 speedups: 1.00, 2.27, 3.58, 3.72, 4.47x.

**git / `git_log`**

| C | p50 ms | p95 ms | max ms | calls/s | speedup |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 17.1 | 21.8 | 44.1 | 54.8 | 1.00x |
| 2 | 26.5 | 37.9 | 58.0 | 56.4 | 1.03x |
| 4 | 48.4 | 73.1 | 74.9 | 55.4 | 1.01x |
| 8 | 84.5 | 140.9 | 151.0 | 56.5 | 1.03x |
| 16 | 165.4 | 281.6 | 313.7 | 55.2 | 1.01x |

Run 2 speedups: 1.00, 0.93, 0.93, 0.93, 0.92x.

**time / `get_current_time`**

| C | p50 ms | p95 ms | max ms | calls/s | speedup |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2.4 | 3.2 | 3.6 | 403.6 | 1.00x |
| 2 | 3.5 | 5.1 | 5.7 | 468.4 | 1.16x |
| 4 | 6.2 | 8.2 | 10.1 | 522.2 | 1.29x |
| 8 | 15.2 | 38.7 | 42.4 | 300.3 | 0.74x |
| 16 | 28.5 | 36.8 | 38.1 | 450.4 | 1.12x |

Run 2 speedups: 1.00, 0.98, 0.96, 0.75, 1.00x.

## 3. Interpretation

- **git is the meaningful result.** `mcp_server_git/server.py` calls synchronous
  GitPython from its async handlers, with no `asyncio.to_thread` or executor.
  Each call blocks the server's only thread, so concurrent calls queue: at C=16
  the median `git_log` takes 165 ms instead of 17 ms. This is the same pattern as
  the `--blocking` fixture. It is a performance limitation, not a protocol bug
  (MCP does not require concurrent handling). It would be straightforward to fix
  by running the git work on a worker thread.
- **time is serialized, but trivially.** `get_current_time` is pure computation
  taking about 2 ms, almost all of it per-message overhead. A single-threaded
  server has no I/O to overlap here, so "serialized" is accurate but not a design
  flaw.
- **filesystem overlaps partially.** Node handles the file reads on background
  I/O threads, so some calls overlap, but at about 1 ms per call the
  single-threaded JavaScript side (parsing, validation, JSON) limits throughput
  to roughly 2,000 calls/s.

## 4. Caveats

- **Fast tools mostly measure overhead.** For tools under 5 ms at C=1 (filesystem
  and time), the numbers reflect pipes, JSON and the server's event loop more
  than the tool. Bench prints a note when this applies.
- **Fast-tool speedups are noisy.** The filesystem server's effective parallelism
  was 2.8 in one run and 4.5 in the next, and the time server had a one-off dip at
  C=8. The verdicts were stable across runs; the exact figures were not.
- **One machine, one tool per server.** Another tool on the same server, such as
  a slow directory walk instead of a small file read, could classify differently.
- **Barrier rounds.** Each round waits for its slowest call before the next
  starts, so one slow call in a round lowers that level's throughput.

## Reproducing

```sh
# fixtures
mcp-profiler bench --tool slow_tool --tool-args '{"seconds": 0.2}' -- python fixtures/echo_server.py --blocking
mcp-profiler bench --tool slow_tool --tool-args '{"seconds": 0.2}' -- python fixtures/echo_server.py

# real servers ($SCRATCH: a throwaway directory)
mkdir -p $SCRATCH/fsroot && printf 'hello from the bench\n%.0s' {1..50} > $SCRATCH/fsroot/sample.txt
git clone . $SCRATCH/repo

mcp-profiler bench --calls-per-level 64 --tool read_text_file --tool-args "{\"path\":\"$SCRATCH/fsroot/sample.txt\"}" \
    -- npx -y @modelcontextprotocol/server-filesystem $SCRATCH/fsroot
mcp-profiler bench --calls-per-level 64 --tool git_log --tool-args "{\"repo_path\":\"$SCRATCH/repo\",\"max_count\":10}" \
    -- uvx mcp-server-git --repository $SCRATCH/repo
mcp-profiler bench --calls-per-level 64 --tool get_current_time --tool-args '{"timezone":"UTC"}' \
    -- uvx mcp-server-time
```
