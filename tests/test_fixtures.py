"""The MCP fixtures work directly and behave identically behind the proxy."""

import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DRIVER = [sys.executable, str(ROOT / "fixtures" / "driver.py")]
SERVER = [sys.executable, str(ROOT / "fixtures" / "echo_server.py")]
PROXY = [sys.executable, "-m", "mcp_profiler.cli"]

TIMING = re.compile(r"\s+[\d.]+ ms")


def drive(*driver_args, server=SERVER, via_proxy=None):
    prefix = [*PROXY, "--log", str(via_proxy), "--"] if via_proxy else []
    return subprocess.run(
        [*DRIVER, *driver_args, "--", *prefix, *server],
        capture_output=True,
        text=True,
        timeout=60,
    )


def total_ms(stdout: str) -> float:
    return float(re.search(r"total calls:\s+([\d.]+) ms", stdout).group(1))


def test_driver_output_identical_through_proxy(tmp_path):
    log = tmp_path / "p.log"
    direct = drive()
    proxied = drive(via_proxy=log)
    assert direct.returncode == proxied.returncode == 0
    assert TIMING.sub(" <t>", direct.stdout) == TIMING.sub(" <t>", proxied.stdout)
    assert "-> 'hello'" in proxied.stdout

    # Server stderr reached the driver directly, but went to the log via the proxy.
    assert "echo_server: starting" in direct.stderr
    assert "echo_server" not in proxied.stderr
    assert "[server] echo_server: starting" in log.read_text()


@pytest.mark.parametrize("via_proxy", [False, True])
def test_blocking_flag_serializes_concurrent_calls(tmp_path, via_proxy):
    calls = ["--concurrent", "--calls", "slow:0.5", "slow:0.5", "slow:0.5"]
    log = tmp_path / "p.log" if via_proxy else None
    overlapped = total_ms(drive(*calls, via_proxy=log).stdout)
    serialized = total_ms(drive(*calls, server=[*SERVER, "--blocking"], via_proxy=log).stdout)
    assert overlapped < 1000  # three 0.5 s sleeps overlap
    assert serialized >= 1400  # one after another
