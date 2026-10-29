import pytest

from mcp_profiler import cli


def test_package_imports():
    import mcp_profiler
    from mcp_profiler import analyze, proxy, trace  # noqa: F401

    assert mcp_profiler.__version__


def test_split_argv():
    assert cli.split_argv(["--log", "p.log", "--", "srv", "--", "x"]) == (
        ["--log", "p.log"],
        ["srv", "--", "x"],
    )
    assert cli.split_argv(["--log", "p.log"]) == (["--log", "p.log"], [])


def test_missing_server_command_errors():
    with pytest.raises(SystemExit) as exc:
        cli.main([])
    assert exc.value.code == 2
