"""Characterization tests for scripts/verify_api_integration.py.

scripts/ is not a package, so the module under test is loaded by file path.
Covers parse_api_client, parse_mcp_server, and main() in both its
"--local"/"--pre-commit" single-agent mode and its default workspace-wide
scan mode, against fixture agent trees rather than the real fleet.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "verify_api_integration.py"
)


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "verify_api_integration_under_test", _SCRIPT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def viapi():
    return _load_module()


API_CLIENT_SOURCE = """
class ExampleApiClient:
    def __init__(self):
        pass

    def authenticate(self):
        pass

    def _private_helper(self):
        pass

    def list_topics(self):
        pass

    def get_records(self):
        pass


class WidgetHandler:
    def unrelated_method(self):
        pass
"""

MCP_SERVER_SOURCE = """
class Dummy:
    def tool(self, *a, **kw):
        def deco(fn):
            return fn
        return deco

mcp = Dummy()

@mcp.tool()
async def registered_tool(client, action: str):
    if action == "list":
        return client.list_topics()
    return getattr(client, "get_records")()

async def kafka_untagged_but_prefixed(client):
    return client.get_records()

async def not_tracked_at_all(client):
    return client.list_topics()
"""

NO_MATCH_SOURCE = "async def plain_helper(client):\n    return client.list_topics()\n"


def test_parse_api_client_finds_public_methods_on_api_or_client_class(tmp_path, viapi):
    api_file = tmp_path / "api_client.py"
    api_file.write_text(API_CLIENT_SOURCE)

    methods = viapi.parse_api_client(str(api_file))

    assert set(methods) == {"list_topics", "get_records"}
    assert methods["list_topics"]["class"] == "ExampleApiClient"


def test_parse_api_client_returns_empty_dict_when_no_matching_class(tmp_path, viapi):
    api_file = tmp_path / "api_client.py"
    api_file.write_text("class Widget:\n    def make(self):\n        pass\n")

    assert viapi.parse_api_client(str(api_file)) == {}


def test_parse_mcp_server_maps_decorated_and_prefixed_tools(tmp_path, viapi):
    mcp_file = tmp_path / "mcp_server.py"
    mcp_file.write_text(MCP_SERVER_SOURCE)
    api_methods = {"list_topics": {}, "get_records": {}}

    tool_mappings, mapped = viapi.parse_mcp_server(str(mcp_file), api_methods)

    assert "registered_tool" in tool_mappings
    assert "kafka_untagged_but_prefixed" in tool_mappings
    assert "not_tracked_at_all" not in tool_mappings
    assert mapped == {"list_topics", "get_records"}
    assert set(tool_mappings["registered_tool"]["methods"]) == {
        "list_topics",
        "get_records",
    }
    assert tool_mappings["registered_tool"]["actions"] == ["list"]


def test_parse_mcp_server_untracked_function_is_ignored(tmp_path, viapi):
    mcp_file = tmp_path / "mcp_server.py"
    mcp_file.write_text(NO_MATCH_SOURCE)

    tool_mappings, mapped = viapi.parse_mcp_server(str(mcp_file), {"list_topics": {}})

    assert tool_mappings == {}
    assert mapped == set()


def _write_agent(agent_dir: Path, api_source: str, mcp_source: str):
    agent_dir.mkdir(parents=True, exist_ok=True)
    (agent_dir / "api_client.py").write_text(api_source)
    (agent_dir / "mcp_server.py").write_text(mcp_source)


def test_main_local_mode_passes_when_coverage_meets_baseline(
    tmp_path, viapi, monkeypatch, capsys
):
    agent_dir = tmp_path / "kafka-mcp"
    _write_agent(agent_dir, API_CLIENT_SOURCE, MCP_SERVER_SOURCE)
    monkeypatch.chdir(agent_dir)
    monkeypatch.setattr(viapi, "BASELINES", {"kafka-mcp": 0.0})
    monkeypatch.setattr(sys, "argv", ["verify_api_integration.py", "--local"])

    with pytest.raises(SystemExit) as exc:
        viapi.main()

    assert exc.value.code == 0
    assert "PASSED" in capsys.readouterr().out


def test_main_local_mode_fails_when_coverage_below_baseline(
    tmp_path, viapi, monkeypatch, capsys
):
    agent_dir = tmp_path / "kafka-mcp"
    _write_agent(agent_dir, API_CLIENT_SOURCE, NO_MATCH_SOURCE)
    monkeypatch.chdir(agent_dir)
    monkeypatch.setattr(viapi, "BASELINES", {"kafka-mcp": 90.0})
    monkeypatch.setattr(sys, "argv", ["verify_api_integration.py", "--local"])

    with pytest.raises(SystemExit) as exc:
        viapi.main()

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "FAILED" in out
    assert "get_records" in out
    assert "authenticate" not in out


def test_main_local_mode_skips_when_no_client_or_server_present(
    tmp_path, viapi, monkeypatch, capsys
):
    empty_dir = tmp_path / "not-an-agent"
    empty_dir.mkdir()
    monkeypatch.chdir(empty_dir)
    monkeypatch.setattr(sys, "argv", ["verify_api_integration.py", "--local"])

    with pytest.raises(SystemExit) as exc:
        viapi.main()

    assert exc.value.code == 0
    assert "Skipping integration parity verification" in capsys.readouterr().out


def test_main_workspace_scan_reports_all_agents(tmp_path, viapi, monkeypatch, capsys):
    # main()'s default mode derives agents_dir from dirname(__file__) + "/../..".
    # Point the module's __file__ at a fake two-level-deep path under tmp_path
    # so agents_dir resolves to tmp_path without touching the real fleet.
    fake_script = tmp_path / "pkg" / "scripts" / "verify_api_integration.py"
    monkeypatch.setattr(viapi, "__file__", str(fake_script))
    monkeypatch.setattr(sys, "argv", ["verify_api_integration.py"])
    monkeypatch.setattr(
        viapi, "BASELINES", {"agent-full": 0.0, "agent-partial": 0.0}
    )

    _write_agent(tmp_path / "agent-full", API_CLIENT_SOURCE, MCP_SERVER_SOURCE)
    _write_agent(tmp_path / "agent-partial", API_CLIENT_SOURCE, NO_MATCH_SOURCE)
    _write_agent(tmp_path / "group" / "agent-nested", API_CLIENT_SOURCE, MCP_SERVER_SOURCE)
    (tmp_path / ".hidden").mkdir()
    (tmp_path / "something.egg-info").mkdir()
    (tmp_path / "somevenvdir").mkdir()
    broken = tmp_path / "agent-broken"
    _write_agent(broken, "def broken(:\n", MCP_SERVER_SOURCE)

    viapi.main()

    captured = capsys.readouterr()
    assert "agent-full" in captured.out
    assert "agent-partial" in captured.out
    assert "agent-nested" in captured.out
    assert "100%" in captured.out
    assert "Parity Gap" in captured.out
    assert "Operation failed: SyntaxError" in captured.err
