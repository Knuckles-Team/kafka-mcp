"""Characterization tests for kafka_mcp's module-level __getattr__.

kafka_mcp.__init__ lazily imports its optional submodules (kafka_mcp.mcp_server,
kafka_mcp.agent_server) the first time an unresolved attribute is looked up, and
exposes two synthetic availability flags (_MCP_AVAILABLE, _AGENT_AVAILABLE) that
probe whether the corresponding optional module can be imported at all.
"""

import kafka_mcp


def test_mcp_available_flag_true_when_mcp_server_importable():
    assert kafka_mcp._MCP_AVAILABLE is True


def test_agent_available_flag_true_when_agent_server_importable():
    assert kafka_mcp._AGENT_AVAILABLE is True


def test_availability_flag_false_when_no_optional_module_matches_fragment(
    monkeypatch,
):
    monkeypatch.setattr(kafka_mcp, "OPTIONAL_MODULES", {})
    assert kafka_mcp.__getattr__("_MCP_AVAILABLE") is False
    assert kafka_mcp.__getattr__("_AGENT_AVAILABLE") is False


def test_getattr_exposes_member_from_lazily_loaded_optional_module():
    # get_mcp_instance is defined in kafka_mcp.mcp_server and only reachable
    # through the optional-module lookup in __getattr__, not CORE_MODULES.
    assert callable(kafka_mcp.get_mcp_instance)


def test_getattr_raises_attribute_error_for_unknown_name():
    import pytest

    with pytest.raises(AttributeError, match="has no attribute 'totally_bogus_name'"):
        kafka_mcp.__getattr__("totally_bogus_name")


def test_dir_includes_lazily_exposed_members():
    # __dir__ merges globals() (populated by _expose_members after a lazy
    # import) with __all__.
    kafka_mcp.get_mcp_instance  # force the lazy import/expose path
    assert "get_mcp_instance" in dir(kafka_mcp)
