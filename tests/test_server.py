"""Tests for FastMCP server creation compatibility."""

from __future__ import annotations

from g_gremlin_hubspot_mcp import server as server_mod


def test_create_mcp_server_uses_version_when_supported(monkeypatch):
    calls: list[dict[str, str]] = []

    class FakeFastMCP:
        def __init__(self, name: str, **kwargs):
            assert name == "g-gremlin-hubspot"
            calls.append(kwargs)

    monkeypatch.setattr(server_mod, "FastMCP", FakeFastMCP)

    server = server_mod._create_mcp_server()
    assert isinstance(server, FakeFastMCP)
    assert calls == [{"version": server_mod.__version__}]


def test_create_mcp_server_falls_back_when_version_kwarg_not_supported(monkeypatch):
    calls: list[dict[str, str]] = []

    class FakeFastMCP:
        def __init__(self, name: str, **kwargs):
            assert name == "g-gremlin-hubspot"
            calls.append(kwargs)
            if "version" in kwargs:
                raise TypeError("FastMCP.__init__() got an unexpected keyword argument 'version'")

    monkeypatch.setattr(server_mod, "FastMCP", FakeFastMCP)

    server = server_mod._create_mcp_server()
    assert isinstance(server, FakeFastMCP)
    assert calls == [{"version": server_mod.__version__}, {}]
