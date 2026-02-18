"""Tests for envelope.py — response envelope and plan hash."""

from __future__ import annotations

import json

import pytest

from g_gremlin_hubspot_mcp.envelope import (
    Artifact,
    Safety,
    Warning,
    build_envelope,
    compute_plan_hash,
    error_envelope,
)
from g_gremlin_hubspot_mcp.runner import RunResult


class TestComputePlanHash:
    def test_deterministic(self):
        data = {"groups": [{"key": "a@b.com", "primary": "1"}]}
        h1 = compute_plan_hash(data)
        h2 = compute_plan_hash(data)
        assert h1 == h2

    def test_starts_with_sha256(self):
        h = compute_plan_hash({"test": True})
        assert h.startswith("sha256:")

    def test_different_data_different_hash(self):
        h1 = compute_plan_hash({"a": 1})
        h2 = compute_plan_hash({"a": 2})
        assert h1 != h2

    def test_key_order_independent(self):
        h1 = compute_plan_hash({"a": 1, "b": 2})
        h2 = compute_plan_hash({"b": 2, "a": 1})
        assert h1 == h2  # sort_keys=True makes this work


class TestBuildEnvelope:
    def test_schema_present(self):
        result = RunResult(stdout='{"ok": true}', stderr="", exit_code=0)
        raw = build_envelope(run_result=result, summary="test")
        parsed = json.loads(raw)
        assert parsed["$schema"] == "GremlinMCPResponse/v1"

    def test_ok_true_on_success(self):
        result = RunResult(stdout='{"ok": true}', stderr="", exit_code=0)
        raw = build_envelope(run_result=result)
        parsed = json.loads(raw)
        assert parsed["ok"] is True

    def test_ok_false_on_failure(self):
        result = RunResult(stdout="", stderr="Error", exit_code=1)
        raw = build_envelope(run_result=result)
        parsed = json.loads(raw)
        assert parsed["ok"] is False

    def test_summary_included(self):
        result = RunResult(stdout='{}', stderr="", exit_code=0)
        raw = build_envelope(run_result=result, summary="Pulled 100 contacts")
        parsed = json.loads(raw)
        assert parsed["summary"] == "Pulled 100 contacts"

    def test_artifact_included_when_provided(self):
        result = RunResult(stdout='{}', stderr="", exit_code=0)
        art = Artifact(path="/tmp/test.csv", row_count=100, columns=["id", "email"])
        raw = build_envelope(run_result=result, artifact=art)
        parsed = json.loads(raw)
        assert parsed["artifact"]["path"] == "/tmp/test.csv"
        assert parsed["artifact"]["row_count"] == 100

    def test_safety_defaults_to_read(self):
        result = RunResult(stdout='{}', stderr="", exit_code=0)
        raw = build_envelope(run_result=result)
        parsed = json.loads(raw)
        assert parsed["safety"]["impact"] == "read"
        assert parsed["safety"]["dry_run"] is False

    def test_safety_with_plan_hash(self):
        result = RunResult(stdout='{}', stderr="", exit_code=0)
        safety = Safety(dry_run=True, requires_apply=True, impact="merge", plan_hash="sha256:abc")
        raw = build_envelope(run_result=result, safety=safety)
        parsed = json.loads(raw)
        assert parsed["safety"]["plan_hash"] == "sha256:abc"
        assert parsed["safety"]["dry_run"] is True

    def test_raw_included(self):
        result = RunResult(stdout='{"test": 1}', stderr="warn", exit_code=0)
        raw = build_envelope(run_result=result)
        parsed = json.loads(raw)
        assert parsed["raw"]["exit_code"] == 0
        assert "warn" in parsed["raw"]["stderr"]

    def test_warnings_surfaced(self):
        result = RunResult(stdout='{}', stderr="", exit_code=0)
        warns = [Warning(code="TEST_WARN", message="test warning")]
        raw = build_envelope(run_result=result, extra_warnings=warns)
        parsed = json.loads(raw)
        assert len(parsed["warnings"]) == 1
        assert parsed["warnings"][0]["code"] == "TEST_WARN"

    def test_meta_includes_version(self):
        result = RunResult(stdout='{}', stderr="", exit_code=0)
        raw = build_envelope(run_result=result)
        parsed = json.loads(raw)
        assert "requires_g_gremlin" in parsed["meta"]
        assert "timestamp" in parsed["meta"]

    def test_agentic_result_extracted(self):
        agentic = json.dumps({
            "$schema": "AgenticResult/v1",
            "command": "test",
            "status": "success",
            "result": {"count": 42},
        })
        result = RunResult(stdout=agentic, stderr="", exit_code=0)
        raw = build_envelope(run_result=result)
        parsed = json.loads(raw)
        assert parsed["data"]["count"] == 42
        assert parsed["raw"]["agentic_result"]["$schema"] == "AgenticResult/v1"


class TestErrorEnvelope:
    def test_error_envelope_ok_false(self):
        raw = error_envelope("Something failed")
        parsed = json.loads(raw)
        assert parsed["ok"] is False
        assert parsed["summary"] == "Something failed"

    def test_error_envelope_has_schema(self):
        raw = error_envelope("fail")
        parsed = json.loads(raw)
        assert parsed["$schema"] == "GremlinMCPResponse/v1"
