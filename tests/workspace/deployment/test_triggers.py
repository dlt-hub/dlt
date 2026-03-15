"""Tests for trigger parsing, normalization, and selectors."""

from typing import List

import pytest

from dlt._workspace.deployment._trigger_helpers import (
    filter_jobs_by_selectors,
    matches_selector,
    normalize_trigger,
    normalize_triggers,
    parse_trigger,
)
from dlt._workspace.deployment.typing import (
    HttpTriggerInfo,
    TEntryPoint,
    TExecutionSpec,
    TJobDefinition,
    TJobRef,
    TParsedTrigger,
    TTrigger,
)


@pytest.mark.parametrize(
    "trigger,expected_type,expected_expr",
    [
        ("schedule:0 8 * * *", "schedule", "0 8 * * *"),
        ("every:5h", "every", "5h"),
        ("once:2026-03-15T08:00:00Z", "once", "2026-03-15T08:00:00Z"),
        ("job.success:jobs.batch.ingest", "job.success", "jobs.batch.ingest"),
        ("job.fail:jobs.batch.ingest", "job.fail", "jobs.batch.ingest"),
        ("http:", "http", ""),
        ("http:9090", "http", "9090"),
        ("http:/mcp", "http", "/mcp"),
        ("http:9090/mcp", "http", "9090/mcp"),
        ("deployment:", "deployment", ""),
        ("webhook:ingest/chat", "webhook", "ingest/chat"),
        ("webhook:", "webhook", ""),
        ("tag:backfill", "tag", "backfill"),
    ],
    ids=[
        "schedule",
        "every",
        "once",
        "job-success",
        "job-fail",
        "http-empty",
        "http-port",
        "http-path",
        "http-port-path",
        "deployment",
        "webhook-path",
        "webhook-empty",
        "tag",
    ],
)
def test_parse_trigger(trigger: str, expected_type: str, expected_expr: str) -> None:
    parsed = parse_trigger(TTrigger(trigger))
    assert parsed.type == expected_type
    # expr is typed — compare as string for simple cases
    if parsed.expr is None:
        assert expected_expr == ""
    else:
        assert str(parsed.expr) == expected_expr or parsed.raw.endswith(expected_expr)


@pytest.mark.parametrize(
    "trigger",
    ["nocolon", "", "unknown:something"],
    ids=["no-colon", "empty", "unknown-type"],
)
def test_parse_trigger_invalid(trigger: str) -> None:
    with pytest.raises(ValueError):
        parse_trigger(TTrigger(trigger))


@pytest.mark.parametrize(
    "trigger,expected_port,expected_path",
    [
        ("http:", None, ""),
        ("http:9090", 9090, ""),
        ("http:/mcp", None, "/mcp"),
        ("http:9090/mcp", 9090, "/mcp"),
        ("http:8080/api/v1", 8080, "/api/v1"),
        ("http:/deep/nested/path", None, "/deep/nested/path"),
        ("http:1", 1, ""),
        ("http:65535", 65535, ""),
    ],
    ids=[
        "empty",
        "port-only",
        "path-only",
        "port-and-path",
        "port-and-deep-path",
        "deep-path-only",
        "min-port",
        "max-port",
    ],
)
def test_parse_http_trigger(trigger: str, expected_port: int, expected_path: str) -> None:
    parsed = parse_trigger(TTrigger(trigger))
    assert parsed.type == "http"
    assert isinstance(parsed.expr, HttpTriggerInfo)
    assert parsed.expr == HttpTriggerInfo(port=expected_port, path=expected_path)


@pytest.mark.parametrize(
    "trigger,error_fragment",
    [
        ("http:0", "1-65535"),
        ("http:99999", "port invalid"),
        ("http:abc", "port invalid"),
        ("http://localhost:5000", "not a full URL"),
        ("http://0.0.0.0:8080/mcp", "not a full URL"),
    ],
    ids=[
        "port-zero",
        "port-too-high",
        "port-nan",
        "full-url-localhost",
        "full-url-with-host",
    ],
)
def test_parse_http_trigger_invalid(trigger: str, error_fragment: str) -> None:
    with pytest.raises(ValueError, match=error_fragment):
        parse_trigger(TTrigger(trigger))


@pytest.mark.parametrize(
    "trigger",
    [
        "schedule:0 8 * * *",
        "every:5h",
        "once:2026-03-15T08:00:00Z",
        "job.success:jobs.batch.ingest",
        "job.fail:jobs.batch.ingest",
        "http:",
        "http:5000",
        "http:/mcp",
        "http:9090/api",
        "deployment:",
        "webhook:ingest/chat",
        "webhook:",
        "tag:backfill",
    ],
    ids=[
        "schedule",
        "every",
        "once",
        "job-success",
        "job-fail",
        "http-empty",
        "http-port",
        "http-path",
        "http-port-path",
        "deployment",
        "webhook-path",
        "webhook-empty",
        "tag",
    ],
)
def test_normalize_trigger_valid(trigger: str) -> None:
    result = normalize_trigger(trigger)
    assert isinstance(result, str)
    assert ":" in result


@pytest.mark.parametrize(
    "trigger,error_fragment",
    [
        ("unknown:foo", "unknown trigger type"),
        ("schedule:", "requires a cron"),
        ("every:", "requires a period"),
        ("once:", "requires a timestamp"),
        ("tag:", "requires a name"),
        ("job.success:", "requires a job_ref"),
        ("job.fail:", "requires a job_ref"),
        ("http:99999", "port invalid"),
        ("http:0", "1-65535"),
        ("http:abc", "port invalid"),
        ("http://localhost:5000", "not a full URL"),
        ("job.success:not_a_ref", "must start with"),
        ("job.fail:bad_ref", "must start with"),
    ],
    ids=[
        "unknown-type",
        "schedule-empty",
        "every-empty",
        "once-empty",
        "tag-empty",
        "job-success-empty",
        "job-fail-empty",
        "http-port-too-high",
        "http-port-zero",
        "http-port-nan",
        "http-full-url",
        "job-success-bad-ref",
        "job-fail-bad-ref",
    ],
)
def test_normalize_trigger_invalid(trigger: str, error_fragment: str) -> None:
    with pytest.raises(ValueError, match=error_fragment):
        normalize_trigger(trigger)


def test_normalize_trigger_typed() -> None:
    """normalize_trigger parses type:expr triggers via their parser."""
    assert normalize_trigger("schedule:0 8 * * *") == TTrigger("schedule:0 8 * * *")
    assert normalize_trigger("tag:backfill") == TTrigger("tag:backfill")
    assert normalize_trigger("http:") == TTrigger("http:")


def test_normalize_trigger_bare_type() -> None:
    """normalize_trigger handles bare type names that take no expression."""
    assert normalize_trigger("deployment") == TTrigger("deployment:")
    assert normalize_trigger("manual") == TTrigger("manual:")
    assert normalize_trigger("http") == TTrigger("http:")
    assert normalize_trigger("webhook") == TTrigger("webhook:")


def test_normalize_trigger_bare_cron() -> None:
    """normalize_trigger detects bare cron expressions and wraps as schedule."""
    assert normalize_trigger("0 8 * * *") == TTrigger("schedule:0 8 * * *")
    assert normalize_trigger("*/5 * * * *") == TTrigger("schedule:*/5 * * * *")
    assert normalize_trigger("0 0 1 * *") == TTrigger("schedule:0 0 1 * *")


def test_normalize_trigger_invalid_bare_text() -> None:
    with pytest.raises(ValueError, match="cannot normalize"):
        normalize_trigger("not a trigger at all")


def test_normalize_none() -> None:
    assert normalize_triggers(None) == []


def test_normalize_single_string() -> None:
    result = normalize_triggers("schedule:0 8 * * *")
    assert result == [TTrigger("schedule:0 8 * * *")]


def test_normalize_list() -> None:
    result = normalize_triggers(["schedule:0 8 * * *", "tag:backfill"])
    assert result == [TTrigger("schedule:0 8 * * *"), TTrigger("tag:backfill")]


def test_normalize_triggers_normalizes() -> None:
    """normalize_triggers normalizes each item via normalize_trigger."""
    result = normalize_triggers(["0 8 * * *", "tag:backfill"])
    assert result == [TTrigger("schedule:0 8 * * *"), TTrigger("tag:backfill")]


def test_normalize_tuple_from_completed() -> None:
    """The .completed property returns a tuple of (success, fail)."""
    completed = (TTrigger("job.success:jobs.a.b"), TTrigger("job.fail:jobs.a.b"))
    result = normalize_triggers(completed)
    assert len(result) == 2
    assert result[0] == TTrigger("job.success:jobs.a.b")
    assert result[1] == TTrigger("job.fail:jobs.a.b")


def _job(
    ref: str,
    triggers: List[str],
    job_type: str = "batch",
    manual: bool = True,
) -> TJobDefinition:
    """Helper to build job defs for selector tests.

    Adds `manual:{ref}` trigger by default, matching `generate_manifest` behavior.
    """
    trigger_list = [TTrigger(t) for t in triggers]
    if manual:
        trigger_list.append(TTrigger(f"manual:{ref}"))
    return {
        "job_ref": TJobRef(ref),
        "entry_point": TEntryPoint(module="m", function="f", job_type=job_type),  # type: ignore[typeddict-item]
        "triggers": trigger_list,
        "execution": TExecutionSpec(),
        "starred": False,
    }


@pytest.mark.parametrize(
    "selector,job_triggers,expected",
    [
        # type shorthand: bare name, with colon, with star
        ("tag", ["tag:backfill"], True),
        ("tag:", ["tag:backfill"], True),
        ("tag:*", ["tag:backfill"], True),
        ("http", ["http:"], True),
        ("http:", ["http:"], True),
        ("http:*", ["http:"], True),
        ("schedule", ["schedule:0 8 * * *"], True),
        ("schedule:", ["schedule:0 8 * * *"], True),
        ("deployment", ["deployment:"], True),
        # type mismatch
        ("http", ["tag:backfill"], False),
        ("tag", ["http:"], False),
        ("schedule", ["tag:foo"], False),
        # exact match
        ("tag:backfill", ["tag:backfill"], True),
        ("tag:backfill", ["tag:deploy"], False),
        ("tag:backfill", ["tag:backfill", "schedule:0 8 * * *"], True),
        # glob match on expression
        ("tag:back*", ["tag:backfill"], True),
        ("tag:back*", ["tag:deploy"], False),
        ("schedule:0 8 *", ["schedule:0 8 * * *"], True),
        # no triggers
        ("tag", [], False),
        ("http", [], False),
    ],
    ids=[
        "type-bare",
        "type-colon",
        "type-star",
        "http-bare",
        "http-colon",
        "http-star",
        "schedule-bare",
        "schedule-colon",
        "deployment-bare",
        "type-mismatch-http-tag",
        "type-mismatch-tag-http",
        "type-mismatch-schedule-tag",
        "exact-match",
        "exact-no-match",
        "exact-multi-trigger",
        "glob-match",
        "glob-no-match",
        "glob-schedule",
        "no-triggers-tag",
        "no-triggers-http",
    ],
)
def test_selector_trigger_matching(selector: str, job_triggers: List[str], expected: bool) -> None:
    j = _job("jobs.mod.test", job_triggers)
    assert matches_selector(selector, j) == expected


@pytest.mark.parametrize(
    "selector,job_type,expected",
    [
        ("batch", "batch", True),
        ("batch", "interactive", False),
        ("interactive", "interactive", True),
        ("interactive", "batch", False),
        ("stream", "stream", True),
        ("stream", "batch", False),
        ("job", "batch", True),
        ("job", "interactive", False),
    ],
    ids=[
        "batch-batch",
        "batch-interactive",
        "interactive-interactive",
        "interactive-batch",
        "stream-stream",
        "stream-batch",
        "job-alias-batch",
        "job-alias-interactive",
    ],
)
def test_selector_job_type(selector: str, job_type: str, expected: bool) -> None:
    j = _job("jobs.mod.test", ["tag:foo"], job_type=job_type)
    assert matches_selector(selector, j) == expected


@pytest.mark.parametrize(
    "pattern,job_ref,expected",
    [
        ("jobs.batch_jobs.*", "jobs.batch_jobs.backfill", True),
        ("jobs.batch_jobs.backfill", "jobs.batch_jobs.backfill", True),
        ("jobs.batch_jobs.*", "jobs.other.transform", False),
        ("*backfill", "jobs.batch_jobs.backfill", True),
        ("*", "jobs.anything.here", True),
        ("jobs.mod.specific", "jobs.mod.specific", True),
        ("jobs.mod.specific", "jobs.mod.other", False),
    ],
    ids=[
        "wildcard-section",
        "exact-ref",
        "no-match-section",
        "suffix-glob",
        "match-all",
        "exact-full",
        "exact-no-match",
    ],
)
def test_selector_manual(pattern: str, job_ref: str, expected: bool) -> None:
    j = _job(job_ref, [])
    assert matches_selector(f"manual:{pattern}", j) == expected


def test_filter_jobs_no_selectors() -> None:
    """No selectors returns all jobs."""
    jobs = [_job("jobs.a", ["tag:x"]), _job("jobs.b", ["http:"])]
    assert len(filter_jobs_by_selectors(jobs, [])) == 2


def test_filter_jobs_single_selector() -> None:
    jobs = [
        _job("jobs.a", ["tag:backfill"]),
        _job("jobs.b", ["http:"]),
        _job("jobs.c", ["schedule:0 8 * * *"]),
    ]
    filtered = filter_jobs_by_selectors(jobs, ["tag"])
    assert len(filtered) == 1
    assert filtered[0]["job_ref"] == "jobs.a"


def test_filter_jobs_multiple_selectors() -> None:
    jobs = [
        _job("jobs.a", ["tag:backfill"]),
        _job("jobs.b", ["http:"], job_type="interactive"),
        _job("jobs.c", ["schedule:0 8 * * *"]),
    ]
    filtered = filter_jobs_by_selectors(jobs, ["tag", "interactive"])
    assert len(filtered) == 2
    refs = {j["job_ref"] for j in filtered}
    assert refs == {"jobs.a", "jobs.b"}


def test_filter_jobs_excludes_event_triggered() -> None:
    """Event-triggered jobs are NOT included by filter_jobs_by_selectors.
    The runner handles event trigger inclusion separately.
    """
    jobs = [
        _job("jobs.a", ["tag:backfill"]),
        _job("jobs.b", ["job.success:jobs.a"]),
        _job("jobs.c", ["schedule:0 8 * * *"]),
    ]
    filtered = filter_jobs_by_selectors(jobs, ["tag:*"])
    refs = {j["job_ref"] for j in filtered}
    assert refs == {"jobs.a"}


def test_filter_jobs_manual_selector() -> None:
    jobs = [
        _job("jobs.batch_jobs.backfill", []),
        _job("jobs.batch_jobs.transform", []),
        _job("jobs.other.cleanup", []),
    ]
    filtered = filter_jobs_by_selectors(jobs, ["manual:jobs.batch_jobs.*"])
    assert len(filtered) == 2
    assert all("batch_jobs" in j["job_ref"] for j in filtered)
