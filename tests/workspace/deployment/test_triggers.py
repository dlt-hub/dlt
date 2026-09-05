"""Tests for trigger parsing, normalization, and selectors."""

from typing import Any, List, Optional

import pytest

from dlt._workspace.deployment.exceptions import InvalidTrigger
from dlt._workspace.deployment._trigger_helpers import (
    is_selector,
    humanize_trigger,
    match_triggers_with_selectors,
    normalize_trigger,
    normalize_triggers,
    parse_trigger,
    pick_trigger,
)
from dlt._workspace.deployment.manifest import expand_trigger_selectors
from dlt._workspace.deployment.typing import (
    HttpTriggerInfo,
    TEntryPoint,
    TExecuteSpec,
    TJobDefinition,
    TJobRef,
    TParsedTrigger,
    TTrigger,
)
from dlt._workspace.deployment.trigger import pipeline_name


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
    with pytest.raises(InvalidTrigger):
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
    with pytest.raises(InvalidTrigger, match=error_fragment):
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
        ("unknown:foo", "unknown type"),
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
        # a bare name cannot be resolved; `section.name` and `jobs.section.name` both can
        ("job.success:not_a_ref", "section.name"),
        ("job.fail:bad_ref", "section.name"),
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
    with pytest.raises(InvalidTrigger, match=error_fragment):
        normalize_trigger(trigger)


def test_normalize_trigger_typed() -> None:
    """normalize_trigger parses type:expr triggers via their parser."""
    assert normalize_trigger("schedule:0 8 * * *") == TTrigger("schedule:0 8 * * *")
    assert normalize_trigger("tag:backfill") == TTrigger("tag:backfill")
    assert normalize_trigger("http:") == TTrigger("http:")


def test_normalize_trigger_bare_type() -> None:
    """normalize_trigger handles bare type names that take no expression."""
    assert normalize_trigger("deployment") == TTrigger("deployment:")
    assert normalize_trigger("http") == TTrigger("http:")
    assert normalize_trigger("webhook") == TTrigger("webhook:")
    # manual is blocked — users must use expose(manual=True)
    with pytest.raises(InvalidTrigger, match="added automatically"):
        normalize_trigger("manual")


def test_normalize_trigger_bare_cron() -> None:
    """normalize_trigger detects bare cron expressions and wraps as schedule."""
    assert normalize_trigger("0 8 * * *") == TTrigger("schedule:0 8 * * *")
    assert normalize_trigger("*/5 * * * *") == TTrigger("schedule:*/5 * * * *")
    assert normalize_trigger("0 0 1 * *") == TTrigger("schedule:0 0 1 * *")


def test_normalize_trigger_invalid_bare_text() -> None:
    with pytest.raises(InvalidTrigger, match="cannot normalize"):
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
        "entry_point": TEntryPoint(
            module="m",
            function="f",
            job_type=job_type,  # type: ignore[typeddict-item]
            launcher="dlt._workspace.deployment.launchers.job",
        ),
        "triggers": trigger_list,
        "execute": TExecuteSpec(),
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
    triggers = [TTrigger(t) for t in job_triggers]
    result = match_triggers_with_selectors("batch", triggers, [selector])
    assert bool(result) == expected


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
    triggers = [TTrigger("tag:foo")]
    result = match_triggers_with_selectors(job_type, triggers, [selector])
    assert bool(result) == expected


@pytest.mark.parametrize(
    "selectors,job_triggers,expected_matched",
    [
        # single selector matches single trigger
        (["tag:backfill"], ["tag:backfill", "manual:jobs.x"], ["tag:backfill"]),
        # single selector matches nothing
        (["tag:deploy"], ["tag:backfill", "manual:jobs.x"], []),
        # multiple selectors OR-ed, each matches its trigger
        (
            ["tag:*", "schedule:*"],
            ["tag:daily", "schedule:0 8 * * *", "manual:jobs.x"],
            ["tag:daily", "schedule:0 8 * * *"],
        ),
        # manual:* matches manual trigger only
        (["manual:*"], ["tag:daily", "manual:jobs.x"], ["manual:jobs.x"]),
        # schedule:* does NOT match manual
        (["schedule:*"], ["schedule:0 8 * * *", "manual:jobs.x"], ["schedule:0 8 * * *"]),
        # every:* matches every trigger only
        (["every:*"], ["every:1m", "manual:jobs.x"], ["every:1m"]),
        # glob on tag expression
        (["tag:back*"], ["tag:backfill", "tag:deploy"], ["tag:backfill"]),
        # http: matches http
        (["http:*"], ["http:", "manual:jobs.x"], ["http:"]),
        # empty selectors = empty result
        ([], ["tag:daily", "manual:jobs.x"], []),
        # job.success selector matches event triggers
        (["job.success:*"], ["job.success:jobs.a", "manual:jobs.x"], ["job.success:jobs.a"]),
        # manual glob with job ref
        (
            ["manual:jobs.mod.*"],
            ["manual:jobs.mod.backfill", "tag:daily"],
            ["manual:jobs.mod.backfill"],
        ),
    ],
    ids=[
        "single-tag-match",
        "single-tag-miss",
        "multi-selector-or",
        "manual-star",
        "schedule-not-manual",
        "every-not-manual",
        "glob-tag-expr",
        "http-match",
        "empty-selectors",
        "event-trigger-match",
        "manual-glob-ref",
    ],
)
def test_match_triggers_with_selectors(
    selectors: List[str],
    job_triggers: List[str],
    expected_matched: List[str],
) -> None:
    triggers = [TTrigger(t) for t in job_triggers]
    result = match_triggers_with_selectors("batch", triggers, selectors)
    assert result == [TTrigger(t) for t in expected_matched]


def test_match_triggers_job_type_returns_all() -> None:
    """Job type selector (batch, interactive) returns ALL triggers."""
    triggers = [TTrigger("tag:daily"), TTrigger("schedule:0 8 * * *")]
    result = match_triggers_with_selectors("batch", triggers, ["batch"])
    assert len(result) == 2


def test_match_triggers_deduplicates() -> None:
    """Overlapping selectors don't produce duplicate triggers."""
    triggers = [TTrigger("tag:backfill")]
    result = match_triggers_with_selectors("batch", triggers, ["tag:*", "tag:backfill"])
    assert result == [TTrigger("tag:backfill")]


def test_normalize_trigger_rejects_synthetic() -> None:
    """Users cannot create manual: or pipeline_name: triggers directly."""
    with pytest.raises(InvalidTrigger, match="added automatically"):
        normalize_trigger("manual:jobs.mod.x")
    with pytest.raises(InvalidTrigger, match="added automatically"):
        normalize_trigger("manual")
    with pytest.raises(InvalidTrigger, match="added automatically"):
        normalize_trigger("pipeline_name:analytics")


def test_parse_pipeline_name_trigger() -> None:
    parsed = parse_trigger(TTrigger("pipeline_name:my_pipeline"))
    assert parsed.type == "pipeline_name"
    assert parsed.expr == "my_pipeline"
    assert parsed.raw == TTrigger("pipeline_name:my_pipeline")


def test_pipeline_name_trigger_creator() -> None:
    assert pipeline_name("analytics") == TTrigger("pipeline_name:analytics")


@pytest.mark.parametrize(
    "matched,default,expected",
    [
        # default_trigger in matched list — preferred
        (
            ["tag:daily", "schedule:0 8 * * *"],
            "schedule:0 8 * * *",
            "schedule:0 8 * * *",
        ),
        # default_trigger not in matched — first wins
        (
            ["tag:daily", "manual:jobs.x"],
            "schedule:0 8 * * *",
            "tag:daily",
        ),
        # no default — first wins
        (["tag:daily", "manual:jobs.x"], None, "tag:daily"),
        # empty list — None
        ([], "schedule:0 8 * * *", None),
        # single match — returns it
        (["every:1h"], None, "every:1h"),
    ],
    ids=[
        "default-in-matched",
        "default-not-in-matched",
        "no-default",
        "empty-matched",
        "single-match",
    ],
)
def test_pick_trigger(matched: List[str], default: Optional[str], expected: Optional[str]) -> None:
    result = pick_trigger(
        [TTrigger(t) for t in matched],
        TTrigger(default) if default else None,
    )
    if expected is None:
        assert result is None
    else:
        assert result == TTrigger(expected)


@pytest.mark.parametrize(
    "trigger,expected",
    [
        ("schedule:0 2 * * *", "schedule: 0 2 * * *"),
        ("every:5m", "every 5m"),
        ("once:2026-03-15T08:00:00Z", "once at 2026-03-15T08:00:00Z"),
        ("manual:jobs.backfill", "manual (backfill)"),
        ("manual:jobs.mod.ingest", "manual (ingest)"),
        ("tag:nightly", "tag:nightly"),
        ("http:", "interactive service"),
        ("http:9090", "interactive service"),
        ("deployment:", "after deployment"),
        ("webhook:ingest", "webhook ingest"),
        ("webhook:", "webhook"),
        ("job.success:jobs.upstream", "after upstream succeeds"),
        ("job.fail:jobs.mod.upstream", "after upstream fails"),
        ("pipeline_name:fruitshop", "pipeline: fruitshop"),
        ("not-a-valid-trigger", "not-a-valid-trigger"),
    ],
    ids=[
        "schedule",
        "every",
        "once",
        "manual",
        "manual-qualified",
        "tag",
        "http-bare",
        "http-port",
        "deployment",
        "webhook",
        "webhook-bare",
        "job-success",
        "job-fail",
        "pipeline-name",
        "unparseable-falls-back-to-raw",
    ],
)
def test_humanize_trigger(trigger: str, expected: str) -> None:
    assert humanize_trigger(TTrigger(trigger)) == expected


@pytest.mark.parametrize(
    "trigger,expected",
    [
        ("job.fail:jobs.batch.daily", "job.fail:jobs.batch.daily"),
        ("job.fail:batch.daily", "job.fail:jobs.batch.daily"),
        # a selector stands as written; only a bare ref is resolved
        ("job.success:batch.*", "job.success:batch.*"),
        ("job.success:jobs.*", "job.success:jobs.*"),
        ("job.fail:*", "job.fail:*"),
        ("job.success:tag:ingest", "job.success:tag:ingest"),
        ("job.success:batch:", "job.success:batch:"),
    ],
    ids=["qualified", "section-name", "section-glob", "qualified-glob", "bare-star", "tag", "type"],
)
def test_normalize_job_event_trigger_refs(trigger: str, expected: str) -> None:
    assert normalize_trigger(trigger) == expected


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("tag:ingest", True),
        ("batch", True),
        ("batch:", True),
        ("jobs.batch.*", True),
        ("jobs.a.b?", True),
        ("*", True),
        ("jobs.a.b", False),
        ("ingest", False),
    ],
    ids=["tag", "type", "type-colon", "ref-glob", "question", "star", "ref", "name"],
)
def test_is_selector(expr: str, expected: bool) -> None:
    """A job ref never carries a colon or a glob character, so either one selects."""
    assert is_selector(expr) is expected


def _job_def(job_ref: str, triggers: List[str], job_type: str = "batch") -> TJobDefinition:
    return {
        "job_ref": TJobRef(job_ref),
        "entry_point": TEntryPoint(
            module="m", function="f", job_type=job_type, launcher="l"  # type: ignore[typeddict-item]
        ),
        "triggers": [TTrigger(t) for t in triggers],
        "execute": TExecuteSpec(),
    }


def test_expand_trigger_selectors() -> None:
    """A selector expands to every other job; only the declaring job is excluded."""
    watcher = _job_def("jobs.ops.inspector", ["job.fail:jobs.*"])
    other_watcher = _job_def("jobs.ops.auditor", ["job.success:jobs.*"])
    ingest = _job_def("jobs.batch.ingest", ["schedule:0 8 * * *"])
    transform = _job_def("jobs.batch.transform", [])
    dashboard = _job_def("jobs.workspace.dashboard", ["http:"], job_type="interactive")
    jobs = [watcher, other_watcher, ingest, transform, dashboard]

    assert expand_trigger_selectors(jobs) == []

    # never itself, never the interactive job; another watcher is a legitimate target
    assert watcher["triggers"] == [
        "job.fail:jobs.ops.auditor",
        "job.fail:jobs.batch.ingest",
        "job.fail:jobs.batch.transform",
    ]
    assert other_watcher["triggers"] == [
        "job.success:jobs.ops.inspector",
        "job.success:jobs.batch.ingest",
        "job.success:jobs.batch.transform",
    ]
    # non-selector triggers pass through untouched
    assert ingest["triggers"] == ["schedule:0 8 * * *"]


def test_expand_trigger_selectors_never_targets_the_declaring_job() -> None:
    """Self-exclusion is the whole loop guard: a job must not trigger itself."""
    watcher = _job_def("jobs.ops.only_job", ["job.fail:jobs.*"])
    jobs = [watcher]
    warnings = expand_trigger_selectors(jobs)
    assert watcher["triggers"] == []
    assert len(warnings) == 1


def test_expand_trigger_selectors_keeps_other_triggers_and_warns() -> None:
    watcher = _job_def("jobs.ops.inspector", ["schedule:0 7 * * *", "job.fail:jobs.nothing.*"])
    warnings = expand_trigger_selectors([watcher, _job_def("jobs.batch.ingest", [])])
    assert watcher["triggers"] == ["schedule:0 7 * * *"]
    assert len(warnings) == 1
    assert "matched no job" in warnings[0]


def test_expand_trigger_selectors_is_case_sensitive() -> None:
    """Manifests must not differ by platform, so matching never folds case."""
    watcher = _job_def("jobs.ops.inspector", ["job.fail:jobs.Batch.*"])
    jobs = [watcher, _job_def("jobs.batch.ingest", [])]
    warnings = expand_trigger_selectors(jobs)
    assert watcher["triggers"] == []
    assert len(warnings) == 1


def _tagged(job_ref: str, tags: List[str], job_type: str = "batch") -> TJobDefinition:
    job_def = _job_def(job_ref, [], job_type=job_type)
    job_def["expose"] = {"tags": tags}
    return job_def


def test_expand_selects_by_tag() -> None:
    """`tag:` reaches a job through the trigger its `expose.tags` synthesizes."""
    watcher = _job_def("jobs.ops.inspector", ["job.fail:tag:ingest"])
    ingest = _tagged("jobs.batch.ingest", ["ingest"])
    load = _tagged("jobs.batch.load", ["ingest", "nightly"])
    report = _tagged("jobs.batch.report", ["nightly"])

    assert expand_trigger_selectors([watcher, ingest, load, report]) == []
    assert watcher["triggers"] == ["job.fail:jobs.batch.ingest", "job.fail:jobs.batch.load"]


@pytest.mark.parametrize("selector", ["batch", "batch:"], ids=["keyword", "trailing-colon"])
def test_expand_selects_by_job_type(selector: str) -> None:
    """A job type selects the same way with or without the colon."""
    watcher = _job_def("jobs.ops.inspector", [f"job.success:{selector}"])
    ingest = _job_def("jobs.batch.ingest", [])
    dashboard = _job_def("jobs.ws.dashboard", ["http:"], job_type="interactive")

    assert expand_trigger_selectors([watcher, ingest, dashboard]) == []
    assert watcher["triggers"] == ["job.success:jobs.batch.ingest"]


@pytest.mark.parametrize(
    "selector", ["jobs.batch.*", "manual:jobs.batch.*"], ids=["ref-glob", "trigger-glob"]
)
def test_expand_selects_by_ref_or_by_manual_trigger(selector: str) -> None:
    """The ref and the synthetic `manual:` trigger reach the same jobs."""
    watcher = _job_def("jobs.ops.inspector", [f"job.fail:{selector}"])
    ingest = _job_def("jobs.batch.ingest", [])
    other = _job_def("jobs.ops.audit", [])

    assert expand_trigger_selectors([watcher, ingest, other]) == []
    assert watcher["triggers"] == ["job.fail:jobs.batch.ingest"]


def test_expand_leaves_an_exact_ref_alone() -> None:
    """The runtime looks a completion trigger up by exact string, so a ref must survive as one."""
    watcher = _job_def("jobs.ops.inspector", ["job.fail:jobs.batch.ingest"])
    expand_trigger_selectors([watcher, _job_def("jobs.batch.ingest", [])])
    assert watcher["triggers"] == ["job.fail:jobs.batch.ingest"]


@pytest.mark.parametrize(
    "selector,expected",
    [("batch:", ["jobs.batch.ingest"]), ("jobs.batch.*", ["jobs.batch.ingest"])],
    ids=["job-type-colon", "ref-glob"],
)
def test_cli_select_understands_the_same_selectors(selector: str, expected: List[str]) -> None:
    """`--select` shares the matcher, so both forms work there too."""
    from dlt._workspace.deployment._run_helpers import select_candidates

    manifest: Any = {
        "jobs": [_job_def("jobs.batch.ingest", []), _job_def("jobs.ops.audit", [], "interactive")]
    }
    assert [jd["job_ref"] for jd, _ in select_candidates(manifest, [selector])] == expected
