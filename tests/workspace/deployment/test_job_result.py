"""Tests for structured job results and their delivery to the dlthub beacon."""

import json as pyjson
from typing import Any, Dict, Iterator, List, Tuple, cast
from unittest.mock import MagicMock, patch

import pytest

from dlt.pipeline import platform

from dlt._workspace.deployment._job_ref import job_category
from dlt._workspace.deployment.exceptions import InvalidJobResultType
from dlt._workspace.deployment.job_result import (
    job_result,
    parse_result_type,
    result_type,
    running_job,
    send_job_result,
    set_job_result,
    take_job_result,
)
from dlt._workspace.deployment.launchers import LAUNCHER_JOB
from dlt._workspace.deployment.launchers.job import run as job_run
from dlt._workspace.deployment.agent.typing import TAgentJobResult, TAgentTrace
from dlt._workspace.deployment.typing import (
    JOB_RESULT_ENGINE_VERSION,
    JOB_RESULT_PAYLOAD_TYPE,
    TJobRef,
    TJobResult,
    TRuntimeEntryPoint,
)

from tests.workspace.utils import isolated_workspace

WORKSPACE = "tests.workspace.cases.workspaces.agent_workspace"


def _entry(function: str) -> TRuntimeEntryPoint:
    module = f"{WORKSPACE}.agent_batch_jobs"
    return {
        "module": module,
        "function": function,
        "job_type": "batch",
        "launcher": LAUNCHER_JOB,
        "job_ref": TJobRef(f"jobs.agent_batch_jobs.{function}"),
    }


def _drain() -> None:
    assert platform._THREAD_POOL is not None
    platform._THREAD_POOL.thread_pool.shutdown(wait=True)


@pytest.fixture
def beacon() -> Iterator[List[Tuple[str, str]]]:
    """Capture beacon PUTs, with the fire-and-forget pool drained on exit."""
    sent: List[Tuple[str, str]] = []

    def _put(url: str, data: str) -> Any:
        sent.append((url, data))
        return MagicMock(status_code=200)

    platform._THREAD_POOL = None
    platform.init_platform_tracker()
    with patch.object(platform, "requests") as requests:
        requests.put.side_effect = _put
        yield sent
        _drain()
    platform._THREAD_POOL = None


def test_job_result_returns_payload_unchanged() -> None:
    """`run.result` is transparent, so a job's return type never depends on how it was run."""
    payload = {"rows": 10}
    with running_job(TJobRef("jobs.x.y")):
        assert job_result(payload, type="etl_summary") is payload
    # outside a job the payload still comes back, it is simply not recorded
    assert job_result(payload, type="etl_summary") is payload


def test_result_type_name_is_required() -> None:
    """A job must say what it returned; the launcher adds only the category."""
    with pytest.raises(TypeError):
        job_result({"a": 1})  # type: ignore[call-arg]


def test_only_top_level_job_declares_result() -> None:
    """A job called as a plain function by another job must not overwrite the run result."""
    with running_job(TJobRef("jobs.x.outer")):
        job_result({"from": "outer"}, type="outer")
        with running_job(TJobRef("jobs.x.inner")):
            job_result({"from": "inner"}, type="inner")
        declared = take_job_result(TJobRef("jobs.x.outer"), "batch")
    assert declared["result"] == {"from": "outer"}
    assert declared["type"] == "job.batch.outer"


def test_take_job_result_clears_and_stamps() -> None:
    with running_job(TJobRef("jobs.x.y")):
        job_result({"a": 1}, type="a")
    taken = take_job_result(TJobRef("jobs.x.y"), "batch")
    assert taken["job_ref"] == "jobs.x.y"
    assert taken["engine_version"] == JOB_RESULT_ENGINE_VERSION
    # a second take finds nothing, so one run delivers at most one result
    assert take_job_result(TJobRef("jobs.x.y"), "batch") is None


@pytest.mark.parametrize(
    "category,name",
    [
        ("background_agent", "dlthub-platform:job-inspector"),
        ("background_agent", "jobs.agents.check_toolkits"),
        ("pipeline", "load_info"),
        ("batch", "etl_summary"),
    ],
    ids=["agent-ref", "dotted-job-ref", "pipeline", "batch"],
)
def test_parse_result_type_survives_dots_in_the_name(category: str, name: str) -> None:
    """The first two segments are closed vocabularies, so the name may carry anything."""
    assert parse_result_type(result_type(category, name)) == (category, name)


@pytest.mark.parametrize("bad", ["etl_summary", "job.batch", "run.batch.x", "job..x", "job.batch."])
def test_parse_result_type_refuses_anything_else(bad: str) -> None:
    with pytest.raises(InvalidJobResultType):
        parse_result_type(bad)


def test_job_category_is_one_rule() -> None:
    """`expose.category`, else `pipeline` for a delivering job, else `job_type`."""
    assert job_category({"category": "background_agent"}, None, "batch") == "background_agent"
    assert job_category({}, {"pipeline_name": "p"}, "batch") == "pipeline"
    assert job_category(None, None, "interactive") == "interactive"
    assert job_category({"category": "dashboard"}, {"pipeline_name": "p"}, "batch") == "dashboard"


def test_send_job_result_without_beacon_is_noop() -> None:
    """No `dlthub_dsn` configured means nothing is sent and nothing raises."""
    result: TJobResult = {"type": "job.batch.job", "engine_version": 1, "result": {}}
    send_job_result(result)


def test_launcher_delivers_declared_result(beacon: List[Tuple[str, str]]) -> None:
    with isolated_workspace("agent_workspace"):
        result = job_run(_entry("daily_ingest"), run_id="r-1", trigger="manual:")
    # the job named the payload, the launcher named the envelope
    assert result["type"] == "job.batch.etl_summary"
    assert result["result"] == {"rows": 10}
    assert "object" not in result


def test_beacon_payload_shape(beacon: List[Tuple[str, str]]) -> None:
    """The beacon derives run identity from the DSN token, so the body must not carry it."""
    with isolated_workspace("agent_workspace") as ctx:
        ctx.runtime_config.dlthub_dsn = "https://beacon.example/token"
        job_run(_entry("daily_ingest"), run_id="r-2", trigger="manual:")
        _drain()

    assert len(beacon) == 1
    url, data = beacon[0]
    assert url.endswith(f"/{JOB_RESULT_PAYLOAD_TYPE}")
    body: Dict[str, Any] = pyjson.loads(data)
    assert "run_id" not in body
    # job_ref is what the beacon dedups on
    assert body["job_ref"] == "jobs.agent_batch_jobs.daily_ingest"
    assert body["type"] == "job.batch.etl_summary"
    assert body["result"] == {"rows": 10}


def test_job_without_result_sends_nothing(beacon: List[Tuple[str, str]]) -> None:
    with isolated_workspace("agent_workspace") as ctx:
        ctx.runtime_config.dlthub_dsn = "https://beacon.example/token"
        result = job_run(_entry("transform"), run_id="r-3", trigger="manual:")
        _drain()
    assert result == "transformed"
    assert beacon == []


def test_set_job_result_ignored_below_top_level() -> None:
    full: TJobResult = {"type": "x", "engine_version": 1}
    with running_job(TJobRef("jobs.x.outer")):
        with running_job(TJobRef("jobs.x.inner")):
            set_job_result(full)
        assert take_job_result(TJobRef("jobs.x.outer"), "batch") is None


def test_print_job_result_renders_payload_and_agent_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`dlthub local run` shows the payload; agent runs also show status, summary and loop."""
    from dlt._workspace.deployment._run_views import print_job_result

    agent_result: TAgentJobResult = {
        "type": "job.background_agent.dlthub-platform:job-inspector",
        "engine_version": 1,
        "status": "succeeded",
        "summary": "found the cause",
        "object": [{"type": "job-run", "id": "job-run/r-9"}],
        # the view reads a handful of trace fields, so a partial one exercises it
        "trace": cast(
            TAgentTrace,
            {
                "loop_type": "pydantic-ai",
                "model": "claude-sonnet-5",
                "turn_count": 3,
                "total_tokens": 165,
                "local_tools": {"Grep": "read", "Bash": "execute"},
            },
        ),
        "result": {"classification": "code"},
    }
    print_job_result(agent_result)
    out = capsys.readouterr().out
    assert "job.background_agent.dlthub-platform:job-inspector" in out
    assert "succeeded" in out and "found the cause" in out
    assert "job-run: job-run/r-9" in out
    assert "pydantic-ai on claude-sonnet-5, 3 turns, 165 tokens" in out
    assert "local tools: Grep (read), Bash (execute)" in out
    # the payload itself is pretty-printed rather than dumped as a repr
    assert '"classification": "code"' in out


def test_print_job_result_for_a_plain_job(capsys: pytest.CaptureFixture[str]) -> None:
    from dlt._workspace.deployment._run_views import print_job_result

    print_job_result({"type": "job.batch.etl_summary", "engine_version": 1, "result": {"rows": 10}})
    out = capsys.readouterr().out
    assert "job.batch.etl_summary" in out
    assert '"rows": 10' in out
    # nothing agent-specific leaks into a plain job's result
    assert "status" not in out and "loop:" not in out
