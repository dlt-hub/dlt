"""Tests for structured job results and their delivery to the dlthub beacon."""

import json as pyjson
from typing import Any, Dict, List, Mapping, Optional, Tuple, cast

import pytest

from dlt._workspace.deployment._job_ref import job_category
from dlt._workspace.deployment._run_views import print_job_result
from dlt._workspace.deployment.exceptions import InvalidJobResultType
from dlt._workspace.deployment.job_result import (
    job_result,
    parse_result_type,
    result_type,
    running_job,
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

from tests.workspace.utils import beacon as beacon, drain_beacon, isolated_workspace

WORKSPACE = "tests.workspace.cases.workspaces.agent_workspace"

AGENT_RESULT: TAgentJobResult = {
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

PLAIN_RESULT: TJobResult = {
    "type": "job.batch.etl_summary",
    "engine_version": 1,
    "result": {"rows": 10},
}


def _entry(function: str) -> TRuntimeEntryPoint:
    module = f"{WORKSPACE}.agent_batch_jobs"
    return {
        "module": module,
        "function": function,
        "job_type": "batch",
        "launcher": LAUNCHER_JOB,
        "job_ref": TJobRef(f"jobs.agent_batch_jobs.{function}"),
    }


def test_top_level_job_owns_the_run_result() -> None:
    """`run.result` is a pass-through, recorded once for the job the launcher invoked."""
    payload = {"from": "outer"}
    with running_job(TJobRef("jobs.x.outer")):
        assert job_result(payload, type="outer") is payload
        # a job called as a plain function by another job does not overwrite the run result
        with running_job(TJobRef("jobs.x.inner")):
            assert job_result({"from": "inner"}, type="inner") == {"from": "inner"}
            set_job_result({"type": "inner", "engine_version": 1})
        declared = take_job_result(TJobRef("jobs.x.outer"), "batch")
    # the job named the payload, taking it stamps the launcher's category and the job ref
    assert declared == {
        "type": "job.batch.outer",
        "engine_version": JOB_RESULT_ENGINE_VERSION,
        "result": payload,
        "job_ref": "jobs.x.outer",
    }
    # a second take finds nothing, so one run delivers at most one result
    assert take_job_result(TJobRef("jobs.x.outer"), "batch") is None
    # outside a job the payload still comes back, it is simply not recorded
    assert job_result(payload, type="outer") is payload
    assert take_job_result(TJobRef("jobs.x.outer"), "batch") is None


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


@pytest.mark.parametrize(
    "expose,deliver,job_type,expected",
    [
        ({"category": "background_agent"}, None, "batch", "background_agent"),
        ({}, {"pipeline_name": "p"}, "batch", "pipeline"),
        (None, None, "interactive", "interactive"),
        ({"category": "dashboard"}, {"pipeline_name": "p"}, "batch", "dashboard"),
    ],
    ids=["expose-category", "delivering-job", "job-type", "category-over-pipeline"],
)
def test_job_category_is_one_rule(
    expose: Optional[Mapping[str, Any]],
    deliver: Optional[Mapping[str, Any]],
    job_type: str,
    expected: str,
) -> None:
    """`expose.category`, else `pipeline` for a delivering job, else `job_type`."""
    assert job_category(expose, deliver, job_type) == expected


def test_launcher_delivers_the_result_to_the_beacon(beacon: List[Tuple[str, str]]) -> None:
    """The job names the payload, the launcher names the envelope and sends it once configured."""
    with isolated_workspace("agent_workspace") as ctx:
        # without `dlthub_dsn` the result comes back and nothing is sent
        result = job_run(_entry("daily_ingest"), run_id="r-1", trigger="manual:")
        assert result["type"] == "job.batch.etl_summary"
        assert result["result"] == {"rows": 10}
        assert "object" not in result
        assert beacon == []

        ctx.runtime_config.dlthub_dsn = "https://beacon.example/token"
        # a job that declares no result returns what it returned, and sends nothing
        assert job_run(_entry("transform"), run_id="r-2", trigger="manual:") == "transformed"
        job_run(_entry("daily_ingest"), run_id="r-3", trigger="manual:")
        drain_beacon()

    assert len(beacon) == 1
    url, data = beacon[0]
    assert url.endswith(f"/{JOB_RESULT_PAYLOAD_TYPE}")
    body: Dict[str, Any] = pyjson.loads(data)
    # the beacon derives run identity from the DSN token, so the body must not carry it
    assert "run_id" not in body
    # job_ref is what the beacon dedups on
    assert body["job_ref"] == "jobs.agent_batch_jobs.daily_ingest"
    assert body["type"] == "job.batch.etl_summary"
    assert body["result"] == {"rows": 10}


@pytest.mark.parametrize(
    "result,shown,hidden",
    [
        (
            AGENT_RESULT,
            [
                "job.background_agent.dlthub-platform:job-inspector",
                "succeeded",
                "found the cause",
                "job-run: job-run/r-9",
                "pydantic-ai on claude-sonnet-5, 3 turns, 165 tokens",
                "local tools: Grep (read), Bash (execute)",
                # the payload is pretty-printed rather than dumped as a repr
                '"classification": "code"',
            ],
            [],
        ),
        # nothing agent-specific leaks into a plain job's result
        (PLAIN_RESULT, ["job.batch.etl_summary", '"rows": 10'], ["status", "loop:"]),
    ],
    ids=["agent", "plain"],
)
def test_print_job_result(
    result: TJobResult, shown: List[str], hidden: List[str], capsys: pytest.CaptureFixture[str]
) -> None:
    """`dlthub local run` shows the payload; agent runs also show status, summary and loop."""
    print_job_result(result)
    out = capsys.readouterr().out
    for text in shown:
        assert text in out, text
    for text in hidden:
        assert text not in out, text
