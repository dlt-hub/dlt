"""Tests for the agent launcher, mirroring `test_launchers.py` for the job launcher.

Every test selects the loop through configuration, which is how a user switches a job to
another loop; parametrizing `loop_type` is what adds a real loop to the same coverage.
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone  # noqa: I251
from typing import Any, Dict, Iterator, List

import pytest

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace.deployment._run_helpers import build_runtime_entry_point
from dlt._workspace.deployment.exceptions import JobResolutionError
from dlt._workspace.deployment.launchers import LAUNCHER_AGENT
from dlt._workspace.deployment.launchers.agent import run as agent_run
from dlt._workspace.deployment.typing import TInstallSpec, TJobRef, TRuntimeEntryPoint

from tests.workspace.utils import importable_workspace

WORKSPACE_MODULES = (
    "__deployment__",
    "agent_jobs",
    "agent_batch_jobs",
    "agent_launcher_jobs",
    "mock_loop",
)
JOBS_MODULE = "agent_launcher_jobs"
_DLT_SPEC: TInstallSpec = {"name": "dlt", "extras": [], "version": "1.29.0", "mode": "pypi"}


@pytest.fixture(params=["mock-loop"], ids=["mock"])
def loop_type(request: pytest.FixtureRequest) -> str:
    """Loop the launcher runs the job on."""
    return request.param


@pytest.fixture
def workspace() -> Iterator[Any]:
    with importable_workspace("agent_workspace", *WORKSPACE_MODULES) as ctx:
        yield ctx


def _entry(
    function: str, loop_type: str, agent_config: Dict[str, str] = None, **config: str
) -> TRuntimeEntryPoint:
    """Entry point whose `config` carries `-c` values, with the loop in the agent section."""
    ep: TRuntimeEntryPoint = {
        "module": JOBS_MODULE,
        "function": function,
        "job_type": "batch",
        "launcher": LAUNCHER_AGENT,
        "job_ref": TJobRef(f"jobs.{JOBS_MODULE}.{function}"),
    }
    ep["config"] = {"agent": {"loop": loop_type, **(agent_config or {})}, **config}
    return ep


def test_agent_launcher_runs_a_declared_agent(workspace: Any, loop_type: str) -> None:
    """The launcher drives the loop and returns the agent job output."""
    output = agent_run(_entry("inspector", loop_type), run_id="a-1", trigger="manual:")

    # the category says which envelope this is, the name which agent produced the payload
    assert output["type"] == "job.background_agent.dlthub-platform:job-inspector"
    assert "agent" not in output
    assert output["status"] == "succeeded"
    assert output["job_ref"] == "jobs.agent_launcher_jobs.mock_inspector"
    assert output["trace"]["loop_type"] == loop_type
    # the decorator's instructions open the run as its user turn
    assert output["result"]["ran"]["user_turn"] == "explain the failure"


def test_agent_launcher_run_context_injection(workspace: Any, loop_type: str) -> None:
    """A decorated function receives run_id and trigger, like any other job."""
    ep = _entry("context_aware", loop_type)
    ep["run_args"] = {"depth": 2}  # type: ignore[typeddict-unknown-key]
    result = agent_run(ep, run_id="a-ctx", trigger="job.fail:jobs.b.ingest")

    assert result["run_id"] == "a-ctx"
    assert result["trigger"] == "job.fail:jobs.b.ingest"
    assert result["run_args"] == {"depth": 2}


def test_agent_launcher_run_context_not_injected(workspace: Any, loop_type: str) -> None:
    """A function that does not declare `run_context` is called without it."""
    assert (
        agent_run(_entry("no_context", loop_type), run_id="a-2", trigger="manual:")
        == "no_context_ok"
    )


def test_agent_launcher_awaits_coroutines(workspace: Any, loop_type: str) -> None:
    assert agent_run(_entry("async_job", loop_type), run_id="a-3", trigger="manual:") == "async_ok"


def test_agent_launcher_interval_injection(workspace: Any, loop_type: str) -> None:
    """Interval parsing is the job launcher's, so an agent job gets it too."""
    ep = _entry("context_aware", loop_type)
    ep["interval_start"] = "2024-01-15T00:00:00Z"
    ep["interval_end"] = "2024-01-16T00:00:00Z"
    result = agent_run(ep, run_id="a-iv", trigger="schedule:0 0 * * *")

    assert result["interval_start"] == datetime(2024, 1, 15, tzinfo=timezone.utc)


def test_agent_launcher_profile_injection(workspace: Any, loop_type: str) -> None:
    """The profile the runtime picked reaches the loop, which hands it to the agent."""
    old = os.environ.pop(WORKSPACE__PROFILE, None)
    try:
        ep = _entry("inspector", loop_type)
        ep["profile"] = "access"
        output = agent_run(ep, run_id="a-prof", trigger="manual:")
        assert output["result"]["ran"]["profile"] == "access"
        assert os.environ[WORKSPACE__PROFILE] == "access"
    finally:
        if old is not None:
            os.environ[WORKSPACE__PROFILE] = old
        else:
            os.environ.pop(WORKSPACE__PROFILE, None)


def test_agent_launcher_with_config(workspace: Any, loop_type: str) -> None:
    """`entry_point.config` reaches `AgentConfiguration`, which outranks the declaration."""
    ep = _entry("inspector", loop_type, {"model": "opus", "max_turns": "11"})
    output = agent_run(ep, run_id="a-cfg", trigger="manual:")

    ran = output["result"]["ran"]
    assert ran["model"] == "anthropic:claude-opus-5"
    assert ran["max_turns"] == 11
    assert output["trace"]["model"] == "anthropic:claude-opus-5"


def test_agent_verbosity_comes_from_config(workspace: Any, loop_type: str) -> None:
    """`-c agent.verbosity=2` decides how much of the run reaches the console."""
    ep = _entry("inspector", loop_type, {"verbosity": "2"})
    output = agent_run(ep, run_id="a-verbose", trigger="manual:")

    assert output["result"]["ran"]["verbosity"] == 2


def test_agent_launcher_unknown_loop(workspace: Any) -> None:
    from dlt._workspace.deployment.agent.exceptions import UnknownAgentLoop

    with pytest.raises(UnknownAgentLoop):
        agent_run(_entry("inspector", "no-such-loop"), run_id="a-4", trigger="manual:")


def test_agent_launcher_function_not_found(workspace: Any, loop_type: str) -> None:
    with pytest.raises(JobResolutionError):
        agent_run(_entry("nonexistent", loop_type), run_id="a-5", trigger="manual:")


def test_agent_launcher_module_not_found(loop_type: str) -> None:
    ep = _entry("inspector", loop_type)
    ep["module"] = "no.such.module"
    with pytest.raises(JobResolutionError):
        agent_run(ep, run_id="a-6", trigger="manual:")


def test_agent_launcher_requires_function(loop_type: str) -> None:
    ep = _entry("inspector", loop_type)
    ep["function"] = None
    with pytest.raises(JobResolutionError, match="entry_point.function"):
        agent_run(ep, run_id="a-7", trigger="manual:")


def test_agent_launcher_rejects_a_plain_job(workspace: Any, loop_type: str) -> None:
    """The agent launcher is only for `run.agent` jobs."""
    ep = _entry("transform", loop_type)
    ep["module"] = "agent_batch_jobs"
    with pytest.raises(JobResolutionError, match="AgentJobFactory"):
        agent_run(ep, run_id="a-8", trigger="manual:")


def test_agent_launcher_via_cli(workspace: Any, loop_type: str) -> None:
    """`python -m` on the agent launcher prints the rendered job output."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dlt._workspace.deployment.launchers.agent",
            "--run-id",
            "a-cli",
            "--trigger",
            "manual:",
            "--entry-point",
            json.dumps(_entry("inspector", loop_type)),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=workspace.run_dir,
    )
    assert result.returncode == 0, result.stderr
    assert "job.background_agent." in result.stdout
    assert "succeeded" in result.stdout
    assert "mock run" in result.stdout


def test_agent_launcher_cli_error_exit_code(workspace: Any, loop_type: str) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dlt._workspace.deployment.launchers.agent",
            "--run-id",
            "a-cli-err",
            "--trigger",
            "manual:",
            "--entry-point",
            json.dumps(_entry("nonexistent", loop_type)),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=workspace.run_dir,
    )
    assert result.returncode != 0


def test_decorator_to_launcher_e2e(workspace: Any, loop_type: str) -> None:
    """The manifest's own entry point runs: what `dlthub local run` hands the launcher."""
    import agent_launcher_jobs  # type: ignore[import-not-found]

    # the manifest generator stamps the module a declared agent was found in
    agent_launcher_jobs.inspector.declare(JOBS_MODULE, "inspector")
    job_def = agent_launcher_jobs.inspector.to_job_definition()
    assert job_def["expose"]["category"] == "background_agent"

    ep = build_runtime_entry_point(
        job_def,
        cli_config={"agent": {"loop": loop_type}},
        profile="access",
        refresh=False,
        interval_start=datetime(2024, 1, 15, tzinfo=timezone.utc),
        interval_end=datetime(2024, 1, 16, tzinfo=timezone.utc),
        dlt_version=_DLT_SPEC,
        tz="UTC",
    )
    output = agent_run(ep, run_id="a-e2e", trigger="manual:")

    assert output["status"] == "succeeded"
    assert output["result"]["ran"]["profile"] == "access"
    assert output["trace"]["agent"] == "dlthub-platform:job-inspector"


def test_declared_agent_runs_as_a_plain_call(workspace: Any, loop_type: str) -> None:
    """Calling the factory outside a launcher runs the same agent."""
    env_key = "JOBS__AGENT_LAUNCHER_JOBS__MOCK_INSPECTOR__AGENT__LOOP"
    os.environ[env_key] = loop_type
    try:
        import agent_launcher_jobs

        output: Dict[str, Any] = agent_launcher_jobs.inspector(failed_job_ref="jobs.b.ingest")
    finally:
        os.environ.pop(env_key, None)

    assert output["status"] == "succeeded"
    assert output["result"]["ran"]["run_context"]["run_id"] == "local"


def test_declared_inputs_come_from_config(workspace: Any, loop_type: str) -> None:
    """`-c failed_run_id=...` fills a declared input, as it fills a function job's argument."""
    ep = _entry("inspector", loop_type, failed_run_id="r-42")
    output = agent_run(ep, run_id="a-in", trigger="manual:")

    inputs = output["trace"]["inputs"]
    assert inputs["failed_run_id"] == "r-42"
    # a declared input nobody supplied stays absent, and renders blank in the prompt
    assert "failed_job_ref" not in inputs


def test_instructions_are_the_user_turn(workspace: Any, loop_type: str) -> None:
    """`agent.instructions` is what a person tells the agent to do, and configuration wins."""
    ep = _entry("inspector", loop_type, {"instructions": "focus on the loader step"})
    output = agent_run(ep, run_id="a-instr", trigger="manual:")

    ran = output["result"]["ran"]
    # the decorator declared "explain the failure"; this run says otherwise
    assert ran["user_turn"] == "focus on the loader step"
    assert output["trace"]["instructions"] == "focus on the loader step"
    # and the task itself stays in the system prompt, rendered against the inputs
    assert "You are a job inspector" in ran["system_prompt"]


def test_the_body_carries_the_inputs_to_the_model(workspace: Any, loop_type: str) -> None:
    """Placeholders in the body are substituted with what configuration and the trigger gave."""
    ep = _entry("inspector", loop_type, failed_run_id="r-42")
    output = agent_run(ep, run_id="a-body", trigger="job.fail:jobs.b.ingest")

    system_prompt = output["result"]["ran"]["system_prompt"]
    assert "with failed run id 'r-42'" in system_prompt
    assert "from trigger `job.fail:jobs.b.ingest`" in system_prompt
    # an input nobody supplied renders blank, and the trace says which
    assert "job_ref ''" in system_prompt
    assert output["trace"]["unresolved_placeholders"] == ["failed_job_ref"]


def test_run_args_fill_declared_inputs(workspace: Any, loop_type: str) -> None:
    ep = _entry("inspector", loop_type, failed_run_id="r-42")
    ep["run_args"] = {"failed_run_id": "r-99"}  # type: ignore[typeddict-unknown-key]
    output = agent_run(ep, run_id="a-in2", trigger="job.fail:jobs.b.ingest")

    assert output["trace"]["inputs"]["failed_run_id"] == "r-99"


@pytest.mark.parametrize(
    "pairs,expected",
    [
        (["failed_run_id=r-42"], {"failed_run_id": "r-42"}),
        # a dotted key addresses a config section, as it does everywhere else in dlt
        (["agent.model=opus"], {"agent": {"model": "opus"}}),
        (
            ["agent.model=opus", "agent.max_turns=11", "failed_run_id=r-42"],
            {"agent": {"model": "opus", "max_turns": "11"}, "failed_run_id": "r-42"},
        ),
        (["url=https://example.com/a=b"], {"url": "https://example.com/a=b"}),
    ],
    ids=["flat", "sectioned", "mixed", "value-with-equals"],
)
def test_cli_config_args_nest_on_dots(pairs: List[str], expected: Dict[str, Any]) -> None:
    from dlt._workspace.cli.dlthub._local_workspace_command import _parse_config_args

    assert _parse_config_args(pairs) == expected


def test_cli_config_reaches_the_agent_configuration(workspace: Any, loop_type: str) -> None:
    """`-c agent.model=opus` must land as `JOBS__..__AGENT__MODEL`, not a dotted env name."""
    from dlt._workspace.cli.dlthub._local_workspace_command import _parse_config_args

    ep = _entry("inspector", loop_type)
    ep["config"].update(_parse_config_args(["agent.model=opus", "failed_run_id=r-7"]))
    output = agent_run(ep, run_id="a-cli-cfg", trigger="manual:")

    assert output["result"]["ran"]["model"] == "anthropic:claude-opus-5"
    assert output["trace"]["inputs"]["failed_run_id"] == "r-7"
    # the input is declared `entity_type: job-run`, so the run says which run it acted on
    assert output["object"] == [{"type": "job-run", "id": "job-run/r-7"}]


def test_function_form_drives_the_loop_the_launcher_built(workspace: Any, loop_type: str) -> None:
    """`@run.agent(agent=...)` names the agent, so the function gets a loop in its run context."""
    output = agent_run(_entry("driver", loop_type), run_id="a-drv", trigger="manual:")

    # a function declares its own agent, so the job ref identifies it; a name may carry dots
    assert output["type"] == "job.background_agent.jobs.agent_launcher_jobs.driver"
    assert output["result"]["ran"]["agent"] == "driver"
    assert output["trace"]["inputs"] == {"failed_run_id": "r-driven"}


def test_agent_declared_inline_runs_like_a_referenced_one(workspace: Any, loop_type: str) -> None:
    ep = _entry("inline", loop_type, failed_run_id="r-inline", depth="3")
    output = agent_run(ep, run_id="a-inline", trigger="manual:")

    assert output["type"] == "job.background_agent.inline-inspector"
    inputs = output["trace"]["inputs"]
    # the declared type is applied, so `depth` arrives as an int
    assert (inputs["failed_run_id"], inputs["depth"]) == ("r-inline", 3)


def test_required_input_missing_from_config_fails_the_run(workspace: Any, loop_type: str) -> None:
    """A required input is a required job argument: the run fails instead of prompting blank."""
    from dlt.common.configuration.exceptions import ConfigFieldMissingException

    with pytest.raises(ConfigFieldMissingException, match="failed_run_id"):
        agent_run(_entry("inline", loop_type), run_id="a-req", trigger="manual:")


def test_agent_declared_by_a_python_function(workspace: Any, loop_type: str) -> None:
    """The signature is the inputs, the return type the output, the docstring the system prompt."""
    ep = _entry("python_inspector", loop_type, failed_run_id="r-77")
    ep["run_args"] = {"depth": 5}  # type: ignore[typeddict-unknown-key]
    output = agent_run(ep, run_id="a-py", trigger="manual:")

    assert output["type"] == "job.background_agent.jobs.agent_launcher_jobs.python_inspector"
    # config filled the parameter, the trigger's run args filled the other
    assert output["trace"]["inputs"] == {"failed_run_id": "r-77", "depth": 5}
    ran = output["result"]["ran"]
    assert ran["agent"] == "python_inspector"
    assert ran["model"] == "anthropic:claude-haiku-4-5"


def test_python_agent_reaches_the_manifest(workspace: Any) -> None:
    import agent_launcher_jobs

    agent_launcher_jobs.python_inspector.declare(JOBS_MODULE, "python_inspector")
    job_def = agent_launcher_jobs.python_inspector.to_job_definition()
    declaration = job_def["agent"]

    assert declaration["name"] == "python_inspector"
    assert declaration["description"] == "Inspects a failed run without any AGENT.md."
    assert declaration["agent_file"] == "agent_launcher_jobs.py:python_inspector"
    assert declaration["tools"] == ["telemetry"]
    # what the job may touch belongs to the job, not to the agent it runs
    assert job_def["access"] == {"data": ["read"], "context": ["read"]}
    assert "access" not in declaration
    # the signature is the job configuration, exactly as a function job's parameters are
    assert {"failed_run_id", "depth"} <= set(job_def["config_keys"])

    # what the job takes and returns belongs to the job too, at the same resolution as config_keys
    assert "inputs" not in declaration
    assert "output" not in declaration
    inputs = job_def["inputs"]
    assert set(inputs["properties"]) == set(job_def["config_keys"]) == {"failed_run_id", "depth"}
    assert inputs["required"] == ["failed_run_id"]
    # the docstring is the system prompt, and the manifest never carries it
    assert "system_prompt" not in declaration

    output = job_def["output"]
    # status and summary are inherited; the launcher's own keys never reach the model
    assert set(output["properties"]) == {
        "status",
        "summary",
        "classification",
        "confidence",
        "evidence",
    }
    assert output["properties"]["status"]["enum"] == ["succeeded", "failed", "aborted"]
    assert set(output["required"]) == {"status", "summary", "classification", "confidence"}


def test_agent_with_no_inputs(workspace: Any, loop_type: str) -> None:
    """The minimum an agent needs: a docstring and a result. `loop.run()` takes nothing."""
    output = agent_run(_entry("sanity_check", loop_type), run_id="a-sanity", trigger="manual:")

    assert output["status"] == "succeeded"
    # a single verb is accepted where a list is
    assert output["trace"]["access"] == {"data": ["read"], "context": ["read"]}
    # nobody gave instructions, so the run opens with the go-signal
    assert output["trace"]["inputs"] == {}
    assert output["result"]["ran"]["user_turn"] == "Begin."
