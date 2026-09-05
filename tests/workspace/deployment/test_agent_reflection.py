"""Tests for reading an agent declaration off a Python function."""

from typing import Any, Dict, List, Literal, Optional

import pytest

import dlt
from dlt.common.typing import Annotated, Doc, NotRequired, TypedDict

from dlt._workspace.deployment.agent.exceptions import InvalidAgentSpec
from dlt._workspace.deployment.agent.reflection import (
    agent_source,
    agent_spec_from_function,
    inputs_from_function,
    output_from_return,
)
from dlt._workspace.deployment.agent.typing import TAgentOutput
from dlt._workspace.deployment.typing import TJobRunContext

SOURCE = "/ws/jobs.py:inspector"


class Report(TAgentOutput):
    classification: Literal["config", "code"]
    confidence: str
    evidence: NotRequired[List[str]]


class NotAnAgentOutput(TypedDict):
    status: str
    summary: str


def inspector(
    failed_run_id: str = dlt.config.value,
    depth: int = 2,
    tags: Optional[List[str]] = None,
    run_context: TJobRunContext = None,
) -> Report:
    """Inspects a failed run and reports a diagnosis.

    You run unattended, seconds after a job failed.
    """


def test_inputs_come_from_the_signature() -> None:
    schema = inputs_from_function(inspector, SOURCE)

    assert set(schema["properties"]) == {"failed_run_id", "depth", "tags"}
    assert schema["properties"]["depth"] == {"default": 2, "title": "Depth", "type": "integer"}
    # `dlt.config.value` means required, resolved from configuration
    assert schema["required"] == ["failed_run_id"]
    assert "default" not in schema["properties"]["failed_run_id"]
    # the run context is implicit, and takes its definition with it
    assert "run_context" not in schema["properties"]
    assert "$defs" not in schema


def test_output_comes_from_the_return_type() -> None:
    schema = output_from_return(inspector, SOURCE)

    assert set(schema["properties"]) == {
        "status",
        "summary",
        "classification",
        "confidence",
        "evidence",
    }
    assert schema["properties"]["status"]["enum"] == ["succeeded", "failed", "aborted"]
    assert set(schema["required"]) == {"status", "summary", "classification", "confidence"}


@pytest.mark.parametrize(
    "annotation", [NotAnAgentOutput, Dict[str, Any]], ids=["plain-typeddict", "dict"]
)
def test_output_must_derive_from_the_agent_output(annotation: Any) -> None:
    def wrong() -> Any:
        pass

    wrong.__annotations__["return"] = annotation
    with pytest.raises(InvalidAgentSpec, match="TAgentOutput"):
        output_from_return(wrong, SOURCE)


def test_spec_assembled_from_the_function() -> None:
    spec = agent_spec_from_function(inspector, SOURCE, {"access": {"data": ["read"]}})

    assert spec["name"] == "inspector"
    assert spec["description"] == "Inspects a failed run and reports a diagnosis."
    # the whole docstring is the system prompt, its first line the description
    assert spec["system_prompt"].endswith("seconds after a job failed.")
    assert spec["access"] == {"data": ["read"]}


def test_decorator_and_function_override_the_referenced_agent() -> None:
    base: Any = {
        "name": "job-inspector",
        "description": "from the toolkit",
        "system_prompt": "toolkit body",
        "access": {"data": ["read", "write"]},
        "tools": ["telemetry"],
        "inputs": {"type": "object", "properties": {"other": {}}},
        "output": {"properties": {"status": {}, "summary": {}}},
        "defaults": {"model": "sonnet", "limits": {"max_turns": 30}},
    }
    spec = agent_spec_from_function(inspector, SOURCE, {"access": {"data": ["read"]}}, base)

    # decorator beats the toolkit
    assert spec["access"] == {"data": ["read"]}
    # the function beats it too, and what neither touches survives
    assert set(spec["inputs"]["properties"]) == {"failed_run_id", "depth", "tags"}
    assert spec["name"] == "inspector"
    assert spec["tools"] == ["telemetry"]
    assert spec["defaults"] == {"model": "sonnet", "limits": {"max_turns": 30}}


def test_agent_source_names_the_file_and_the_function() -> None:
    assert agent_source(inspector, "inspector").endswith("test_agent_reflection.py:inspector")


class BareOutput(TAgentOutput):
    pass


@pytest.mark.parametrize(
    "annotation", [None, Any, TAgentOutput, BareOutput], ids=["none", "any", "base", "empty-sub"]
)
def test_output_defaults_to_the_job_result(annotation: Any) -> None:
    """Saying nothing about the result means the agent reports `status` and `summary`."""

    def bare(run_context: TJobRunContext = None) -> Any:
        pass

    if annotation is None:
        del bare.__annotations__["return"]
    else:
        bare.__annotations__["return"] = annotation

    schema = output_from_return(bare, SOURCE)
    assert set(schema["properties"]) == {"status", "summary"}
    assert set(schema["required"]) == {"status", "summary"}


def test_the_docstring_is_the_whole_prompt() -> None:
    """An agent that takes nothing has a system prompt and no inputs."""

    def sanity(run_context: TJobRunContext = None) -> BareOutput:
        """Lists what it can see.

        Report every skill, tool and MCP tool you are given.
        """

    spec = agent_spec_from_function(sanity, SOURCE, {})
    assert spec["inputs"]["properties"] == {}
    assert spec["description"] == "Lists what it can see."
    assert spec["system_prompt"].endswith("MCP tool you are given.")


@pytest.mark.parametrize(
    "access,expected",
    [
        ({"data": "write"}, {"data": ["write"]}),
        ({"local": "all", "context": ["read"]}, {"local": ["all"], "context": ["read"]}),
        ({"data": "read", "toolkits": False}, {"data": ["read"], "toolkits": False}),
    ],
    ids=["single-verb", "mixed", "with-toolkits"],
)
def test_access_accepts_a_single_verb(access: Any, expected: Any) -> None:
    spec = agent_spec_from_function(inspector, SOURCE, {"access": access})
    assert spec["access"] == expected


def test_inputs_schema_passes_manifest_validation() -> None:
    """The inputs are a JSON Schema, so whatever a generator emits must survive validation."""
    from dlt._workspace.deployment.agent.manifest import to_agent_definition
    from dlt._workspace.deployment.manifest import validate_manifest

    spec = agent_spec_from_function(inspector, SOURCE, {})
    manifest: Any = {
        "engine_version": 1,
        "created_at": "2026-01-01T00:00:00Z",
        "deployment_module": "__deployment__",
        "jobs": [
            {
                "job_ref": "jobs.ops.inspector",
                "entry_point": {
                    "module": "m",
                    "function": "inspector",
                    "job_type": "batch",
                    "launcher": "dlt._workspace.deployment.launchers.agent",
                },
                "triggers": [],
                "execute": {"concurrency": 1},
                "agent": to_agent_definition(spec),
            }
        ],
    }
    assert "additionalProperties" in spec["inputs"]
    result = validate_manifest(manifest)
    assert result.is_valid, result.errors


class Described(TAgentOutput):
    category_tools: Annotated[Dict[str, List[str]], Doc("all detected tools, in your categories")]
    # a bare string reads as a description too, though flake8 takes it for a forward reference
    hunch: Annotated[NotRequired[str], "what you suspect, one line"]  # noqa: F722
    plain: str


def test_annotated_describes_what_the_agent_reads() -> None:
    """`Annotated` is the one place a description can be written without importing pydantic."""

    def inspector(
        run_id: Annotated[str, Doc("the run to look at")] = dlt.config.value,
        depth: Annotated[int, Doc("how deep to dig")] = 2,
        untouched: str = "x",
    ) -> Described:
        """Inspects a run."""
        return None

    output = output_from_return(inspector, SOURCE)["properties"]
    inputs = inputs_from_function(inspector, SOURCE)["properties"]

    assert output["category_tools"]["description"] == "all detected tools, in your categories"
    assert output["hunch"]["description"] == "what you suspect, one line"
    assert "description" not in output["plain"]
    # the contract fields carry their meaning down from `TAgentOutput`
    assert "aborted" in output["status"]["description"]
    assert output["summary"]["description"].startswith("Markdown.")
    # a Literal is a set of values, never a description
    assert output["status"]["enum"] == ["succeeded", "failed", "aborted"]

    assert inputs["run_id"]["description"] == "the run to look at"
    assert inputs["depth"]["description"] == "how deep to dig"
    assert "description" not in inputs["untouched"]


def test_agent_output_is_the_payload_alone() -> None:
    """The agent returns a payload type, not the envelope, so no launcher field reaches the model."""

    def inspector() -> TAgentOutput:
        """Inspects a run."""
        return None

    schema = output_from_return(inspector, SOURCE)

    assert set(schema["properties"]) == {"status", "summary"}
    for envelope_field in ("type", "engine_version", "result", "object", "job_ref", "trace"):
        assert envelope_field not in schema["properties"]
    assert "$defs" not in schema
