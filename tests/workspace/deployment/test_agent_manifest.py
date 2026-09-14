"""Tests for loading `AGENT.md` and resolving the components it references."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import pytest

from dlt.common.json import json

from dlt._workspace.deployment.agent.exceptions import (
    AgentComponentNotFound,
    InvalidAgentSpec,
)
from dlt._workspace.deployment.agent.configuration import (
    spec_from_agent_inputs,
    warn_unbound_inputs,
    warn_unreferenced_inputs,
)
from dlt._workspace.deployment.agent.manifest import (
    declared_placeholders,
    granted,
    inline_components,
    inputs_schema,
    load_agent_spec,
    render_placeholders,
    resolve_agent_dir,
    resolve_component_ref,
    to_agent_definition,
    validate_agent_spec,
)
from dlt._workspace.deployment.agent.typing import TAgentSpec
from dlt._workspace.deployment.typing import AGENT_DEFINITION_ENGINE_VERSION

from tests.workspace.utils import isolated_workspace


AGENT_REF = "dlthub-platform:job-inspector"


def _spec(run_dir: str) -> Any:
    return load_agent_spec(resolve_agent_dir(AGENT_REF, run_dir))


def test_resolve_agent_dir_from_toolkit_ref() -> None:
    with isolated_workspace("agent_workspace") as ctx:
        agent_dir = resolve_agent_dir(AGENT_REF, ctx.run_dir)
    assert agent_dir.endswith(os.path.join(".claude", "dlthub", "agents", "job-inspector"))


def test_resolve_agent_dir_from_path() -> None:
    with isolated_workspace("agent_workspace") as ctx:
        agent_dir = resolve_agent_dir(".claude/dlthub/agents/job-inspector", ctx.run_dir)
    assert Path(agent_dir, "AGENT.md").is_file()


@pytest.mark.parametrize(
    "ref", ["../outside", "dlthub-platform:missing"], ids=["escapes-workspace", "unknown-agent"]
)
def test_resolve_agent_dir_rejects(ref: str) -> None:
    with isolated_workspace("agent_workspace") as ctx:
        with pytest.raises(AgentComponentNotFound):
            resolve_agent_dir(ref, ctx.run_dir)


def test_a_missing_agent_says_what_to_run_next() -> None:
    """The manifest names an agent nobody installed: the error is the recovery instruction."""
    with isolated_workspace("agent_workspace") as ctx:
        # the workspace has dlthub-platform installed, but no such agent in it
        with pytest.raises(AgentComponentNotFound) as installed:
            resolve_agent_dir("dlthub-platform:no-such-agent", ctx.run_dir)
        with pytest.raises(AgentComponentNotFound) as absent:
            resolve_agent_dir("not-a-toolkit:no-such-agent", ctx.run_dir)
        with pytest.raises(AgentComponentNotFound) as bare:
            resolve_agent_dir("no-such-agent", ctx.run_dir)
        with pytest.raises(AgentComponentNotFound) as by_path:
            resolve_agent_dir(".claude/dlthub/agents/mine/AGENT.md", ctx.run_dir)

    said = str(installed.value)
    assert "is installed but has no agent 'no-such-agent'" in said
    assert "dlthub ai toolkit install dlthub-platform --overwrite" in said
    assert "dlthub ai toolkit info dlthub-platform" in said

    said = str(absent.value)
    assert "'not-a-toolkit' is not installed" in said
    assert "dlthub ai toolkit install not-a-toolkit" in said
    assert "dlthub ai toolkit list" in said
    # and where the install has to land the file
    assert said.rstrip().endswith(os.path.join("no-such-agent", "AGENT.md") + " in place.")

    assert "names no toolkit" in str(bare.value)
    assert "dlthub ai toolkit list" in str(bare.value)

    # a path ref is answered by a path, not by a toolkit command
    said = str(by_path.value)
    assert said.endswith(os.path.join("mine", "AGENT.md") + " is present.")
    assert "toolkit" not in said


def test_load_agent_spec_keeps_body_as_system_prompt() -> None:
    with isolated_workspace("agent_workspace") as ctx:
        spec = _spec(ctx.run_dir)
    assert spec["name"] == "job-inspector"
    assert "You are a job inspector" in spec["system_prompt"]
    # `defaults` stay on the spec; only the manifest subset drops them
    assert spec["defaults"]["model"] == "sonnet"


@pytest.mark.parametrize(
    "frontmatter,body,reason",
    [
        ("name: a\ndescription: b", "", "body is empty"),
        ("name: a\ndescription: b\ninputs:\n  prompt: p", "b", "must not declare 'prompt'"),
    ],
    ids=["empty-body", "prompt-input"],
)
def test_load_agent_spec_rejects(tmp_path: Path, frontmatter: str, body: str, reason: str) -> None:
    agent_dir = tmp_path / "broken"
    agent_dir.mkdir()
    text = f"---\n{frontmatter}\n---\n{body}\n" if frontmatter else body
    (agent_dir / "AGENT.md").write_text(text, encoding="utf-8")
    with pytest.raises(InvalidAgentSpec, match=reason):
        load_agent_spec(str(agent_dir))


def test_load_agent_spec_needs_only_a_body(tmp_path: Path) -> None:
    """No frontmatter at all: the folder names the agent and the rest defaults."""
    agent_dir = tmp_path / "tool-checker"
    agent_dir.mkdir()
    (agent_dir / "AGENT.md").write_text(
        "This is sanity check agent. Please list all tools.\n", encoding="utf-8"
    )
    spec = load_agent_spec(str(agent_dir))

    assert spec["name"] == "tool-checker"
    assert "description" not in spec
    assert spec["inputs"] == {}
    assert spec["system_prompt"] == "This is sanity check agent. Please list all tools."
    # `status` and `summary` come from TAgentJobResult, which every agent reports
    assert set(spec["output"]["properties"]) == {"status", "summary"}
    assert spec["output"]["required"] == ["status", "summary"]
    # nothing downstream may assume a description is there
    assert "description" not in to_agent_definition(spec)


@pytest.mark.parametrize(
    "declared", ["", "description:", "description: null"], ids=["absent", "bare", "null"]
)
def test_a_description_that_says_nothing_is_no_description(tmp_path: Path, declared: str) -> None:
    agent_dir = tmp_path / "quiet"
    agent_dir.mkdir()
    (agent_dir / "AGENT.md").write_text(
        f"---\nname: quiet\n{declared}\n---\nbody\n", encoding="utf-8"
    )
    assert "description" not in load_agent_spec(str(agent_dir))


def test_the_frontmatter_name_wins_over_the_folder(tmp_path: Path) -> None:
    agent_dir = tmp_path / "folder-name"
    agent_dir.mkdir()
    (agent_dir / "AGENT.md").write_text("---\nname: tool checker\n---\nbody\n", encoding="utf-8")
    assert load_agent_spec(str(agent_dir))["name"] == "tool checker"


def _load_output(tmp_path: Path, output: str) -> Dict[str, Any]:
    """Output schema of an agent whose frontmatter carries the given `output:` block."""
    agent_dir = tmp_path / "with-output"
    agent_dir.mkdir()
    (agent_dir / "AGENT.md").write_text(f"---\nname: a\n{output}---\nbody\n", encoding="utf-8")
    return load_agent_spec(str(agent_dir))["output"]


def test_declared_output_gains_the_standard_fields(tmp_path: Path) -> None:
    """An agent declares what it adds; `status` and `summary` arrive on their own."""
    output = _load_output(
        tmp_path,
        "output:\n"
        "  type: object\n"
        "  properties:\n"
        "    status:\n"
        "      enum: [succeeded, failed, aborted]\n"
        "    seen_tools:\n"
        "      type: array\n",
    )

    assert output["properties"]["seen_tools"] == {"type": "array"}
    assert output["properties"]["summary"]["type"] == "string"
    assert output["required"] == ["status", "summary"]
    # the declaration named `status` but said nothing about when to use which outcome
    assert "`aborted`" in output["properties"]["status"]["description"]


def test_the_typed_dict_wins_over_the_file(tmp_path: Path) -> None:
    """`TAgentJobResult` defines the standard fields, so a file cannot retype them."""
    output = _load_output(
        tmp_path,
        "output:\n"
        "  properties:\n"
        "    status:\n"
        "      type: integer\n"
        "      description: how many tools I found\n"
        "    summary:\n"
        "      type: integer\n"
        "  required: [status]\n",
    )

    status = output["properties"]["status"]
    assert status["type"] == "string"
    assert status["enum"] == ["succeeded", "failed", "aborted"]
    assert "how many tools" not in status["description"]
    # all three outcomes are named, so the model knows which one to report
    for outcome in ("succeeded", "failed", "aborted"):
        assert f"`{outcome}`" in status["description"]
    assert output["properties"]["summary"]["type"] == "string"
    assert output["required"] == ["status", "summary"]


def test_a_declared_output_keeps_its_own_shape(tmp_path: Path) -> None:
    output = _load_output(
        tmp_path,
        "output:\n"
        "  title: Tool report\n"
        "  description: what the agent saw\n"
        "  properties:\n"
        "    finding:\n"
        "      $ref: '#/$defs/Finding'\n"
        "  $defs:\n"
        "    Finding:\n"
        "      type: object\n",
    )

    assert output["title"] == "Tool report"
    assert output["description"] == "what the agent saw"
    assert output["$defs"]["Finding"] == {"type": "object"}
    assert output["properties"]["finding"] == {"$ref": "#/$defs/Finding"}
    # the merge brings the two fields over, not the type they come from
    assert "TAgentJobResult" not in json.dumps(output)


@pytest.mark.parametrize(
    "access,accepted",
    [
        ({"context": ["read"]}, True),
        ({"context": "read"}, True),
        ({"context": ["all"]}, True),
        ({"context": ["write"]}, False),
        ({"context": ["read", "execute"]}, False),
        ({"context": ["deploy"]}, False),
        ({"data": ["deploy"]}, False),
        ({"local": ["read", "network"]}, True),
    ],
    ids=[
        "read",
        "read-scalar",
        "all",
        "write",
        "read-and-execute",
        "deploy",
        "unknown-data",
        "local",
    ],
)
def test_access_verbs_are_held_to_the_axis(access: Dict[str, Any], accepted: bool) -> None:
    """`context` types the whole ladder but serves `read`; the rest is refused at the manifest."""
    spec = cast(
        TAgentSpec,
        {
            "name": "a",
            "description": "b",
            "system_prompt": "p",
            "output": {"properties": {"status": {}, "summary": {}}},
            "access": access,
        },
    )
    if accepted:
        assert validate_agent_spec(spec, "AGENT.md")["access"] == {
            axis: [verbs] if isinstance(verbs, str) else verbs for axis, verbs in access.items()
        }
    else:
        with pytest.raises(InvalidAgentSpec, match="takes") as refusal:
            validate_agent_spec(spec, "AGENT.md")
        # a verb the type carries but nothing implements says so, rather than reading as a typo
        typed_but_unserved = set(access.get("context") or []) - {"read"}
        assert ("no runtime serves" in str(refusal.value)) is bool(typed_but_unserved)


def test_load_agent_spec_without_inputs(tmp_path: Path) -> None:
    """An agent that takes nothing needs no `inputs` block."""
    agent_dir = tmp_path / "sanity"
    agent_dir.mkdir()
    (agent_dir / "AGENT.md").write_text(
        "---\nname: sanity\ndescription: Lists what it can see.\n"
        "output:\n  properties:\n    status: {}\n    summary: {}\n---\nbody\n",
        encoding="utf-8",
    )
    spec = load_agent_spec(str(agent_dir))

    assert spec["inputs"] == {}
    assert spec["system_prompt"] == "body"


def test_to_agent_definition_is_the_manifest_subset() -> None:
    with isolated_workspace("agent_workspace") as ctx:
        spec = _spec(ctx.run_dir)
    definition = to_agent_definition(
        spec, ".claude/dlthub/agents/job-inspector/AGENT.md", "extra prompt", "haiku"
    )
    assert definition["engine_version"] == AGENT_DEFINITION_ENGINE_VERSION
    assert definition["agent_file"] == ".claude/dlthub/agents/job-inspector/AGENT.md"
    assert definition["instructions"] == "extra prompt"
    assert definition["model"] == "haiku"
    # the runtime gets the declaration, never the defaults or the prompt, and never what the job
    # itself carries: its access, its inputs and its output
    assert "defaults" not in definition
    assert "system_prompt" not in definition
    assert "access" not in definition
    assert "inputs" not in definition
    assert "output" not in definition
    assert definition["tools"] == ["telemetry"]


def test_inputs_schema_coerces_required() -> None:
    with isolated_workspace("agent_workspace") as ctx:
        schema = inputs_schema(_spec(ctx.run_dir))
    # `required: {}` is how "nothing is required" is written; JSON Schema wants an array
    assert schema["required"] == []
    assert set(schema["properties"]) == {"failed_run_id", "failed_job_ref"}


def test_granted_expands_all() -> None:
    with isolated_workspace("agent_workspace") as ctx:
        spec = _spec(ctx.run_dir)
    assert granted(spec, "local") == {"read", "write", "execute", "network"}
    assert granted(spec, "data") == {"read"}


@pytest.mark.parametrize(
    "template,values,expected,unresolved",
    [
        ("run {{ a }}", {"a": "x"}, "run x", []),
        ("run {{ a.b }}", {"a": {"b": "y"}}, "run y", []),
        ("run {{ missing }}", {}, "run ", ["missing"]),
        ("run {{ a }}", {"a": None}, "run ", []),
        ("{{ lookup(x) }} stays", {}, "{{ lookup(x) }} stays", []),
    ],
    ids=["flat", "dotted", "missing", "none-value", "function-call-unsupported"],
)
def test_render_placeholders(
    template: str, values: Dict[str, Any], expected: str, unresolved: List[str]
) -> None:
    text, missing = render_placeholders(template, values)
    assert text == expected
    assert missing == unresolved


@pytest.mark.parametrize(
    "ref,kind",
    [("dlthub-platform:job-resources", "rule"), ("dlthub-platform:debug-deployment", "skill")],
    ids=["rule", "skill"],
)
def test_resolve_component_ref(ref: str, kind: str) -> None:
    with isolated_workspace("agent_workspace") as ctx:
        path = resolve_component_ref(ref, kind, ctx.run_dir)  # type: ignore[arg-type]
    assert Path(path).is_file()


def test_inline_components_skips_what_it_cannot_resolve() -> None:
    """A narrowed agent is weaker, not broken, so an unresolved ref only warns."""
    with isolated_workspace("agent_workspace") as ctx:
        blocks = inline_components(
            ["dlthub-platform:job-resources", "nope:missing"], "rule", ctx.run_dir
        )
    assert len(blocks) == 1
    assert blocks[0].startswith('<rule ref="dlthub-platform:job-resources">')


def _inputs_spec(required: Any) -> Any:
    return {
        "name": "job-inspector",
        "description": "d",
        "access": {},
        "inputs": {
            "type": "object",
            "properties": {
                "failed_run_id": {"type": "string"},
                "depth": {"type": "integer"},
                "verbose": {"type": "boolean"},
                "extras": {"type": "object"},
            },
            "required": required,
        },
        "output": {},
        "system_prompt": "b",
    }


@pytest.mark.parametrize(
    "required,expected_run_id_hint",
    [(["failed_run_id"], str), ({}, Optional[str]), (None, Optional[str])],
    ids=["required-list", "empty-mapping", "absent"],
)
def test_spec_from_agent_inputs(required: Any, expected_run_id_hint: Any) -> None:
    """Declared inputs become job configuration: one field each, typed by the schema."""
    spec_cls = spec_from_agent_inputs(_inputs_spec(required))
    fields = spec_cls.get_resolvable_fields()

    assert set(fields) == {"failed_run_id", "depth", "verbose", "extras"}
    # a required input keeps a non-optional hint, so resolution enforces it
    assert fields["failed_run_id"] == expected_run_id_hint
    assert fields["depth"] == Optional[int]
    assert fields["verbose"] == Optional[bool]
    assert fields["extras"] == Optional[Dict[str, Any]]
    assert spec_cls.__name__ == "JobInspectorInputsConfiguration"


def test_the_body_is_a_template_over_the_inputs() -> None:
    """The task lives in the body, so its placeholders are the inputs the agent is given."""
    with isolated_workspace("agent_workspace") as ctx:
        spec = _spec(ctx.run_dir)

    assert declared_placeholders(spec["system_prompt"]) == {
        "failed_job_ref",
        "failed_run_id",
        "run_context.trigger",
    }
    prompt, unresolved = render_placeholders(
        spec["system_prompt"],
        {"failed_job_ref": "jobs.ingest", "run_context": {"trigger": "job.fail:*"}},
    )
    assert "Investigate job_ref 'jobs.ingest' from trigger `job.fail:*`" in prompt
    assert "You are a job inspector" in prompt
    # an input nobody supplied renders blank and is reported
    assert unresolved == ["failed_run_id"]


def test_warn_unreferenced_inputs() -> None:
    """An input no placeholder mentions reaches no model, so the manifest says so."""
    spec = _inputs_spec(["failed_run_id"])
    spec["system_prompt"] = "Inspect {{ failed_run_id }} for {{ run_context.trigger }}."

    assert warn_unreferenced_inputs(spec) == ["depth", "verbose", "extras"]

    spec["system_prompt"] += " Depth {{ depth }}, verbose {{ verbose }}, extras {{ extras }}."
    assert warn_unreferenced_inputs(spec) == []


def test_warn_unbound_inputs() -> None:
    """A decorated function that cannot receive a declared input is reported, not rejected."""

    def takes_one(run_context: Any = None, failed_run_id: str = None) -> None:
        pass

    def takes_kwargs(**kwargs: Any) -> None:
        pass

    spec = _inputs_spec(["failed_run_id"])
    assert warn_unbound_inputs(spec, takes_one) == ["depth", "verbose", "extras"]
    # a function collecting kwargs can receive anything
    assert warn_unbound_inputs(spec, takes_kwargs) == []
