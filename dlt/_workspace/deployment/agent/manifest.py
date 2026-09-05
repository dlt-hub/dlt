"""Loads an `AGENT.md` and resolves the components it references."""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, cast

from dlt.common import logger

from dlt._workspace.cli.dlthub.ai.agents import COMPONENT_MARKERS, TComponentType
from dlt._workspace.cli.dlthub.ai.utils import load_toolkits_index, resolve_installed_component
from dlt._workspace.cli.formatters import parse_frontmatter
from dlt._workspace.deployment.agent.exceptions import (
    AgentComponentNotFound,
    InvalidAgentSpec,
)
from dlt._workspace.access import (
    ACCESS_AXES,
    ACCESS_AXIS_VERBS,
    ACCESS_AXIS_VOCABULARY,
    granted_verbs,
    missing_access,
)
from dlt._workspace.deployment.agent.typing import TAgentSpec
from dlt._workspace.deployment.typing import AGENT_DEFINITION_ENGINE_VERSION, TAgentDefinition
from dlt._workspace.typing import TWorkspaceAccess


_PLACEHOLDER = re.compile(r"\{\{\s*([\w.]+)\s*\}\}")


def agent_manifest_path(agent_dir: str) -> str:
    """`AGENT.md` inside an agent folder."""
    return os.path.join(agent_dir, COMPONENT_MARKERS["agent"])


def load_agent_spec(agent_dir: str) -> TAgentSpec:
    """Reads `AGENT.md` from an agent folder into a spec, body included.

    The body is the only thing an agent cannot do without. Frontmatter is optional, and so is
    every field in it: the name falls back to the folder.

    Args:
        agent_dir (str): Folder holding `AGENT.md`.

    Returns:
        TAgentSpec: Frontmatter plus the markdown body as `system_prompt`.

    Raises:
        InvalidAgentSpec: The body is empty, or a declared field breaks the contract.
    """
    path = agent_manifest_path(agent_dir)
    if not os.path.isfile(path):
        raise InvalidAgentSpec(path, "file does not exist")
    frontmatter, body = parse_frontmatter(open(path, "r", encoding="utf-8").read())
    if not body.strip():
        raise InvalidAgentSpec(path, "body is empty. The body is the system prompt")

    raw_spec: Dict[str, Any] = dict(frontmatter)
    raw_spec["system_prompt"] = body.strip()
    if not raw_spec.get("name"):
        raw_spec["name"] = os.path.basename(os.path.normpath(agent_dir))
    return validate_agent_spec(cast(TAgentSpec, raw_spec), path)


def validate_agent_spec(spec: TAgentSpec, source: str) -> TAgentSpec:
    """Holds a spec to the contract, wherever it was declared. Returns it unchanged.

    Args:
        spec (TAgentSpec): Spec read from `AGENT.md` or synthesized from a function.
        source (str): What to name in the error: a file path, or `<file>:<function>`.

    Raises:
        InvalidAgentSpec: The name or the system prompt is missing, or a field breaks the contract.
    """
    # reflection imports this module, so the output generator can only be reached from here
    from dlt._workspace.deployment.agent.reflection import with_standard_output

    declared: Dict[str, Any] = dict(spec)
    for required_key in ("name", "system_prompt"):
        if not declared.get(required_key):
            raise InvalidAgentSpec(source, f"must declare {required_key!r}")

    if not declared.get("description"):
        # a bare `description:` reads as null in YAML, and no description is not an empty one
        spec.pop("description", None)
    spec["output"] = with_standard_output(declared.get("output"))
    spec["inputs"] = declared.get("inputs") or {}
    if "prompt" in spec["inputs"]:
        raise InvalidAgentSpec(source, "inputs must not declare 'prompt'. Put the task in the body")
    access: Dict[str, Any] = dict(declared.get("access") or {})
    for axis in ACCESS_AXES:
        if isinstance(access.get(axis), str):
            access[axis] = [access[axis]]
        allowed = ACCESS_AXIS_VERBS[axis] + ("all",)
        refused = sorted({verb for verb in access.get(axis) or [] if verb not in allowed})
        if refused:
            unserved = sorted(set(ACCESS_AXIS_VOCABULARY[axis]) - set(allowed))
            reason = f"access.{axis} takes {', '.join(allowed)}"
            if set(unserved) & set(refused):
                reason += f", and no runtime serves {', '.join(unserved)} yet"
            raise InvalidAgentSpec(source, f"{reason}. Got {', '.join(refused)}")
    if access:
        spec["access"] = cast(TWorkspaceAccess, access)
    return spec


def declared_placeholders(system_prompt: str) -> Set[str]:
    """Names the system prompt refers to, `run_context.trigger` included."""
    return {match.group(1) for match in _PLACEHOLDER.finditer(system_prompt)}


def to_agent_definition(
    spec: TAgentSpec,
    agent_file: Optional[str] = None,
    instructions: Optional[str] = None,
    model: Optional[str] = None,
) -> TAgentDefinition:
    """Manifest subset of a spec: no `defaults`, no system prompt, plus decorator overrides.

    `inputs` and `output` are not here either: the job definition carries them, as it does for
    every other job.
    """
    definition: TAgentDefinition = {
        "engine_version": AGENT_DEFINITION_ENGINE_VERSION,
        "name": spec["name"],
    }
    if description := spec.get("description"):
        definition["description"] = description
    if agent_file:
        definition["agent_file"] = agent_file
    raw: Dict[str, Any] = dict(spec)
    for key in ("tools", "skills", "rules"):
        value = raw.get(key)
        if value:
            definition[key] = list(value)
    if instructions:
        definition["instructions"] = instructions
    if model:
        definition["model"] = model
    return definition


def inputs_schema(spec: TAgentSpec) -> Dict[str, Any]:
    """`inputs` as JSON Schema."""
    schema: Dict[str, Any] = dict(spec["inputs"])
    # `required: {}` is how "nothing is required" was written. JSON Schema wants an array
    required = schema.get("required")
    if isinstance(required, dict):
        schema["required"] = sorted(required)
    return schema


def granted(spec: TAgentSpec, axis: str) -> Set[str]:
    """Verbs the agent declared on one `access` axis, with `all` expanded."""
    return granted_verbs(spec.get("access"), axis)


def render_placeholders(template: str, values: Mapping[str, Any]) -> Tuple[str, List[str]]:
    """Substitutes `{{ name }}` and `{{ a.b }}`, blanking what is absent.

    Returns:
        Tuple[str, List[str]]: Rendered text, and the placeholders that did not resolve.
    """
    unresolved: List[str] = []

    def lookup(match: "re.Match[str]") -> str:
        value: Any = values
        for part in match.group(1).split("."):
            if not isinstance(value, Mapping) or part not in value:
                unresolved.append(match.group(1))
                return ""
            value = value[part]
        return "" if value is None else str(value)

    return _PLACEHOLDER.sub(lookup, template), unresolved


def _workspace_path(ref: str, kind: "TComponentType", workspace_root: str) -> str:
    """Resolve a workspace-relative component ref, refusing anything outside the workspace."""
    candidate = os.path.realpath(os.path.join(workspace_root, ref))
    root = os.path.realpath(workspace_root)
    if os.path.commonpath([candidate, root]) != root:
        raise AgentComponentNotFound(ref, kind, [f"{candidate} (outside the workspace)"])
    return candidate


def _is_path_ref(ref: str) -> bool:
    return os.sep in ref or "/" in ref or ref.endswith((".md", ".mdc"))


def resolve_agent_dir(agent_ref: str, workspace_root: str) -> str:
    """Resolves `<toolkit>:<agent>` or a workspace-relative path to an agent folder.

    Args:
        agent_ref (str): `"<toolkit>:<agent>"` reference, or a path under the workspace.
        workspace_root (str): Workspace directory the reference is resolved against.

    Returns:
        str: Absolute path of the agent folder.

    Raises:
        AgentComponentNotFound: The reference does not name an installed agent.
    """
    if _is_path_ref(agent_ref):
        candidate = _workspace_path(agent_ref, "agent", workspace_root)
        # the ref may point at the agent folder or at the `AGENT.md` inside it
        if os.path.basename(candidate) == COMPONENT_MARKERS["agent"]:
            candidate = os.path.dirname(candidate)
        if os.path.isfile(agent_manifest_path(candidate)):
            return candidate
        raise AgentComponentNotFound(agent_ref, "agent", [agent_manifest_path(candidate)])

    toolkit, _, name = agent_ref.rpartition(":")
    path = resolve_installed_component(toolkit, name, "agent", Path(workspace_root))
    if path is None:
        raise AgentComponentNotFound(
            agent_ref,
            "agent",
            _component_destination("agent", toolkit, name, workspace_root),
            toolkit=toolkit,
            installed=toolkit in load_toolkits_index(),
        )
    return str(path.parent)


def resolve_component_ref(ref: str, kind: "TComponentType", workspace_root: str) -> str:
    """Resolves a skill or rule ref to the file installed in the project."""
    if _is_path_ref(ref):
        candidate = _workspace_path(ref, kind, workspace_root)
        if os.path.isfile(candidate):
            return candidate
        raise AgentComponentNotFound(ref, kind, [candidate])

    toolkit, _, name = ref.rpartition(":")
    path = resolve_installed_component(toolkit, name, kind, Path(workspace_root))
    if path is None:
        raise AgentComponentNotFound(
            ref,
            kind,
            _component_destination(kind, toolkit, name, workspace_root),
            toolkit=toolkit,
            installed=toolkit in load_toolkits_index(),
        )
    return str(path)


def _component_destination(
    kind: "TComponentType", toolkit: str, name: str, workspace_root: str
) -> List[str]:
    """Where installing the toolkit would put the component, for the agent that installed it."""
    from dlt._workspace.cli.dlthub.ai.agents import AI_AGENTS

    entry = load_toolkits_index().get(toolkit)
    host = entry.get("agent") if entry else None
    variants = [AI_AGENTS[host]] if host in AI_AGENTS else list(AI_AGENTS.values())
    paths = [
        variant().component_path(kind, name, toolkit, Path(workspace_root)) for variant in variants
    ]
    return [str(path) for path in paths if path is not None]


def inline_components(refs: List[str], kind: TComponentType, workspace_root: str) -> List[str]:
    """Reads each ref and wraps it in a labelled block for the system prompt."""
    blocks: List[str] = []
    for ref in refs or []:
        try:
            path = resolve_component_ref(ref, kind, workspace_root)
        except AgentComponentNotFound as e:
            # an agent with less access is weaker, not broken
            logger.warning(f"Skipping {kind} {ref!r} for the agent prompt: {e}")
            continue
        text = open(path, "r", encoding="utf-8").read().strip()
        blocks.append(f'<{kind} ref="{ref}">\n{text}\n</{kind}>')
    return blocks
