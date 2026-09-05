"""Job configuration synthesized from an agent's declared inputs."""

import inspect
from typing import List, Type

from dlt.common import logger
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.typing import AnyFun
from dlt.common.utils import get_callable_name

from dlt._workspace.deployment.agent.manifest import declared_placeholders
from dlt._workspace.deployment.agent.typing import TAgentSpec
from dlt._workspace.deployment.reflection import spec_from_inputs_schema


def spec_from_agent_inputs(agent_spec: TAgentSpec) -> Type[BaseConfiguration]:
    """Configuration spec with one field per input the agent declares."""
    return spec_from_inputs_schema(agent_spec["name"], agent_spec["inputs"])


def warn_unreferenced_inputs(agent_spec: TAgentSpec) -> List[str]:
    """Declared inputs no placeholder in the system prompt uses. Warns, never raises."""
    referenced = {
        name.partition(".")[0] for name in declared_placeholders(agent_spec["system_prompt"])
    }
    unreferenced = [
        name for name in (agent_spec["inputs"].get("properties") or {}) if name not in referenced
    ]
    if unreferenced:
        logger.warning(
            f"Agent {agent_spec['name']!r} declares inputs"
            f" {', '.join(map(repr, unreferenced))} that its system prompt never mentions."
            " Add {{ name }} where each belongs, or drop it."
        )
    return unreferenced


def warn_unbound_inputs(agent_spec: TAgentSpec, f: AnyFun) -> List[str]:
    """Declared inputs the decorated function cannot receive. Warns, never raises."""
    try:
        parameters = inspect.signature(f).parameters
    except (ValueError, TypeError):
        return []
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters.values()):
        return []
    unbound = [
        name for name in (agent_spec["inputs"].get("properties") or {}) if name not in parameters
    ]
    if unbound:
        logger.warning(
            f"Agent {agent_spec['name']!r} declares inputs {', '.join(map(repr, unbound))} that"
            f" {get_callable_name(f)}() does not accept. Nothing will pass them."
        )
    return unbound
