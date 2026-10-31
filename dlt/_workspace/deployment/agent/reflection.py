"""An agent declared as a Python function: its signature, return type and docstring."""

import inspect
from copy import deepcopy
from functools import lru_cache
from typing import Any, Dict, Mapping, Optional, Type, Union, cast

from dlt.common.typing import AnyFun
from dlt.common.utils import get_callable_name

from dlt._workspace.deployment.agent.exceptions import InvalidAgentSpec
from dlt._workspace.deployment.agent.manifest import validate_agent_spec
from dlt._workspace.deployment.agent.typing import TAgentOutput, TAgentSpec
from dlt._workspace.deployment.reflection import (
    derives_from,
    inputs_from_function,
    output_schema,
    return_hint,
)

SPEC_KEYS = ("access", "tools", "skills", "rules")
SCHEMA_TYPE_KEYS = frozenset(
    (
        "type",
        "anyOf",
        "oneOf",
        "allOf",
        "$ref",
        "enum",
        "const",
        "items",
        "prefixItems",
        "properties",
        "additionalProperties",
        "format",
    )
)
"""JSON Schema keys that define a value's type."""


def output_from_return(f: AnyFun, source: str) -> Dict[str, Any]:
    """JSON Schema of the agent's own output, taken from the return type."""
    hint = return_hint(f)
    if hint in (inspect.Signature.empty, None, Any):
        # nothing declared: the agent reports the base outcome, `status` and `summary`
        hint = TAgentOutput
    if isinstance(hint, str):
        raise InvalidAgentSpec(
            source, f"return type {hint!r} could not be resolved. Define or import it in the module"
        )
    if not derives_from(hint, TAgentOutput):
        raise InvalidAgentSpec(
            source,
            "must return TAgentOutput or a TypedDict deriving from it, got"
            f" {getattr(hint, '__name__', hint)!r}",
        )
    return output_schema(hint, source)


@lru_cache(maxsize=1)
def _standard_output() -> Dict[str, Any]:
    return output_schema(TAgentOutput, "TAgentOutput")


def with_standard_output(
    declared: Union[Dict[str, Any], Type[Any], None], source: str = "output"
) -> Dict[str, Any]:
    """Declared output schema with `status` and `summary` of `TAgentOutput` added."""
    standard = deepcopy(_standard_output())
    if not declared:
        return standard
    if not isinstance(declared, Mapping):
        declared = output_schema(declared, source)
    schema = deepcopy(dict(declared))
    schema["type"] = "object"
    schema["properties"] = {**(schema.get("properties") or {}), **standard["properties"]}
    required = schema.get("required")
    # `required: {}` is how "nothing is required" is written in YAML, as it is for inputs
    declared_required = sorted(required) if isinstance(required, dict) else list(required or [])
    schema["required"] = sorted({*declared_required, *standard["required"]})
    return schema


def merge_inputs(base: Dict[str, Any], signature: Dict[str, Any]) -> Dict[str, Any]:
    """Inputs of the function signature, completed from the agent definition's inputs."""
    base_properties: Dict[str, Any] = base.get("properties") or {}
    properties: Dict[str, Any] = {}
    # only inputs in the signature are kept
    for name, prop in (signature.get("properties") or {}).items():
        declared = base_properties.get(name) or {}
        # an annotated parameter keeps its own type, the definition fills in the rest
        typed = any(key in prop for key in SCHEMA_TYPE_KEYS)
        properties[name] = {
            **{k: v for k, v in declared.items() if not (typed and k in SCHEMA_TYPE_KEYS)},
            **prop,
        }
    merged = {**signature, "properties": properties}
    if base_defs := base.get("$defs"):
        merged["$defs"] = {**base_defs, **(signature.get("$defs") or {})}
    return merged


def agent_spec_from_function(
    f: AnyFun,
    source: str,
    declared: Dict[str, Any],
    base: Optional[TAgentSpec] = None,
) -> TAgentSpec:
    """Builds the agent spec a decorated function declares."""
    # override order: referenced agent, then decorator arguments, then the function itself
    spec: Dict[str, Any] = dict(base) if base else {}
    docstring = inspect.cleandoc(f.__doc__ or "")

    spec["name"] = declared.get("name") or get_callable_name(f)
    if docstring:
        spec["description"] = docstring.split("\n", 1)[0]
        spec["system_prompt"] = docstring
    for key in SPEC_KEYS:
        if declared.get(key) is not None:
            spec[key] = declared[key]
    # `defaults` stay the referenced agent's: decorator settings are layered over them at run time

    inputs: Dict[str, Any] = dict(spec.get("inputs") or {})
    signature_inputs = inputs_from_function(f, source)
    if signature_inputs.get("properties") or not inputs:
        inputs = merge_inputs(inputs, signature_inputs)
    spec["inputs"] = inputs

    # a function driving a referenced agent may return anything; then that agent's output stands
    hint = return_hint(f)
    if derives_from(hint, TAgentOutput) or not spec.get("output"):
        spec["output"] = output_from_return(f, source)

    return validate_agent_spec(cast(TAgentSpec, spec), source)


def agent_source(f: AnyFun, name: str) -> str:
    """Where a function-declared agent lives: `<module file>:<name>`."""
    return f"{inspect.getfile(f)}:{name}"


__all__ = [
    "agent_source",
    "agent_spec_from_function",
    "merge_inputs",
    "output_from_return",
    "with_standard_output",
]
