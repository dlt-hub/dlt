"""An agent declared as a Python function: its signature, return type and docstring."""

import inspect
from copy import deepcopy
from functools import lru_cache
from typing import Any, Dict, Optional, cast

from dlt.common.typing import AnyFun
from dlt.common.utils import get_callable_name

from dlt._workspace.deployment.agent.exceptions import InvalidAgentSpec
from dlt._workspace.deployment.agent.manifest import validate_agent_spec
from dlt._workspace.deployment.agent.typing import TAgentDefaults, TAgentOutput, TAgentSpec
from dlt._workspace.deployment.reflection import derives_from, inputs_from_function, output_schema

SPEC_KEYS = ("access", "tools", "skills", "rules")
DEFAULTS_KEYS = ("model", "limits", "loop_run_args", "trigger")


def output_from_return(f: AnyFun, source: str) -> Dict[str, Any]:
    """JSON Schema of the agent's own output, taken from the return type."""
    hint = inspect.signature(f).return_annotation
    if hint in (inspect.Signature.empty, None, Any):
        # nothing declared: the agent reports the base outcome, `status` and `summary`
        hint = TAgentOutput
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


def with_standard_output(declared: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Declared output plus `status` and `summary`, which `TAgentOutput` alone defines.

    Args:
        declared (Optional[Dict[str, Any]]): Output JSON Schema an `AGENT.md` carries, if any.

    Returns:
        Dict[str, Any]: The declaration with the standard fields written over it.
    """
    standard = deepcopy(_standard_output())
    if not declared:
        return standard
    schema = deepcopy(dict(declared))
    schema["type"] = "object"
    schema["properties"] = {**(schema.get("properties") or {}), **standard["properties"]}
    required = schema.get("required")
    # `required: {}` is how "nothing is required" is written in YAML, as it is for inputs
    declared_required = sorted(required) if isinstance(required, dict) else list(required or [])
    schema["required"] = sorted({*declared_required, *standard["required"]})
    return schema


def agent_spec_from_function(
    f: AnyFun,
    source: str,
    declared: Dict[str, Any],
    base: Optional[TAgentSpec] = None,
) -> TAgentSpec:
    """Assembles the agent a function declares.

    The `agent` reference, when given, is the base; decorator arguments override it, and the
    function's own signature, return type and docstring override those in turn.

    Args:
        f (AnyFun): The decorated function.
        source (str): `<file>:<name>`, named in errors and carried as the agent folder.
        declared (Dict[str, Any]): Decorator arguments, `AGENT.md` field names.
        base (Optional[TAgentSpec]): Spec of the agent the decorator referenced.
    """
    spec: Dict[str, Any] = dict(base) if base else {}
    docstring = inspect.cleandoc(f.__doc__ or "")

    spec["name"] = declared.get("name") or get_callable_name(f)
    if docstring:
        spec["description"] = docstring.split("\n", 1)[0]
        spec["system_prompt"] = docstring
    for key in SPEC_KEYS:
        if declared.get(key) is not None:
            spec[key] = declared[key]

    defaults: TAgentDefaults = dict(spec.get("defaults") or {})  # type: ignore[assignment]
    for key in DEFAULTS_KEYS:
        if declared.get(key) is not None:
            defaults[key] = declared[key]  # type: ignore[literal-required]
    if defaults:
        spec["defaults"] = defaults

    inputs: Dict[str, Any] = dict(spec.get("inputs") or {})
    signature_inputs = inputs_from_function(f, source)
    if signature_inputs.get("properties") or not inputs:
        inputs.update(signature_inputs)
    spec["inputs"] = inputs

    # a function driving a referenced agent may return anything; then that agent's output stands
    return_hint = inspect.signature(f).return_annotation
    if derives_from(return_hint, TAgentOutput) or not spec.get("output"):
        spec["output"] = output_from_return(f, source)

    return validate_agent_spec(cast(TAgentSpec, spec), source)


def agent_source(f: AnyFun, name: str) -> str:
    """Where a function-declared agent lives: `<module file>:<name>`."""
    return f"{inspect.getfile(f)}:{name}"


__all__ = [
    "agent_source",
    "agent_spec_from_function",
    "output_from_return",
    "with_standard_output",
]
