"""JSON Schema of job inputs and outputs."""

import inspect
import re
from typing import Any, Dict, List, Optional, Type, cast

from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.reflection.json_schema import JsonSchemaBuilder
from dlt.common.reflection.spec import spec_from_signature
from dlt.common.typing import (
    AnyFun,
    annotation_metadata,
    get_args,
    get_type_globals,
    is_typeddict,
    resolve_single_annotation,
)

from dlt._workspace.deployment.exceptions import InvalidJobSchema
from dlt._workspace.deployment.typing import (
    RUN_CONTEXT_INPUT,
    THubEntityType,
    TLegacyHubEntityType,
)

ENTITY_TYPE_KEY = "entity_type"
"""Schema keyword on a property whose value is the unique id of a workspace entity of that type."""


class Entity:
    """`Annotated[str, Entity("job-runs")]`: the value is the unique id of a workspace entity."""

    def __init__(self, type: THubEntityType) -> None:  # noqa: A002
        self.type = type


def annotated_entity(hint: Any) -> Optional[Entity]:
    """The `Entity` marker an `Annotated` hint carries, if any."""
    for metadata in annotation_metadata(hint):
        if isinstance(metadata, Entity):
            return metadata
    return None


def schema_builder() -> JsonSchemaBuilder:
    """Schema builder that writes `Entity` markers as `entity_type`."""
    return JsonSchemaBuilder(
        annotate=lambda m: {ENTITY_TYPE_KEY: m.type} if isinstance(m, Entity) else None
    )


JSON_SCHEMA_TYPES: Dict[str, Any] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "array": List[Any],
    "object": Dict[str, Any],
}


def injectable_fields(spec: Optional[Type[BaseConfiguration]]) -> Dict[str, Any]:
    """Fields of a job configspec that configuration fills."""
    fields = spec.get_resolvable_fields() if spec is not None else {}
    # the launcher passes the run context itself
    return {name: hint for name, hint in fields.items() if name != RUN_CONTEXT_INPUT}


def inputs_from_spec(spec: Optional[Type[BaseConfiguration]], source: str) -> Dict[str, Any]:
    """JSON Schema of the fields of a job configspec that configuration fills."""
    builder = schema_builder()
    try:
        schema = builder.spec_schema(spec, exclude=(RUN_CONTEXT_INPUT,)) if spec else {}
    except Exception as ex:
        raise InvalidJobSchema(source, f"inputs cannot be read from {spec!r}: {ex}") from ex
    schema.pop("title", None)
    schema.update({"type": "object", "additionalProperties": False})
    schema.setdefault("properties", {})
    return builder.with_defs(schema)


def inputs_from_function(f: AnyFun, source: str) -> Dict[str, Any]:
    """JSON Schema of the arguments of `f` that configuration can inject."""
    return inputs_from_spec(spec_from_signature(f, inspect.signature(f))[0], source)


def return_hint(f: AnyFun) -> Any:
    """Return annotation of `f`, resolved when the module stores annotations as strings."""
    hint = inspect.signature(f).return_annotation
    return resolve_single_annotation(hint, globalns=get_type_globals(f))


def job_result_from_return(f: AnyFun, source: str) -> Optional[Dict[str, Any]]:
    """Output JSON Schema of `f`, or `None` unless it returns a TypedDict."""
    hint = return_hint(f)
    if not is_typeddict(hint):
        return None
    return output_schema(hint, source)


def output_schema(hint: Any, source: str) -> Dict[str, Any]:
    """JSON Schema of a TypedDict or pydantic model, `Annotated` markers of every field included."""
    try:
        return schema_builder().root_schema(hint)
    except Exception as ex:
        raise InvalidJobSchema(source, f"output cannot be read from {hint!r}: {ex}") from ex


def derives_from(hint: Any, base: Any) -> bool:
    """True for `base` itself and for any TypedDict deriving from it."""
    if hint is base:
        return True
    return any(derives_from(parent, base) for parent in getattr(hint, "__orig_bases__", ()))


def spec_from_inputs_schema(name: str, inputs: Dict[str, Any]) -> Type[BaseConfiguration]:
    """Configuration spec with one field per property of an inputs JSON Schema."""
    # `required` is a list, or the `{}` mapping form an `AGENT.md` may carry
    required = set(inputs.get("required") or ())
    annotations: Dict[str, Any] = {}
    fields: Dict[str, Any] = {"__module__": __name__}
    for field, schema in (inputs.get("properties") or {}).items():
        # `Optional[T]` is written as `anyOf` T and null
        variants = [v for v in schema.get("anyOf") or [schema] if v.get("type") != "null"]
        hint = JSON_SCHEMA_TYPES.get(variants[0].get("type"), Any) if len(variants) == 1 else Any
        # a non-optional hint left unresolved raises, exactly as a required job argument does
        annotations[field] = hint if field in required else Optional[hint]
        fields[field] = None
    fields["__annotations__"] = annotations

    spec_name = "".join(part.capitalize() for part in re.split(r"[\W_]+", name))
    return configspec()(type(f"{spec_name}InputsConfiguration", (BaseConfiguration,), fields))


def model_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    """The schema as a model gets it: `entity_type` keywords moved into `$comment`."""

    def convert(node: Any) -> Any:
        if isinstance(node, list):
            return [convert(item) for item in node]
        if not isinstance(node, dict):
            return node
        entity_type = node.get(ENTITY_TYPE_KEY)
        # a property that happens to be named `entity_type` maps to a schema, the keyword to a name
        is_keyword = isinstance(entity_type, str)
        converted = {
            k: convert(v) for k, v in node.items() if not (is_keyword and k == ENTITY_TYPE_KEY)
        }
        # strict validators refuse unknown keywords, `$comment` is skipped but read by models
        if is_keyword:
            note = f"{ENTITY_TYPE_KEY}: {entity_type}"
            comment = converted.get("$comment")
            converted["$comment"] = f"{comment}; {note}" if comment else note
        return converted

    return cast(Dict[str, Any], convert(schema))


def entity_properties(schema: Optional[Dict[str, Any]], source: str) -> Dict[str, THubEntityType]:
    """Property name to entity type for every property carrying `entity_type`, in declaration order.

    Raises:
        InvalidJobSchema: A property names an entity type dlt does not know.
    """
    known = get_args(THubEntityType)
    found: Dict[str, THubEntityType] = {}
    for name, prop in ((schema or {}).get("properties") or {}).items():
        entity_type = prop.get(ENTITY_TYPE_KEY)
        if entity_type is None:
            continue
        if entity_type not in known and entity_type not in get_args(TLegacyHubEntityType):
            raise InvalidJobSchema(
                source,
                f"{name}: entity_type {entity_type!r} is not one of {', '.join(known)}",
            )
        found[name] = entity_type
    return found


__all__ = [
    "ENTITY_TYPE_KEY",
    "RUN_CONTEXT_INPUT",
    "Entity",
    "annotated_entity",
    "derives_from",
    "entity_properties",
    "injectable_fields",
    "inputs_from_function",
    "inputs_from_spec",
    "job_result_from_return",
    "model_schema",
    "output_schema",
    "return_hint",
    "schema_builder",
    "spec_from_inputs_schema",
]
