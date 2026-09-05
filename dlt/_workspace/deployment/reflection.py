"""What a job takes and what it returns, as JSON Schema.

A job's inputs are the arguments configuration can inject, so the schema holds the fields of the
configspec `with_config` synthesizes and nothing else.
"""

import inspect
import re
import typing
import warnings
from typing import Any, Collection, Dict, List, Mapping, Optional, Tuple, Type, cast, get_args

from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.json import json
from dlt.common.reflection.spec import spec_from_signature
from dlt.common.typing import AnyFun, ConfigValueSentinel, NotRequired, is_typeddict

from dlt._workspace.deployment.exceptions import InvalidJobSchema
from dlt._workspace.deployment.typing import RUN_CONTEXT_INPUT, THubEntityType

ENTITY_TYPE_KEY = "entity_type"
"""Schema keyword on a property whose value is the unique id of a workspace entity of that type."""


JSON_SCHEMA_TYPES: Dict[str, Any] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "array": List[Any],
    "object": Dict[str, Any],
}


def injectable_fields(spec: Optional[Type[BaseConfiguration]]) -> Dict[str, Any]:
    """Fields of a job configspec that configuration fills. The launcher passes the run context."""
    fields = spec.get_resolvable_fields() if spec is not None else {}
    return {name: hint for name, hint in fields.items() if name != RUN_CONTEXT_INPUT}


def config_field_names(f: AnyFun) -> Collection[str]:
    """Fields of the configspec `f` gets, as `with_config` synthesizes it."""
    return injectable_fields(spec_from_signature(f, inspect.signature(f))[0])


def inputs_from_function(
    f: AnyFun, source: str, fields: Optional[Collection[str]] = None
) -> Dict[str, Any]:
    """Arguments JSON Schema of `f`, holding exactly what configuration can inject.

    Args:
        f (AnyFun): Function the job runs.
        source (str): What to name in the error, usually the job ref.
        fields (Optional[Collection[str]]): Resolvable fields of the job's configspec. Read from
            the signature when absent.

    Returns:
        Dict[str, Any]: JSON Schema of the arguments, `required` following `dlt.config.value`.
    """
    # pydantic is optional and this module is imported with every workspace, so it stays here
    from dlt.common.libs.pydantic import PydanticJsonSchemaWarning, TypeAdapter

    if fields is None:
        fields = config_field_names(f)
    try:
        with warnings.catch_warnings():
            # `dlt.config.value` is not serializable, and is dropped from the schema below
            warnings.simplefilter("ignore", PydanticJsonSchemaWarning)
            schema: Dict[str, Any] = TypeAdapter(f).json_schema()
    except Exception as ex:
        raise InvalidJobSchema(source, f"inputs cannot be read from the signature: {ex}") from ex
    properties: Dict[str, Any] = {
        name: prop for name, prop in (schema.get("properties") or {}).items() if name in fields
    }
    schema["properties"] = properties
    required = [name for name in schema.get("required") or [] if name in properties]
    # `dlt.config.value` is dlt's "required, resolved from config". pydantic sees only a default
    for name, parameter in inspect.signature(f).parameters.items():
        if isinstance(parameter.default, ConfigValueSentinel) and name in properties:
            properties[name].pop("default", None)
            required.append(name)
    if required:
        schema["required"] = required
    else:
        schema.pop("required", None)
    describe_properties(properties, typing.get_type_hints(f, include_extras=True))
    prune_unreferenced_defs(schema)
    return schema


def job_result_from_return(f: AnyFun, source: str) -> Optional[Dict[str, Any]]:
    """Output JSON Schema of `f`, or `None` unless it returns a TypedDict."""
    hint = inspect.signature(f).return_annotation
    if not is_typeddict(hint):
        return None
    return output_schema(hint, source)


def output_schema(hint: Any, source: str) -> Dict[str, Any]:
    """JSON Schema of a TypedDict, with what `Annotated` says about each field written in."""
    from dlt.common.libs.pydantic import TypeAdapter

    try:
        schema: Dict[str, Any] = TypeAdapter(hint).json_schema()
    except Exception as ex:
        raise InvalidJobSchema(source, f"output cannot be read from {hint!r}: {ex}") from ex
    properties: Dict[str, Any] = schema.get("properties") or {}
    describe_properties(properties, typing.get_type_hints(hint, include_extras=True))
    prune_unreferenced_defs(schema)
    return schema


def derives_from(hint: Any, base: Any) -> bool:
    """True for `base` itself and for any TypedDict deriving from it."""
    if hint is base:
        return True
    return any(derives_from(parent, base) for parent in getattr(hint, "__orig_bases__", ()))


def spec_from_inputs_schema(name: str, inputs: Dict[str, Any]) -> Type[BaseConfiguration]:
    """Configuration spec with one field per declared input.

    Inputs are to a declared job what parameters are to a function job, so they resolve the same
    way. They come from the job's config section, through every provider, with the declared type.
    """
    # `required` is a list, or the `{}` mapping form an `AGENT.md` may carry
    required = set(inputs.get("required") or ())
    annotations: Dict[str, Any] = {}
    fields: Dict[str, Any] = {"__module__": __name__}
    for field, schema in (inputs.get("properties") or {}).items():
        hint = JSON_SCHEMA_TYPES.get(schema.get("type"), Any)
        # a non-optional hint left unresolved raises, exactly as a required job argument does
        annotations[field] = hint if field in required else Optional[hint]
        fields[field] = None
    fields["__annotations__"] = annotations

    spec_name = "".join(part.capitalize() for part in re.split(r"[\W_]+", name))
    return configspec()(type(f"{spec_name}InputsConfiguration", (BaseConfiguration,), fields))


def prune_unreferenced_defs(schema: Dict[str, Any]) -> None:
    """Drops the `$defs` nothing points at, following the references between those kept."""
    defs = schema.get("$defs")
    if not defs:
        return
    kept: Dict[str, Any] = {}
    referenced = json.dumps({k: v for k, v in schema.items() if k != "$defs"})
    while found := {
        k: v for k, v in defs.items() if k not in kept and f'#/$defs/{k}"' in referenced
    }:
        kept.update(found)
        referenced = json.dumps(found)
    if kept:
        schema["$defs"] = kept
    else:
        schema.pop("$defs")


def annotation_metadata(annotation: Any) -> Tuple[Any, ...]:
    """What an `Annotated` hint carries, read through `NotRequired`."""
    if typing.get_origin(annotation) is NotRequired:
        return annotation_metadata(typing.get_args(annotation)[0])
    # only `Annotated` carries metadata; `Literal` args are values, not annotations
    return cast(Tuple[Any, ...], getattr(annotation, "__metadata__", ()))


def annotated_description(annotation: Any) -> Optional[str]:
    """Description an `Annotated` hint carries: a `Doc` marker, or a plain string."""
    for metadata in annotation_metadata(annotation):
        if isinstance(metadata, str):
            return metadata
        # typing_extensions.Doc, as PEP 727 defines it. pydantic ignores it, we do not
        documentation = getattr(metadata, "documentation", None)
        if isinstance(documentation, str):
            return documentation
    return None


class Entity:
    """`Annotated[str, Entity("job-run")]`: the value is the unique id of a workspace entity."""

    def __init__(self, type: THubEntityType) -> None:  # noqa: A002
        self.type = type


def annotated_entity(annotation: Any) -> Optional[Entity]:
    """The `Entity` marker an `Annotated` hint carries, if any."""
    for metadata in annotation_metadata(annotation):
        if isinstance(metadata, Entity):
            return metadata
    return None


def describe_properties(properties: Dict[str, Any], hints: Mapping[str, Any]) -> None:
    """Writes what `Annotated` says about each field into the schema its reader gets."""
    for name, annotation in hints.items():
        if name not in properties:
            continue
        if described := annotated_description(annotation):
            properties[name].setdefault("description", described)
        if entity := annotated_entity(annotation):
            properties[name].setdefault(ENTITY_TYPE_KEY, entity.type)


def model_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    """The schema as a model gets it: `entity_type` moved into `$comment`, nothing else changed.

    Strict validators such as the Claude CLI's refuse keywords they do not know, and `$comment`
    is the one standard slot for a note that validation ignores and a model still reads.
    """

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
        if entity_type not in known:
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
    "annotated_description",
    "annotated_entity",
    "config_field_names",
    "derives_from",
    "entity_properties",
    "injectable_fields",
    "inputs_from_function",
    "job_result_from_return",
    "model_schema",
    "output_schema",
    "spec_from_inputs_schema",
]
