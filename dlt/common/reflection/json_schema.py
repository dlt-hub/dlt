"""JSON Schema of Python type hints, configspecs and TypedDicts."""

import enum
import inspect
from typing import Any, Callable, Collection, Dict, List, Optional, Set, Type

from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    is_base_configuration_inner_hint,
)
from dlt.common.data_types import py_type_to_sc_type
from dlt.common.data_types.typing import TDataType
from dlt.common.json import json
from dlt.common.typing import (
    NotRequired,
    Required,
    SecretSentinel,
    annotation_metadata,
    extract_union_types,
    get_args,
    get_literal_args,
    get_origin,
    get_type_hints,
    is_annotated,
    is_dict_generic_type,
    is_list_generic_type,
    is_literal_type,
    is_newtype_type,
    is_optional_type,
    is_set_generic_type,
    is_subclass,
    is_typeddict,
    is_union_type,
)

TAnnotate = Callable[[Any], Optional[Dict[str, Any]]]
"""Keywords an `Annotated` metadata item adds to a schema, or `None` when it adds none."""

DATA_TYPE_SCHEMAS: Dict[TDataType, Dict[str, Any]] = {
    "text": {"type": "string"},
    "bigint": {"type": "integer"},
    "double": {"type": "number"},
    "bool": {"type": "boolean"},
    # a string keeps every digit, which a JSON number may not
    "decimal": {"type": ["number", "string"]},
    "wei": {"type": ["number", "string"]},
    "timestamp": {"type": "string", "format": "date-time"},
    "date": {"type": "string", "format": "date"},
    "time": {"type": "string", "format": "time"},
    "binary": {"type": "string", "contentEncoding": "base64"},
}
"""JSON Schema of each dlt data type a Python type maps to."""


def annotated_description(hint: Any) -> Optional[str]:
    """Description an `Annotated` hint carries: a `Doc` marker, or a plain string."""
    for metadata in annotation_metadata(hint):
        if isinstance(metadata, str):
            return metadata
        # typing_extensions.Doc, as PEP 727 defines it
        documentation = getattr(metadata, "documentation", None)
        if isinstance(documentation, str):
            return documentation
    return None


def json_default(value: Any) -> Any:
    """`value` as a JSON Schema `default`, or `None` when it has no JSON form."""
    if isinstance(value, enum.Enum):
        value = value.value
    try:
        return json.loads(json.dumps(value))
    except Exception:
        return None


class JsonSchemaBuilder:
    """Builds JSON Schemas of type hints, collecting nested TypedDicts and configspecs in `defs`."""

    def __init__(self, annotate: Optional[TAnnotate] = None) -> None:
        self.defs: Dict[str, Any] = {}
        self._types: Dict[str, Any] = {}
        self._annotate = annotate
        self._annotation_keywords: Set[str] = {"description", "writeOnly"}

    def hint_schema(self, hint: Any) -> Dict[str, Any]:
        """JSON Schema of `hint`, nested object types referenced from `defs`."""
        if get_origin(hint) in (NotRequired, Required):
            return self.hint_schema(get_args(hint)[0])
        if is_annotated(hint):
            return self._annotated_schema(hint)
        if is_newtype_type(hint):
            return self.hint_schema(hint.__supertype__)
        if hint is type(None):
            return {"type": "null"}
        if is_literal_type(hint):
            return self._enum_schema([json_default(v) for v in get_literal_args(hint)])
        if is_subclass(hint, enum.Enum):
            return self._enum_schema([json_default(member) for member in hint])
        if is_union_type(hint):
            return self._union_schema(hint)
        if is_typeddict(hint):
            return {"$ref": self._define(hint, self.typed_dict_schema)}
        if is_base_configuration_inner_hint(hint):
            ref = {"$ref": self._define(hint, self.spec_schema)}
            # credentials parse a native value, ie. a connection string, as well as their fields
            if _parses_native_value(hint):
                return {"anyOf": [ref, {"type": "string"}]}
            return ref
        if _is_pydantic_model(hint):
            return {"$ref": self._define(hint, self._pydantic_model_schema)}
        if is_list_generic_type(hint) or is_set_generic_type(hint):
            items = [a for a in get_args(hint) if a is not Ellipsis]
            if not items:
                return {"type": "array"}
            return {"type": "array", "items": self.hint_schema(items[0])}
        if is_dict_generic_type(hint):
            args = get_args(hint)
            values = self.hint_schema(args[1]) if len(args) == 2 else {}
            return {"type": "object", "additionalProperties": values or True}
        if hint in (list, tuple, set, frozenset):
            return {"type": "array"}
        if hint is dict:
            return {"type": "object"}
        if isinstance(hint, type):
            try:
                return dict(DATA_TYPE_SCHEMAS.get(py_type_to_sc_type(hint), {}))
            except TypeError:
                pass
        # `Any`, `CallableAny` and every type without a JSON form accept anything
        return {}

    def typed_dict_schema(self, td: Any) -> Dict[str, Any]:
        """Object schema of a TypedDict, `NotRequired` keys left out of `required`."""
        properties = {
            name: self.hint_schema(hint)
            for name, hint in get_type_hints(td, include_extras=True).items()
        }
        required = [name for name in properties if name in td.__required_keys__]
        schema = _object_schema(td.__name__, properties, required)
        # a class docstring is not inherited, so only the TypedDict's own one describes it
        if td.__doc__:
            schema["description"] = inspect.cleandoc(td.__doc__)
        return schema

    def spec_schema(
        self, spec: Type[BaseConfiguration], exclude: Collection[str] = ()
    ) -> Dict[str, Any]:
        """Object schema of a configspec, one property per field configuration resolves."""
        prototype = spec()
        properties: Dict[str, Any] = {}
        required: List[str] = []
        for name, hint in spec.get_resolvable_fields().items():
            if name in exclude:
                continue
            prop = self.hint_schema(hint)
            default = getattr(prototype, name, None)
            if default is not None:
                if (value := json_default(default)) is not None:
                    prop["default"] = value
            # the resolver's rule: a field that is not optional and has no value is missing
            elif not is_optional_type(hint):
                required.append(name)
            properties[name] = prop
        return _object_schema(spec.__name__, properties, required)

    def root_schema(self, hint: Any) -> Dict[str, Any]:
        """Self-contained schema of `hint`, an object type inlined at the root."""
        schema = self.hint_schema(hint)
        ref = schema.get("$ref")
        key = ref.rpartition("/")[2] if isinstance(ref, str) else None
        # the root definition is inlined unless a type inside it refers back to it
        if key and f'"{ref}"' not in json.dumps(self.defs):
            schema = self.defs.pop(key)
        return self.with_defs(schema)

    def with_defs(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """`schema` with the definitions it refers to, directly or through other definitions."""
        if self.defs:
            schema["$defs"] = dict(self.defs)
        prune_unreferenced_defs(schema)
        return schema

    def _annotated_schema(self, hint: Any) -> Dict[str, Any]:
        inner, *metadata = get_args(hint)
        schema = dict(self.hint_schema(inner))
        if (description := annotated_description(hint)) is not None:
            schema["description"] = description
        if SecretSentinel in metadata:
            schema["writeOnly"] = True
        for item in metadata if self._annotate else ():
            if keywords := self._annotate(item):
                self._annotation_keywords.update(keywords)
                schema.update(keywords)
        return schema

    def _union_schema(self, hint: Any) -> Dict[str, Any]:
        variants = [self.hint_schema(t) for t in extract_union_types(hint)]
        # a variant that accepts anything makes the whole union accept anything
        schema: Dict[str, Any] = {} if {} in variants else {"anyOf": variants}
        # `Optional[Annotated[T, ...]]` describes the field, so its annotations go on the field
        described = [v for v in variants if v.get("type") != "null"]
        if len(described) == 1:
            for key in self._annotation_keywords & described[0].keys():
                schema[key] = described[0].pop(key)
        return schema

    def _enum_schema(self, values: List[Any]) -> Dict[str, Any]:
        schema: Dict[str, Any] = {"enum": values}
        types = {self.hint_schema(type(v)).get("type") for v in values}
        if len(types) == 1 and isinstance(next(iter(types)), str):
            schema["type"] = types.pop()
        return schema

    def _define(self, hint: Any, build: Callable[[Any], Dict[str, Any]]) -> str:
        """Adds the schema of a named type once and returns the reference to it."""
        key = self._key(hint)
        if key not in self.defs:
            # registered before it is built, so a type referring to itself terminates
            self.defs[key] = {}
            self._types[key] = hint
            self.defs[key].update(build(hint))
        return f"#/$defs/{key}"

    def _key(self, hint: Any) -> str:
        # the type name, qualified by module when another type took it
        for key in (hint.__name__, f"{hint.__module__}__{hint.__qualname__}".replace(".", "__")):
            if self._types.get(key, hint) is hint:
                return key
        return f"{hint.__name__}__{id(hint)}"

    def _pydantic_model_schema(self, model: Any) -> Dict[str, Any]:
        schema: Dict[str, Any] = model.model_json_schema(ref_template="#/$defs/{model}")
        for key, definition in (schema.pop("$defs", None) or {}).items():
            self.defs.setdefault(key, definition)
        return schema


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


def _object_schema(title: str, properties: Dict[str, Any], required: List[str]) -> Dict[str, Any]:
    schema: Dict[str, Any] = {"title": title, "type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _parses_native_value(spec: Type[BaseConfiguration]) -> bool:
    return spec.parse_native_representation is not BaseConfiguration.parse_native_representation


def _is_pydantic_model(hint: Any) -> bool:
    # duck-typed, so pydantic is imported only by the model itself
    return isinstance(hint, type) and callable(getattr(hint, "model_json_schema", None))


__all__ = [
    "DATA_TYPE_SCHEMAS",
    "JsonSchemaBuilder",
    "TAnnotate",
    "annotated_description",
    "json_default",
    "prune_unreferenced_defs",
]
