import datetime  # noqa: I251
import enum
from typing import Any, Dict, List, Optional

import pytest

from dlt.common import Decimal, pendulum
from dlt.common.configuration.specs import (
    BaseConfiguration,
    ConnectionStringCredentials,
    configspec,
)
from dlt.common.reflection.json_schema import JsonSchemaBuilder
from dlt.common.typing import Annotated, Doc, NotRequired, TSecretStrValue, TypedDict


class _Size(enum.IntEnum):
    SMALL = 1
    LARGE = 2


class _Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class _Tag(str):
    pass


@pytest.mark.parametrize(
    "hint,expected",
    [
        (pendulum.DateTime, {"type": "string", "format": "date-time"}),
        (datetime.date, {"type": "string", "format": "date"}),
        (_Tag, {"type": "string"}),
        (_Size, {"enum": [1, 2], "type": "integer"}),
        (Decimal, {"type": ["number", "string"]}),
        (bytes, {"type": "string", "contentEncoding": "base64"}),
        (TSecretStrValue, {"type": "string", "writeOnly": True}),
        (List[int], {"type": "array", "items": {"type": "integer"}}),
        (Dict[str, int], {"type": "object", "additionalProperties": {"type": "integer"}}),
        (Optional[Any], {}),
    ],
    ids=[
        "pendulum-subclass",
        "date",
        "str-subclass",
        "int-enum",
        "decimal",
        "bytes",
        "secret",
        "list-of-int",
        "dict-of-int",
        "any",
    ],
)
def test_scalar_and_container_hints(hint: Any, expected: Dict[str, Any]) -> None:
    assert JsonSchemaBuilder().hint_schema(hint) == expected


@configspec
class _Settings(BaseConfiguration):
    name: str = None
    color: _Color = _Color.BLUE
    amount: Decimal = Decimal("1.5")
    note: Optional[str] = None


def test_spec_fields_take_defaults_and_the_resolvers_required_rule() -> None:
    schema = JsonSchemaBuilder().spec_schema(_Settings)
    properties = schema["properties"]

    # a field without a value that is not optional is required
    assert schema["required"] == ["name"]
    # an enum default is its value, a decimal default keeps every digit
    assert properties["color"] == {"enum": ["red", "blue"], "type": "string", "default": "blue"}
    assert properties["amount"] == {"type": ["number", "string"], "default": "1.5"}
    assert properties["note"] == {"anyOf": [{"type": "string"}, {"type": "null"}]}


class _Evidence(TypedDict):
    """A fact that supports the diagnosis.

    Quote it as found.
    """

    source: Annotated[str, Doc("where it was found")]
    run_id: NotRequired[str]


class _Nested(TypedDict):
    """The diagnosis."""

    main: _Evidence
    items: List[_Evidence]
    maybe: Optional[_Evidence]
    by_key: NotRequired[Dict[str, _Evidence]]


class _Node(TypedDict):
    children: List["_Node"]


def test_typed_dicts_nest_however_they_are_embedded() -> None:
    schema = JsonSchemaBuilder().root_schema(_Nested)

    evidence = schema["$defs"]["_Evidence"]
    assert evidence["properties"]["source"]["description"] == "where it was found"
    assert evidence["required"] == ["source"]
    # the class docstring describes the type, at the root and in a definition
    assert schema["description"] == "The diagnosis."
    assert evidence["description"] == "A fact that supports the diagnosis.\n\nQuote it as found."
    assert "description" not in JsonSchemaBuilder().root_schema(_Node)["$defs"]["_Node"]
    # direct, in a list, optional and as dict values: every embedding is one definition
    ref = {"$ref": "#/$defs/_Evidence"}
    properties = schema["properties"]
    assert properties["main"] == ref
    assert properties["items"] == {"type": "array", "items": ref}
    assert properties["maybe"] == {"anyOf": [ref, {"type": "null"}]}
    assert properties["by_key"] == {"type": "object", "additionalProperties": ref}
    assert schema["required"] == ["main", "items", "maybe"]

    # a type referring to itself stays a definition, the root refers to it
    node = JsonSchemaBuilder().root_schema(_Node)
    assert node["$ref"] == "#/$defs/_Node"
    assert node["$defs"]["_Node"]["properties"]["children"]["items"] == {"$ref": "#/$defs/_Node"}


def test_same_named_types_keep_their_own_definitions() -> None:
    Item = TypedDict("Item", {"a": Annotated[str, Doc("from one module")]})
    Other = TypedDict("Item", {"b": Annotated[str, Doc("from another")]})  # type: ignore[name-match]
    Other.__module__ = "elsewhere"
    Both = TypedDict("Both", {"one": Item, "two": Other})

    defs = JsonSchemaBuilder().root_schema(Both)["$defs"]

    assert len(defs) == 2
    described = sorted(
        field["description"] for item in defs.values() for field in item["properties"].values()
    )
    assert described == ["from another", "from one module"]


class _Window(TypedDict):
    start: datetime.datetime
    amount: NotRequired[Decimal]


@configspec
class _Destination(BaseConfiguration):
    credentials: ConnectionStringCredentials = None
    dataset: Optional[str] = None


class _Route(TypedDict):
    destination: _Destination
    tables: List[str]


@configspec
class _Source(BaseConfiguration):
    windows: List[_Window] = None
    destination: Optional[_Destination] = None


class _Job(TypedDict):
    source: _Source
    routes: List[_Route]


def test_configspecs_and_typed_dicts_nest_in_each_other() -> None:
    """A TypedDict holding configspecs and lists of TypedDicts that hold configspecs again."""
    schema = JsonSchemaBuilder().root_schema(_Job)
    defs = schema["$defs"]

    assert set(defs) == {
        "_Source",
        "_Window",
        "_Destination",
        "_Route",
        "ConnectionStringCredentials",
    }
    assert schema["properties"]["routes"] == {"type": "array", "items": {"$ref": "#/$defs/_Route"}}
    # a configspec holding a list of TypedDicts and an optional configspec
    source = defs["_Source"]["properties"]
    assert source["windows"] == {"type": "array", "items": {"$ref": "#/$defs/_Window"}}
    assert source["destination"] == {"anyOf": [{"$ref": "#/$defs/_Destination"}, {"type": "null"}]}
    assert defs["_Source"]["required"] == ["windows"]
    # a TypedDict holding a configspec that holds credentials
    assert defs["_Route"]["properties"]["destination"] == {"$ref": "#/$defs/_Destination"}
    credentials = defs["_Destination"]["properties"]["credentials"]
    assert credentials == {
        "anyOf": [{"$ref": "#/$defs/ConnectionStringCredentials"}, {"type": "string"}]
    }
    assert defs["_Destination"]["required"] == ["credentials"]
    password = defs["ConnectionStringCredentials"]["properties"]["password"]
    assert password == {"anyOf": [{"type": "string"}, {"type": "null"}], "writeOnly": True}

    # the references resolve for a validator, through every level
    jsonschema = pytest.importorskip("jsonschema")
    validator = jsonschema.Draft202012Validator(schema)
    destination = {"credentials": "duckdb:///x.db"}
    route: Dict[str, Any] = {"destination": destination, "tables": ["a", "b"]}
    job = {
        "source": {"windows": [{"start": "2026-10-06T00:00:00Z"}], "destination": destination},
        "routes": [route],
    }
    validator.validate(job)
    # a list item breaking its definition fails the whole document
    route["tables"] = "a"
    assert not validator.is_valid(job)


class _Marker:
    def __init__(self, value: str) -> None:
        self.value = value


class _Row(TypedDict):
    row_id: Optional[Annotated[str, _Marker("key"), Doc("row id")]]
    tags: List[str]


def test_annotations_of_an_optional_field_describe_the_field() -> None:
    builder = JsonSchemaBuilder(
        annotate=lambda m: {"x-marker": m.value} if isinstance(m, _Marker) else None
    )

    schema = builder.root_schema(_Row)

    assert schema["title"] == "_Row"
    assert schema["properties"]["row_id"] == {
        "anyOf": [{"type": "string"}, {"type": "null"}],
        "description": "row id",
        "x-marker": "key",
    }
    assert schema["required"] == ["row_id", "tags"]
    assert "$defs" not in schema
