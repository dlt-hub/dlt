"""Tests for resource validation with pydantic schema
"""
import typing as t

import pytest
import dlt
from dlt.common.schema.typing import ColumnValidator
from dlt.extract.validation import PydanticValidator
from dlt.extract.exceptions import ValidationError, ResourceExtractionError

from pydantic import BaseModel


class SimpleModel(BaseModel):
    a: int
    b: str


def test_validator_model_in_decorator() -> None:
    # model passed in decorator
    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[t.Dict[str, t.Any]]:
        yield {"a": 1, "b": "2"}
        yield {"a": 2, "b": "3"}

    # Items are passed through model
    data = list(some_data())
    assert data == [SimpleModel(a=1, b="2"), SimpleModel(a=2, b="3")]



def test_validator_model_in_apply_hints() -> None:
    # model passed in apply_hints

    @dlt.resource
    def some_data() -> t.Iterator[t.Dict[str, t.Any]]:
        yield {"a": 1, "b": "2"}
        yield {"a": 2, "b": "3"}

    resource = some_data()
    resource.apply_hints(columns=SimpleModel)

    # Items are passed through model
    data = list(resource)
    assert data == [SimpleModel(a=1, b="2"), SimpleModel(a=2, b="3")]


def test_remove_validator() -> None:

    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[t.Dict[str, t.Any]]:
        yield {"a": 1, "b": "2"}
        yield {"a": 2, "b": "3"}

    resource = some_data()
    resource.validator = None

    data = list(resource)
    assert data == [{"a": 1, "b": "2"}, {"a": 2, "b": "3"}]


def test_replace_validator_model() -> None:

    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[t.Dict[str, t.Any]]:
        yield {"a": 1, "b": "2"}
        yield {"a": 2, "b": "3"}

    resource = some_data()

    class AnotherModel(BaseModel):
        a: int
        b: str
        c: float = 0.5

    # Use apply_hints to replace the validator
    resource.apply_hints(columns=AnotherModel)

    data = list(resource)
    # Items are validated with the new model
    assert data == [AnotherModel(a=1, b="2", c=0.5), AnotherModel(a=2, b="3", c=0.5)]

    # Ensure only one validator is applied in steps
    steps = resource._pipe.steps
    assert len(steps) == 2

    assert isinstance(steps[-1], ColumnValidator)
    assert steps[-1].model is AnotherModel


def test_validator_property_setter() -> None:
    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[t.Dict[str, t.Any]]:
        yield {"a": 1, "b": "2"}
        yield {"a": 2, "b": "3"}

    resource = some_data()

    assert isinstance(resource.validator, PydanticValidator) and resource.validator.model is SimpleModel

    class AnotherModel(BaseModel):
        a: int
        b: str
        c: float = 0.5

    resource.validator = PydanticValidator(AnotherModel)

    assert resource.validator and resource.validator.model is AnotherModel

    data = list(resource)
    # Items are validated with the new model
    assert data == [AnotherModel(a=1, b="2", c=0.5), AnotherModel(a=2, b="3", c=0.5)]


def test_failed_validation() -> None:
    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[t.Dict[str, t.Any]]:
        yield {"a": 1, "b": "z"}
        # yield item that fails schema validation
        yield {"a": "not_int", "b": "x"}

    # extraction fails with ValidationError
    with pytest.raises(ResourceExtractionError) as exinfo:
        list(some_data())

    assert isinstance(exinfo.value.__cause__, ValidationError)
