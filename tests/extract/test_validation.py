"""Tests for resource validation with pydantic schema
"""
import typing as t
import pytest

import dlt
from dlt.common import json
from dlt.common.typing import TDataItems
from dlt.common.libs.pydantic import BaseModel, FullValidationError, ValidationError

from dlt.extract.typing import ValidateItem
from dlt.extract.validation import PydanticValidator
from dlt.extract.exceptions import ResourceExtractionError


class SimpleModel(BaseModel):
    a: int
    b: str


@pytest.mark.parametrize("yield_list", [True, False])
def test_validator_model_in_decorator(yield_list: bool) -> None:
    # model passed in decorator
    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[TDataItems]:
        items = [{"a": 1, "b": "2"}, {"a": 2, "b": "3"}]
        if yield_list:
            yield items
        else:
            yield from items

    # Items are passed through model
    data = list(some_data())
    # compare content-wise. model names change due to extra settings on columns
    assert json.dumpb(data) == json.dumpb([SimpleModel(a=1, b="2"), SimpleModel(a=2, b="3")])


@pytest.mark.parametrize("yield_list", [True, False])
def test_validator_model_in_apply_hints(yield_list: bool) -> None:
    # model passed in apply_hints

    @dlt.resource
    def some_data() -> t.Iterator[TDataItems]:
        items = [{"a": 1, "b": "2"}, {"a": 2, "b": "3"}]
        if yield_list:
            yield items
        else:
            yield from items

    resource = some_data()
    resource.apply_hints(columns=SimpleModel)

    # Items are passed through model
    data = list(resource)
    assert json.dumpb(data) == json.dumpb([SimpleModel(a=1, b="2"), SimpleModel(a=2, b="3")])


@pytest.mark.parametrize("yield_list", [True, False])
def test_remove_validator(yield_list: bool) -> None:

    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[TDataItems]:
        items = [{"a": 1, "b": "2"}, {"a": 2, "b": "3"}]
        if yield_list:
            yield items
        else:
            yield from items

    resource = some_data()
    resource.validator = None

    data = list(resource)
    assert json.dumpb(data) == json.dumpb([{"a": 1, "b": "2"}, {"a": 2, "b": "3"}])


@pytest.mark.parametrize("yield_list", [True, False])
def test_replace_validator_model(yield_list: bool) -> None:

    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[TDataItems]:
        items = [{"a": 1, "b": "2"}, {"a": 2, "b": "3"}]
        if yield_list:
            yield items
        else:
            yield from items

    resource = some_data()

    class AnotherModel(BaseModel):
        a: int
        b: str
        c: float = 0.5

    # Use apply_hints to replace the validator
    resource.apply_hints(columns=AnotherModel)

    data = list(resource)
    # Items are validated with the new model
    assert json.dumpb(data) == json.dumpb([AnotherModel(a=1, b="2", c=0.5), AnotherModel(a=2, b="3", c=0.5)])

    # Ensure only one validator is applied in steps
    steps = resource._pipe.steps
    assert len(steps) == 2

    assert isinstance(steps[-1], ValidateItem)
    # model name will change according to extra items handling
    assert steps[-1].model.__name__.startswith(AnotherModel.__name__)  # type: ignore[attr-defined]


@pytest.mark.parametrize("yield_list", [True, False])
def test_validator_property_setter(yield_list: bool) -> None:

    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[TDataItems]:
        items = [{"a": 1, "b": "2"}, {"a": 2, "b": "3"}]
        if yield_list:
            yield items
        else:
            yield from items

    resource = some_data()

    assert isinstance(resource.validator, PydanticValidator) and resource.validator.model.__name__.startswith(SimpleModel.__name__)

    class AnotherModel(BaseModel):
        a: int
        b: str
        c: float = 0.5

    resource.validator = PydanticValidator(AnotherModel, column_mode="freeze", data_mode="freeze")

    assert resource.validator and resource.validator.model.__name__.startswith(AnotherModel.__name__)

    data = list(resource)
    # Items are validated with the new model
    assert json.dumpb(data) == json.dumpb([AnotherModel(a=1, b="2", c=0.5), AnotherModel(a=2, b="3", c=0.5)])


@pytest.mark.parametrize("yield_list", [True, False])
def test_failed_validation(yield_list: bool) -> None:
    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[TDataItems]:
        # yield item that fails schema validation
        items = [{"a": 1, "b": "z"}, {"a": "not_int", "b": "x"}]
        if yield_list:
            yield items
        else:
            yield from items

    # extraction fails with ValidationError
    with pytest.raises(ResourceExtractionError) as exinfo:
        list(some_data())

    assert isinstance(exinfo.value.__cause__, FullValidationError)
    # assert str(PydanticValidator(SimpleModel)) in str(exinfo.value)
