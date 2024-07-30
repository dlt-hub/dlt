"""Tests for resource validation with pydantic schema
"""
import typing as t
import pytest

import dlt
from dlt.common import json
from dlt.common.schema.exceptions import DataValidationError
from dlt.common.typing import TDataItems
from dlt.common.libs.pydantic import BaseModel

from dlt.extract import DltResource
from dlt.extract.items import ValidateItem
from dlt.extract.validation import PydanticValidator
from dlt.extract.exceptions import ResourceExtractionError
from dlt.pipeline.exceptions import PipelineStepFailed


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
    assert json.dumpb(data) == json.dumpb(
        [AnotherModel(a=1, b="2", c=0.5), AnotherModel(a=2, b="3", c=0.5)]
    )

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

    assert isinstance(
        resource.validator, PydanticValidator
    ) and resource.validator.model.__name__.startswith(SimpleModel.__name__)

    class AnotherModel(BaseModel):
        a: int
        b: str
        c: float = 0.5

    resource.validator = PydanticValidator(AnotherModel, column_mode="freeze", data_mode="freeze")

    assert resource.validator and resource.validator.model.__name__.startswith(
        AnotherModel.__name__
    )

    data = list(resource)
    # Items are validated with the new model
    assert json.dumpb(data) == json.dumpb(
        [AnotherModel(a=1, b="2", c=0.5), AnotherModel(a=2, b="3", c=0.5)]
    )


@pytest.mark.parametrize("yield_list", [True, False])
def test_default_validation(yield_list: bool) -> None:
    @dlt.resource(columns=SimpleModel)
    def some_data() -> t.Iterator[TDataItems]:
        # yield item that fails schema validation
        items = [{"a": 1, "b": "z"}, {"a": "not_int", "b": "x"}]
        if yield_list:
            yield items
        else:
            yield from items

    # some_data must have default Pydantic schema contract
    assert some_data().schema_contract == {
        "tables": "evolve",
        "columns": "discard_value",
        "data_type": "freeze",
    }

    # extraction fails with ValidationError
    with pytest.raises(ResourceExtractionError) as exinfo:
        list(some_data())

    val_ex = exinfo.value.__cause__
    assert isinstance(val_ex, DataValidationError)
    assert val_ex.schema_name is None
    assert val_ex.table_name == "some_data"
    assert val_ex.column_name == "('items', 1, 'a')" if yield_list else "('a',)"
    assert val_ex.data_item == {"a": "not_int", "b": "x"}
    assert val_ex.schema_entity == "data_type"

    # fail in pipeline
    @dlt.resource(columns=SimpleModel)
    def some_data_extra() -> t.Iterator[TDataItems]:
        # yield item that fails schema validation
        items = [{"a": 1, "b": "z", "c": 1.3}, {"a": "not_int", "b": "x"}]
        if yield_list:
            yield items
        else:
            yield from items

    pipeline = dlt.pipeline()
    with pytest.raises(PipelineStepFailed) as py_ex:
        pipeline.extract(some_data_extra())
    assert isinstance(py_ex.value.__cause__, ResourceExtractionError)
    assert isinstance(py_ex.value.__cause__.__cause__, DataValidationError)
    val_ex = py_ex.value.__cause__.__cause__
    assert val_ex.table_name == "some_data_extra"
    assert val_ex.schema_entity == "data_type"  # extra field is the cause
    assert val_ex.data_item == {"a": "not_int", "b": "x"}


@pytest.mark.parametrize("yield_list", [True, False])
def test_validation_with_contracts(yield_list: bool) -> None:
    def some_data() -> t.Iterator[TDataItems]:
        # yield item that fails schema validation
        items = [{"a": 1, "b": "z"}, {"a": "not_int", "b": "x"}, {"c": "not_int"}]
        if yield_list:
            yield items
        else:
            yield from items

    # let it evolve
    r: DltResource = dlt.resource(some_data(), schema_contract="evolve", columns=SimpleModel)
    validator: PydanticValidator[SimpleModel] = r.validator  # type: ignore[assignment]
    assert validator.column_mode == "evolve"
    assert validator.data_mode == "evolve"
    assert validator.model.__name__.endswith("AnyExtraAllow")
    items = list(r)
    assert len(items) == 3
    # fully valid
    assert items[0]["a"] == 1
    assert items[0]["b"] == "z"
    # data type not valid
    assert items[1]["a"] == "not_int"
    assert items[1]["b"] == "x"
    # extra attr and data invalid
    assert items[2]["a"] is None
    assert items[2]["b"] is None
    assert items[2]["c"] == "not_int"

    # let it drop
    r = dlt.resource(some_data(), schema_contract="discard_row", columns=SimpleModel)
    validator = r.validator  # type: ignore[assignment]
    assert validator.column_mode == "discard_row"
    assert validator.data_mode == "discard_row"
    assert validator.model.__name__.endswith("ExtraForbid")
    items = list(r)
    assert len(items) == 1
    assert items[0]["a"] == 1
    assert items[0]["b"] == "z"

    # filter just offending values
    with pytest.raises(NotImplementedError):
        # pydantic data_type cannot be discard_value
        dlt.resource(some_data(), schema_contract="discard_value", columns=SimpleModel)
    r = dlt.resource(
        some_data(),
        schema_contract={"columns": "discard_value", "data_type": "evolve"},
        columns=SimpleModel,
    )
    validator = r.validator  # type: ignore[assignment]
    assert validator.column_mode == "discard_value"
    assert validator.data_mode == "evolve"
    # ignore is the default so no Extra in name
    assert validator.model.__name__.endswith("Any")
    items = list(r)
    assert len(items) == 3
    # c is gone from the last model
    assert "c" not in items[2]
