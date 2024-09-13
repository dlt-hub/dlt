import dlt, os, pytest
import contextlib
from typing import Any, Callable, Iterator, Union, Optional, Type

from dlt.common.schema.typing import TSchemaContract
from dlt.common.utils import uniq_id
from dlt.common.schema.exceptions import DataValidationError

from dlt.extract import DltResource
from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.extract.exceptions import ResourceExtractionError

from tests.pipeline.utils import load_table_counts
from tests.utils import (
    TestDataItemFormat,
    skip_if_not_active,
    data_to_item_format,
    ALL_TEST_DATA_ITEM_FORMATS,
)

skip_if_not_active("duckdb")

SCHEMA_CONTRACT = ["evolve", "discard_value", "discard_row", "freeze"]
LOCATIONS = ["source", "resource", "override"]
SCHEMA_ELEMENTS = ["tables", "columns", "data_type"]


OLD_COLUMN_NAME = "name"
NEW_COLUMN_NAME = "new_col"
VARIANT_COLUMN_NAME = "some_int__v_text"
SUBITEMS_TABLE = "items__sub_items"
NEW_ITEMS_TABLE = "new_items"
ITEMS_TABLE = "items"


@contextlib.contextmanager
def raises_step_exception(check_raise: bool = True, expected_nested_error: Type[Any] = None) -> Any:
    expected_nested_error = expected_nested_error or DataValidationError
    if not check_raise:
        yield
        return
    with pytest.raises(PipelineStepFailed) as py_exc:
        yield
    if py_exc.value.step == "extract":
        print(type(py_exc.value.__context__))
        assert isinstance(py_exc.value.__context__, expected_nested_error)
    else:
        # normalize
        assert isinstance(py_exc.value.__context__.__context__, expected_nested_error)


def items(settings: TSchemaContract) -> Any:
    # NOTE: names must be normalizeds
    @dlt.resource(name="Items", write_disposition="append", schema_contract=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {"id": index, "SomeInt": 1, "name": f"item {index}"}

    return load_items


def items_with_variant(settings: TSchemaContract) -> Any:
    @dlt.resource(name="Items", write_disposition="append", schema_contract=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {"id": index, "name": f"item {index}", "SomeInt": "hello"}

    return load_items


def items_with_new_column(settings: TSchemaContract) -> Any:
    @dlt.resource(name="Items", write_disposition="append", schema_contract=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {"id": index, "name": f"item {index}", "New^Col": "hello"}

    return load_items


def items_with_subtable(settings: TSchemaContract) -> Any:
    @dlt.resource(name="Items", write_disposition="append", schema_contract=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {
                "id": index,
                "name": f"item {index}",
                "sub_items": [
                    {"id": index + 1000, "SomeInt": 5, "name": f"sub item {index + 1000}"}
                ],
            }

    return load_items


def items_with_new_column_in_subtable(settings: TSchemaContract) -> Any:
    @dlt.resource(name="Items", write_disposition="append", schema_contract=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {
                "id": index,
                "name": f"item {index}",
                "sub_items": [
                    {"id": index + 1000, "name": f"sub item {index + 1000}", "New^Col": "hello"}
                ],
            }

    return load_items


def items_with_variant_in_subtable(settings: TSchemaContract) -> Any:
    @dlt.resource(name="Items", write_disposition="append", schema_contract=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {
                "id": index,
                "name": f"item {index}",
                "sub_items": [
                    {"id": index + 1000, "name": f"sub item {index + 1000}", "SomeInt": "hello"}
                ],
            }

    return load_items


def new_items(settings: TSchemaContract) -> Any:
    @dlt.resource(name=NEW_ITEMS_TABLE, write_disposition="append", schema_contract=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {"id": index, "some_int": 1, "name": f"item {index}"}

    return load_items


def run_resource(
    pipeline: Pipeline,
    resource_fun: Callable[..., DltResource],
    settings: Any,
    item_format: TestDataItemFormat = "object",
    duplicates: int = 1,
) -> None:
    for item in settings.keys():
        assert item in LOCATIONS
        ev_settings = settings[item]
        if ev_settings in SCHEMA_CONTRACT:
            continue
        for key, val in ev_settings.items():
            assert val in SCHEMA_CONTRACT
            assert key in SCHEMA_ELEMENTS

    @dlt.source(name="freeze_tests", schema_contract=settings.get("source"))
    def source() -> Iterator[DltResource]:
        for idx in range(duplicates):
            resource: DltResource = resource_fun(settings.get("resource"))
            if item_format != "object":
                resource._pipe.replace_gen(data_to_item_format(item_format, resource._pipe.gen()))  # type: ignore
            resource.table_name = resource.name
            yield resource.with_name(resource.name + str(idx))

    # run pipeline
    pipeline.run(source(), schema_contract=settings.get("override"))

    # check global settings
    assert pipeline.default_schema._settings.get("schema_contract", None) == (
        settings.get("override") or settings.get("source")
    )

    # check items table settings
    # assert pipeline.default_schema.tables[ITEMS_TABLE].get("schema_contract", {}) == (settings.get("resource") or {})

    # check effective table settings
    # assert resolve_contract_settings_for_table(None, "items", pipeline.default_schema) == expand_schema_contract_settings(settings.get("resource") or settings.get("override") or "evolve")


def get_pipeline():
    import duckdb

    return dlt.pipeline(
        pipeline_name="contracts_" + uniq_id(),
        destination=dlt.destinations.duckdb(credentials=duckdb.connect(":memory:")),
        dev_mode=True,
    )


@pytest.mark.parametrize("contract_setting", SCHEMA_CONTRACT)
@pytest.mark.parametrize("setting_location", LOCATIONS)
@pytest.mark.parametrize("item_format", ALL_TEST_DATA_ITEM_FORMATS)
def test_new_tables(
    contract_setting: str, setting_location: str, item_format: TestDataItemFormat
) -> None:
    pipeline = get_pipeline()

    full_settings = {setting_location: {"tables": contract_setting}}
    run_resource(pipeline, items, {}, item_format)
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 10
    assert OLD_COLUMN_NAME in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]

    run_resource(pipeline, items_with_new_column, full_settings, item_format)
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 20
    assert NEW_COLUMN_NAME in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]

    # test adding new table
    with raises_step_exception(contract_setting == "freeze"):
        run_resource(pipeline, new_items, full_settings, item_format)
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts.get(NEW_ITEMS_TABLE, 0) == (10 if contract_setting in ["evolve"] else 0)
    # delete extracted files if left after exception
    pipeline.drop_pending_packages()

    # NOTE: arrow / pandas do not support variants and subtables so we must skip
    if item_format == "object":
        # run add variant column
        run_resource(pipeline, items_with_variant, full_settings)
        table_counts = load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
        )
        assert table_counts[ITEMS_TABLE] == 30
        assert VARIANT_COLUMN_NAME in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]

        # test adding new subtable
        with raises_step_exception(contract_setting == "freeze"):
            run_resource(pipeline, items_with_subtable, full_settings)

        table_counts = load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
        )
        assert table_counts[ITEMS_TABLE] == 30 if contract_setting in ["freeze"] else 40
        assert table_counts.get(SUBITEMS_TABLE, 0) == (10 if contract_setting in ["evolve"] else 0)


@pytest.mark.parametrize("contract_setting", SCHEMA_CONTRACT)
@pytest.mark.parametrize("setting_location", LOCATIONS)
@pytest.mark.parametrize("item_format", ALL_TEST_DATA_ITEM_FORMATS)
def test_new_columns(
    contract_setting: str, setting_location: str, item_format: TestDataItemFormat
) -> None:
    full_settings = {setting_location: {"columns": contract_setting}}

    pipeline = get_pipeline()
    run_resource(pipeline, items, {}, item_format)
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 10
    assert OLD_COLUMN_NAME in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]

    # new should work
    run_resource(pipeline, new_items, full_settings, item_format)
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    expected_items_count = 10
    assert table_counts[ITEMS_TABLE] == expected_items_count
    assert table_counts[NEW_ITEMS_TABLE] == 10

    # test adding new column twice: filter will try to catch it before it is added for the second time
    with raises_step_exception(contract_setting == "freeze"):
        run_resource(pipeline, items_with_new_column, full_settings, item_format, duplicates=2)
    # delete extracted files if left after exception
    pipeline.drop_pending_packages()

    if contract_setting == "evolve":
        assert NEW_COLUMN_NAME in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]
    else:
        assert NEW_COLUMN_NAME not in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    expected_items_count += 20 if contract_setting in ["evolve", "discard_value"] else 0
    assert table_counts[ITEMS_TABLE] == expected_items_count

    # NOTE: arrow / pandas do not support variants and subtables so we must skip
    if item_format == "object":
        # subtable should work
        run_resource(pipeline, items_with_subtable, full_settings)
        table_counts = load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
        )
        expected_items_count += 10
        expected_subtable_items_count = 10
        assert table_counts[ITEMS_TABLE] == expected_items_count
        assert table_counts[SUBITEMS_TABLE] == expected_subtable_items_count

        # test adding variant column
        run_resource(pipeline, items_with_variant, full_settings)
        # variants are not new columns and should be able to always evolve
        assert VARIANT_COLUMN_NAME in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]
        table_counts = load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
        )
        expected_items_count += 10
        assert table_counts[ITEMS_TABLE] == expected_items_count

        # test adding new column in subtable (subtable exists already)
        with raises_step_exception(contract_setting == "freeze"):
            run_resource(pipeline, items_with_new_column_in_subtable, full_settings, item_format)
        # delete extracted files if left after exception
        pipeline.drop_pending_packages()
        table_counts = load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
        )
        # main table only does not get loaded on freeze exception
        expected_items_count += 0 if contract_setting in ["freeze"] else 10
        # subtable gets loaded on evolve and discard
        expected_subtable_items_count += (
            10 if contract_setting in ["evolve", "discard_value"] else 0
        )
        assert table_counts[ITEMS_TABLE] == expected_items_count
        assert table_counts[SUBITEMS_TABLE] == expected_subtable_items_count
        # new column may only appear in evolve mode
        if contract_setting == "evolve":
            assert NEW_COLUMN_NAME in pipeline.default_schema.tables[SUBITEMS_TABLE]["columns"]
        else:
            assert NEW_COLUMN_NAME not in pipeline.default_schema.tables[SUBITEMS_TABLE]["columns"]

        # loading variant column will always work in subtable
        run_resource(pipeline, items_with_variant_in_subtable, full_settings, item_format)
        table_counts = load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
        )
        expected_subtable_items_count += 10
        expected_items_count += 10
        assert table_counts[ITEMS_TABLE] == expected_items_count
        assert table_counts[SUBITEMS_TABLE] == expected_subtable_items_count
        assert VARIANT_COLUMN_NAME in pipeline.default_schema.tables[SUBITEMS_TABLE]["columns"]


@pytest.mark.parametrize("contract_setting", SCHEMA_CONTRACT)
@pytest.mark.parametrize("setting_location", LOCATIONS)
def test_variant_columns(contract_setting: str, setting_location: str) -> None:
    full_settings = {setting_location: {"data_type": contract_setting}}
    pipeline = get_pipeline()
    run_resource(pipeline, items, {})
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    expected_items_count = 10
    expected_subtable_items_count = 0
    assert table_counts[ITEMS_TABLE] == expected_items_count
    assert OLD_COLUMN_NAME in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]

    # subtable should work
    run_resource(pipeline, items_with_subtable, full_settings)
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    expected_items_count += 10
    expected_subtable_items_count += 10
    assert table_counts[ITEMS_TABLE] == expected_items_count
    assert table_counts[SUBITEMS_TABLE] == expected_subtable_items_count

    # new should work
    run_resource(pipeline, new_items, full_settings)
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == expected_items_count
    assert table_counts[SUBITEMS_TABLE] == expected_subtable_items_count
    assert table_counts[NEW_ITEMS_TABLE] == 10

    # test adding new column
    run_resource(pipeline, items_with_new_column, full_settings)
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    expected_items_count += 10
    assert table_counts[ITEMS_TABLE] == expected_items_count
    assert table_counts[SUBITEMS_TABLE] == expected_subtable_items_count
    assert NEW_COLUMN_NAME in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]

    # test adding variant column
    with raises_step_exception(contract_setting == "freeze"):
        run_resource(pipeline, items_with_variant, full_settings)
    pipeline.drop_pending_packages()

    if contract_setting == "evolve":
        assert VARIANT_COLUMN_NAME in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]
    else:
        assert VARIANT_COLUMN_NAME not in pipeline.default_schema.tables[ITEMS_TABLE]["columns"]
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    expected_items_count += 10 if contract_setting in ["evolve", "discard_value"] else 0
    assert table_counts[ITEMS_TABLE] == expected_items_count

    # test adding new column in subtable (subtable exists already)
    run_resource(pipeline, items_with_new_column_in_subtable, full_settings)
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    expected_items_count += 10
    expected_subtable_items_count += 10
    assert table_counts[ITEMS_TABLE] == expected_items_count
    assert table_counts[SUBITEMS_TABLE] == expected_subtable_items_count
    assert NEW_COLUMN_NAME in pipeline.default_schema.tables[SUBITEMS_TABLE]["columns"]

    # loading variant column will always work in subtable
    with raises_step_exception(contract_setting == "freeze"):
        run_resource(pipeline, items_with_variant_in_subtable, full_settings)
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    # main table only does not get loaded on freeze exception
    expected_items_count += 0 if contract_setting in ["freeze"] else 10
    # subtable gets loaded on evolve and discard
    expected_subtable_items_count += 10 if contract_setting in ["evolve", "discard_value"] else 0
    assert table_counts[ITEMS_TABLE] == expected_items_count
    assert table_counts[SUBITEMS_TABLE] == expected_subtable_items_count
    # new column may only appear in evolve mode
    if contract_setting == "evolve":
        assert VARIANT_COLUMN_NAME in pipeline.default_schema.tables[SUBITEMS_TABLE]["columns"]
    else:
        assert VARIANT_COLUMN_NAME not in pipeline.default_schema.tables[SUBITEMS_TABLE]["columns"]


def test_settings_precedence() -> None:
    pipeline = get_pipeline()

    # load some data
    run_resource(pipeline, items, {})

    # trying to add new column when forbidden on resource will fail
    run_resource(pipeline, items_with_new_column, {"resource": {"columns": "discard_row"}})

    # when allowed on override it will work
    run_resource(
        pipeline,
        items_with_new_column,
        {"resource": {"columns": "freeze"}, "override": {"columns": "evolve"}},
    )


def test_settings_precedence_2() -> None:
    pipeline = get_pipeline()

    # load some data
    run_resource(pipeline, items, {"source": {"data_type": "discard_row"}})
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 10

    # trying to add variant when forbidden on source will fail
    run_resource(pipeline, items_with_variant, {"source": {"data_type": "discard_row"}})
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 10

    # if allowed on resource it will pass
    run_resource(
        pipeline,
        items_with_variant,
        {"resource": {"data_type": "evolve"}, "source": {"data_type": "discard_row"}},
    )
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 20

    # if allowed on override it will also pass
    run_resource(
        pipeline,
        items_with_variant,
        {
            "resource": {"data_type": "discard_row"},
            "source": {"data_type": "discard_row"},
            "override": {"data_type": "evolve"},
        },
    )
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 30


@pytest.mark.parametrize("setting_location", LOCATIONS)
def test_change_mode(setting_location: str) -> None:
    pipeline = get_pipeline()

    # load some data
    run_resource(pipeline, items, {})
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 10

    # trying to add variant when forbidden will fail
    run_resource(pipeline, items_with_variant, {setting_location: {"data_type": "discard_row"}})
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 10

    # now allow
    run_resource(pipeline, items_with_variant, {setting_location: {"data_type": "evolve"}})
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 20


@pytest.mark.parametrize("setting_location", LOCATIONS)
def test_single_settings_value(setting_location: str) -> None:
    pipeline = get_pipeline()

    run_resource(pipeline, items, {})
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 10

    # trying to add variant when forbidden will fail
    run_resource(pipeline, items_with_variant, {setting_location: "discard_row"})
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 10

    # trying to add new column will fail
    run_resource(pipeline, items_with_new_column, {setting_location: "discard_row"})
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 10

    # trying to add new table will fail
    run_resource(pipeline, new_items, {setting_location: "discard_row"})
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 10
    assert NEW_ITEMS_TABLE not in table_counts


def test_data_contract_interaction() -> None:
    """
    ensure data contracts with pydantic are enforced properly
    """
    from pydantic import BaseModel, Extra

    class Items(BaseModel):
        id: int  # noqa: A003
        name: Optional[str]
        amount: Union[int, str, None]

        class Config:
            extra = Extra.forbid

    @dlt.resource(name="items")
    def get_items():
        yield from [
            {
                "id": 5,
                "name": "dave",
                "amount": 6,
            }
        ]

    @dlt.resource(name="items", columns=Items)
    def get_items_with_model():
        yield from [
            {
                "id": 5,
                "name": "dave",
                "amount": 6,
            }
        ]

    @dlt.resource(name="items")
    def get_items_new_col():
        yield from [{"id": 5, "name": "dave", "amount": 6, "new_col": "hello"}]

    # test valid object
    pipeline = get_pipeline()
    # items with model work
    pipeline.run([get_items_with_model()])
    assert pipeline.last_trace.last_normalize_info.row_counts.get("items", 0) == 1

    # loading once with pydantic will freeze the cols
    pipeline = get_pipeline()
    pipeline.run([get_items_with_model()])
    with raises_step_exception(True):
        pipeline.run([get_items_new_col()])

    # it is possible to override contract when there are new columns
    # items with model alone does not work, since contract is set to freeze
    pipeline = get_pipeline()
    pipeline.run([get_items_with_model()])
    pipeline.run([get_items_new_col()], schema_contract="evolve")
    assert pipeline.last_trace.last_normalize_info.row_counts.get("items", 0) == 1


def test_different_objects_in_one_load() -> None:
    pipeline = get_pipeline()

    @dlt.resource(name="items")
    def get_items():
        yield {"id": 1, "name": "dave", "amount": 50}
        yield {"id": 2, "name": "dave", "amount": 50, "new_column": "some val"}

    pipeline.run([get_items()], schema_contract={"columns": "freeze", "tables": "evolve"})
    assert pipeline.last_trace.last_normalize_info.row_counts[ITEMS_TABLE] == 2


@pytest.mark.parametrize("table_mode", ["discard_row", "evolve", "freeze"])
def test_dynamic_tables(table_mode: str) -> None:
    pipeline = get_pipeline()

    # adding columns with a data type makes this columns complete which makes this table complete -> it fails in the normalize because
    #   the tables is NOT new according to normalizer so the row is not discarded
    # remove that and it will pass because the table contains just one incomplete column so it is incomplete so it is treated as new
    # if you uncomment update code in the extract the problem probably goes away
    @dlt.resource(name="items", table_name=lambda i: i["tables"], columns={"id": {}})
    def get_items():
        yield {
            "id": 1,
            "tables": "one",
        }
        yield {"id": 2, "tables": "two", "new_column": "some val"}

    with raises_step_exception(table_mode == "freeze"):
        pipeline.run([get_items()], schema_contract={"tables": table_mode})

    if table_mode != "freeze":
        assert pipeline.last_trace.last_normalize_info.row_counts.get("one", 0) == (
            1 if table_mode == "evolve" else 0
        )
        assert pipeline.last_trace.last_normalize_info.row_counts.get("two", 0) == (
            1 if table_mode == "evolve" else 0
        )


@pytest.mark.parametrize("column_mode", ["discard_row", "evolve", "freeze"])
def test_defined_column_in_new_table(column_mode: str) -> None:
    pipeline = get_pipeline()

    @dlt.resource(name="items", columns=[{"name": "id", "data_type": "bigint", "nullable": False}])
    def get_items():
        yield {
            "id": 1,
            "key": "value",
        }

    pipeline.run([get_items()], schema_contract={"columns": column_mode})
    assert pipeline.last_trace.last_normalize_info.row_counts.get("items", 0) == 1


@pytest.mark.parametrize("column_mode", ["freeze", "discard_row", "evolve"])
def test_new_column_from_hint_and_data(column_mode: str) -> None:
    pipeline = get_pipeline()

    # we define complete column on id, this creates a complete table
    # normalizer does not know that it is a new table and discards the row
    # and it also excepts on column freeze

    @dlt.resource(name="items", columns=[{"name": "id", "data_type": "bigint", "nullable": False}])
    def get_items():
        yield {
            "id": 1,
            "key": "value",
        }

    pipeline.run([get_items()], schema_contract={"columns": column_mode})
    assert pipeline.last_trace.last_normalize_info.row_counts.get("items", 0) == 1


@pytest.mark.parametrize("column_mode", ["freeze", "discard_row", "evolve"])
def test_two_new_columns_from_two_rows(column_mode: str) -> None:
    pipeline = get_pipeline()

    # this creates a complete table in first row
    # and adds a new column to complete tables in 2nd row
    # the test does not fail only because you clone schema in normalize

    @dlt.resource()
    def items():
        yield {
            "id": 1,
        }
        yield {
            "id": 1,
            "key": "value",
        }

    pipeline.run([items()], schema_contract={"columns": column_mode})
    assert pipeline.last_trace.last_normalize_info.row_counts[ITEMS_TABLE] == 2


@pytest.mark.parametrize("column_mode", ["freeze", "discard_row", "evolve"])
def test_dynamic_new_columns(column_mode: str) -> None:
    pipeline = get_pipeline()

    # fails because dlt is not able to add _dlt_load_id to tables. I think we should do an exception for those
    # 1. schema.dlt_tables() - everything evolve
    # 2. is_dlt_column (I hope we have helper) - column evolve, data_type freeze

    def dynamic_columns(item):
        if item["id"] == 1:
            return [{"name": "key", "data_type": "text", "nullable": True}]
        if item["id"] == 2:
            return [{"name": "id", "data_type": "bigint", "nullable": True}]

    @dlt.resource(name="items", table_name=lambda i: "items", schema_contract={"columns": column_mode})  # type: ignore
    def get_items():
        yield {
            "id": 1,
            "key": "value",
        }
        yield {
            "id": 2,
            "key": "value",
        }

    items = get_items()
    items.apply_hints(columns=dynamic_columns)
    # apply hints apply to `items` not the original resource, so doing get_items() below removed them completely
    pipeline.run(items)
    assert pipeline.last_trace.last_normalize_info.row_counts.get("items", 0) == 2


@pytest.mark.parametrize("contract_setting", SCHEMA_CONTRACT)
@pytest.mark.parametrize("as_list", [True, False])
def test_pydantic_contract_implementation(contract_setting: str, as_list: bool) -> None:
    from pydantic import BaseModel

    class Items(BaseModel):
        # for variant test below we must allow allow id to be nullable
        id: Optional[int]  # noqa: A003
        name: str

    def get_items(as_list: bool = False):
        items = [
            {
                "id": 5,
                "name": "dave",
            }
        ]
        if as_list:
            yield items
        else:
            yield from items

    def get_items_extra_attribute(as_list: bool = False):
        items = [{"id": 5, "name": "dave", "blah": "blubb"}]
        if as_list:
            yield items
        else:
            yield from items

    def get_items_extra_variant(as_list: bool = False):
        items = [
            {
                "id": "five",
                "name": "dave",
            }
        ]
        if as_list:
            yield items
        else:
            yield from items

    # test columns complying to model
    pipeline = get_pipeline()
    pipeline.run(
        [get_items(as_list)],
        schema_contract={"columns": contract_setting},
        columns=Items,
        table_name="items",
    )
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 1

    # test columns extra attribute
    with raises_step_exception(
        contract_setting in ["freeze"],
        expected_nested_error=(
            ResourceExtractionError if contract_setting == "freeze" else NotImplementedError
        ),
    ):
        pipeline.run(
            [get_items_extra_attribute(as_list)],
            schema_contract={"columns": contract_setting},
            columns=Items,
            table_name="items",
        )
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 1 if (contract_setting in ["freeze", "discard_row"]) else 2

    # test columns with variant
    with raises_step_exception(
        contract_setting in ["freeze", "discard_value"],
        expected_nested_error=(
            ResourceExtractionError if contract_setting == "freeze" else NotImplementedError
        ),
    ):
        pipeline.run(
            [get_items_extra_variant(as_list)],
            schema_contract={"data_type": contract_setting},
            columns=Items,
            table_name="items",
        )
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts[ITEMS_TABLE] == 1 if (contract_setting in ["freeze", "discard_row"]) else 3


def test_write_to_existing_database_tables_frozen() -> None:
    pipeline = get_pipeline()

    # Create a database schema with table
    with pipeline.sql_client() as c:
        table_name = c.make_qualified_table_name("test_items")
        c.create_dataset()
        c.execute_sql(
            f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL,"
            " _dlt_load_id VARCHAR NOT NULL, _dlt_id VARCHAR NOT NULL)"
        )

    data = [
        {"id": 101, "name": "sub item 101"},
        {"id": 101, "name": "sub item 102"},
    ]

    # we are trying to load to a table existing on the destination but not known to our internal schema
    # this will fail!
    with raises_step_exception(
        expected_nested_error=DataValidationError,
    ):
        pipeline.run(
            data,
            table_name="test_items",
            schema_contract={"tables": "freeze", "columns": "freeze", "data_type": "freeze"},
        )
