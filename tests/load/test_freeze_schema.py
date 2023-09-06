import dlt, os, pytest
from dlt.common.schema.typing import TSchemaEvolutionSettings
from dlt.common.utils import uniq_id
import duckdb
from typing import Any
from dlt.extract.source import DltSource, DltResource

from tests.load.pipeline.utils import load_table_counts
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.common.schema.exceptions import SchemaFrozenException
from dlt.common.schema import utils

from tests.utils import skip_if_not_active

skip_if_not_active("duckdb")

SCHEMA_EVOLUTION_SETTINGS = ["evolve", "freeze-and-trim", "freeze-and-discard", "freeze-and-raise"]
LOCATIONS = ["source", "resource", "override"]
SCHEMA_ELEMENTS = ["table", "column", "column_variant"]

def items(settings: TSchemaEvolutionSettings) -> Any:

    @dlt.resource(name="items", write_disposition="append", schema_evolution_settings=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {
                "id": index,
                "some_int": 1,
                "name": f"item {index}"
            }

    return load_items

def items_with_variant(settings: TSchemaEvolutionSettings) -> Any:

    @dlt.resource(name="items", write_disposition="append", schema_evolution_settings=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {
                "id": index,
                "name": f"item {index}",
                "some_int": "hello"
            }

    return load_items

def items_with_new_column(settings: TSchemaEvolutionSettings) -> Any:

    @dlt.resource(name="items", write_disposition="append", schema_evolution_settings=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {
                "id": index,
                "name": f"item {index}",
                "new_col": "hello"
            }

    return load_items


def items_with_subtable(settings: TSchemaEvolutionSettings) -> Any:

    @dlt.resource(name="items", write_disposition="append", schema_evolution_settings=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {
                "id": index,
                "name": f"item {index}",
                "sub_items": [{
                    "id": index + 1000,
                    "name": f"sub item {index + 1000}"
                }]
            }

    return load_items

def new_items(settings: TSchemaEvolutionSettings) -> Any:

    @dlt.resource(name="new_items", write_disposition="append", schema_evolution_settings=settings)
    def load_items():
        for _, index in enumerate(range(0, 10), 1):
            yield {
                "id": index,
                "some_int": 1,
                "name": f"item {index}"
            }

    return load_items

OLD_COLUMN_NAME = "name"
NEW_COLUMN_NAME = "new_col"
VARIANT_COLUMN_NAME = "some_int__v_text"
SUBITEMS_TABLE = "items__sub_items"
NEW_ITEMS_TABLE = "new_items"


def run_resource(pipeline, resource_fun, settings) -> DltSource:

    for item in settings.keys():
        assert item in LOCATIONS
        ev_settings = settings[item]
        if ev_settings in SCHEMA_EVOLUTION_SETTINGS:
            continue
        for key, val in ev_settings.items():
            assert val in SCHEMA_EVOLUTION_SETTINGS
            assert key in SCHEMA_ELEMENTS

    @dlt.source(name="freeze_tests", schema_evolution_settings=settings.get("source"))
    def source() -> DltResource:
        return resource_fun(settings.get("resource"))

    # run pipeline
    pipeline.run(source(), schema_evolution_settings=settings.get("override"))

    # check updated schema
    # assert pipeline.default_schema._settings["schema_evolution_settings"] == (settings if settings_location == "source" else None)

    # check items table settings
    # assert pipeline.default_schema.tables["items"]["schema_evolution_settings"] == (settings if settings_location == "resource" else None)

@pytest.mark.parametrize("evolution_setting", SCHEMA_EVOLUTION_SETTINGS)
@pytest.mark.parametrize("setting_location", LOCATIONS)
def test_freeze_new_tables(evolution_setting: str, setting_location: str) -> None:

    full_settings = {
        setting_location: {
        "table": evolution_setting
    }}
    pipeline = dlt.pipeline(pipeline_name=uniq_id(), destination='duckdb', credentials=duckdb.connect(':memory:'), full_refresh=True)
    run_resource(pipeline, items, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 10
    assert OLD_COLUMN_NAME in pipeline.default_schema.tables["items"]["columns"]

    run_resource(pipeline, items_with_new_column, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 20
    assert NEW_COLUMN_NAME in pipeline.default_schema.tables["items"]["columns"]

    run_resource(pipeline, items_with_variant, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 30
    assert VARIANT_COLUMN_NAME in pipeline.default_schema.tables["items"]["columns"]

    # test adding new subtable
    if evolution_setting == "freeze-and-raise":
        with pytest.raises(PipelineStepFailed) as py_ex:
            run_resource(pipeline, items_with_subtable, full_settings)
        assert isinstance(py_ex.value.__context__, SchemaFrozenException)
    else:
        run_resource(pipeline, items_with_subtable, full_settings)

    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 30 if evolution_setting in ["freeze-and-raise"] else 40
    assert table_counts.get(SUBITEMS_TABLE, 0) == (10 if evolution_setting in ["evolve"] else 0)

    # test adding new table
    if evolution_setting == "freeze-and-raise":
        with pytest.raises(PipelineStepFailed) as py_ex:
            run_resource(pipeline, new_items, full_settings)
        assert isinstance(py_ex.value.__context__, SchemaFrozenException)
    else:
        run_resource(pipeline, new_items, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts.get("new_items", 0) == (10 if evolution_setting in ["evolve"] else 0)


@pytest.mark.parametrize("evolution_setting", SCHEMA_EVOLUTION_SETTINGS)
@pytest.mark.parametrize("setting_location", LOCATIONS)
def test_freeze_new_columns(evolution_setting: str, setting_location: str) -> None:

    full_settings = {
        setting_location: {
        "column": evolution_setting
    }}

    pipeline = dlt.pipeline(pipeline_name=uniq_id(), destination='duckdb', credentials=duckdb.connect(':memory:'))
    run_resource(pipeline, items, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 10
    assert OLD_COLUMN_NAME in pipeline.default_schema.tables["items"]["columns"]

    # subtable should work
    run_resource(pipeline, items_with_subtable, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 20
    assert table_counts[SUBITEMS_TABLE] == 10

    # new should work
    run_resource(pipeline, new_items, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 20
    assert table_counts[NEW_ITEMS_TABLE] == 10

    # test adding new column
    if evolution_setting == "freeze-and-raise":
        with pytest.raises(PipelineStepFailed) as py_ex:
            run_resource(pipeline, items_with_new_column, full_settings)
        assert isinstance(py_ex.value.__context__, SchemaFrozenException)
    else:
        run_resource(pipeline, items_with_new_column, full_settings)

    if evolution_setting == "evolve":
        assert NEW_COLUMN_NAME in pipeline.default_schema.tables["items"]["columns"]
    else:
        assert NEW_COLUMN_NAME not in pipeline.default_schema.tables["items"]["columns"]
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == (30 if evolution_setting in ["evolve", "freeze-and-trim"] else 20)

    # test adding variant column
    if evolution_setting == "freeze-and-raise":
        with pytest.raises(PipelineStepFailed) as py_ex:
            run_resource(pipeline, items_with_variant, full_settings)
        assert isinstance(py_ex.value.__context__, SchemaFrozenException)
    else:
        run_resource(pipeline, items_with_variant, full_settings)

    if evolution_setting == "evolve":
        assert VARIANT_COLUMN_NAME in pipeline.default_schema.tables["items"]["columns"]
    else:
        assert VARIANT_COLUMN_NAME not in pipeline.default_schema.tables["items"]["columns"]
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == (40 if evolution_setting in ["evolve", "freeze-and-trim"] else 20)


@pytest.mark.parametrize("evolution_setting", SCHEMA_EVOLUTION_SETTINGS)
@pytest.mark.parametrize("setting_location", LOCATIONS)
def test_freeze_variants(evolution_setting: str, setting_location: str) -> None:

    full_settings = {
        setting_location: {
        "column_variant": evolution_setting
    }}
    pipeline = dlt.pipeline(pipeline_name=uniq_id(), destination='duckdb', credentials=duckdb.connect(':memory:'))
    run_resource(pipeline, items, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 10
    assert OLD_COLUMN_NAME in pipeline.default_schema.tables["items"]["columns"]

    # subtable should work
    run_resource(pipeline, items_with_subtable, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 20
    assert table_counts[SUBITEMS_TABLE] == 10

    # new should work
    run_resource(pipeline, new_items, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 20
    assert table_counts[NEW_ITEMS_TABLE] == 10

    # test adding new column
    run_resource(pipeline, items_with_new_column, full_settings)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 30
    assert NEW_COLUMN_NAME in pipeline.default_schema.tables["items"]["columns"]

    # test adding variant column
    if evolution_setting == "freeze-and-raise":
        with pytest.raises(PipelineStepFailed) as py_ex:
            run_resource(pipeline, items_with_variant, full_settings)
        assert isinstance(py_ex.value.__context__, SchemaFrozenException)
    else:
        run_resource(pipeline, items_with_variant, full_settings)

    if evolution_setting == "evolve":
        assert VARIANT_COLUMN_NAME in pipeline.default_schema.tables["items"]["columns"]
    else:
        assert VARIANT_COLUMN_NAME not in pipeline.default_schema.tables["items"]["columns"]
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == (40 if evolution_setting in ["evolve", "freeze-and-trim"] else 30)


def test_settings_precedence() -> None:
    pipeline = dlt.pipeline(pipeline_name=uniq_id(), destination='duckdb', credentials=duckdb.connect(':memory:'))

    # load some data
    run_resource(pipeline, items, {})

    # trying to add new column when forbidden on resource will fail
    run_resource(pipeline, items_with_new_column, {"resource": {
        "column": "freeze-and-discard"
    }})

    # when allowed on override it will work
    run_resource(pipeline, items_with_new_column, {
        "resource": {"column": "freeze-and-raise"},
        "override": {"column": "evolve"}
    })


def test_settings_precedence_2() -> None:
    pipeline = dlt.pipeline(pipeline_name=uniq_id(), destination='duckdb', credentials=duckdb.connect(':memory:'))

    # load some data
    run_resource(pipeline, items, {"source": {
        "column_variant": "freeze-and-discard"
    }})
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 10

    # trying to add variant when forbidden on source will fail
    run_resource(pipeline, items_with_variant, {"source": {
        "column_variant": "freeze-and-discard"
    }})
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 10

    # if allowed on resource it will pass
    run_resource(pipeline, items_with_variant, {
        "resource": {"column_variant": "evolve"},
        "source": {"column_variant": "freeze-and-discard"}
    })
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 20

    # if allowed on override it will also pass
    run_resource(pipeline, items_with_variant, {
        "resource": {"column_variant": "freeze-and-discard"},
        "source": {"column_variant": "freeze-and-discard"},
        "override": {"column_variant": "evolve"},
    })
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 30

@pytest.mark.parametrize("setting_location", LOCATIONS)
def test_change_mode(setting_location: str) -> None:
    pipeline = dlt.pipeline(pipeline_name=uniq_id(), destination='duckdb', credentials=duckdb.connect(':memory:'))

    # load some data
    run_resource(pipeline, items, {})
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 10

    # trying to add variant when forbidden on source will fail
    run_resource(pipeline, items_with_variant, {setting_location: {
        "column_variant": "freeze-and-discard"
    }})
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 10

    # now allow
    run_resource(pipeline, items_with_variant, {setting_location: {
        "column_variant": "evolve"
    }})
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 20


