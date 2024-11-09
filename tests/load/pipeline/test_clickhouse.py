from typing import Iterator, Any, Generator

import pytest

import dlt
from dlt.common.typing import TDataItem
from dlt.common.utils import uniq_id
from dlt.common.libs.pyarrow import row_tuples_to_arrow
from dlt.common.destination import DestinationCapabilitiesContext
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import (
    load_table_counts,
    load_tables_to_dicts,
    assert_load_info,
    assert_records_as_set,
)


@pytest.fixture
def pipeline(destination_config: DestinationTestConfiguration) -> Generator[Any, None, None]:
    pipeline = destination_config.setup_pipeline(f"clickhouse_{uniq_id()}", dev_mode=True)
    yield pipeline
    with pipeline.sql_client() as client:
        client.drop_dataset()


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
def test_clickhouse_destination_append(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(f"clickhouse_{uniq_id()}", dev_mode=True)

    try:

        @dlt.resource(name="items", write_disposition="append")
        def items() -> Iterator[TDataItem]:
            yield {
                "id": 1,
                "name": "item",
                "sub_items": [
                    {"id": 101, "name": "sub item 101"},
                    {"id": 101, "name": "sub item 102"},
                ],
            }

        pipeline.run(
            items,
            **destination_config.run_kwargs,
            staging=destination_config.staging,
        )

        table_counts = load_table_counts(
            pipeline,
            *[t["name"] for t in pipeline.default_schema._schema_tables.values()],
        )
        assert table_counts["items"] == 1
        assert table_counts["items__sub_items"] == 2
        assert table_counts["_dlt_loads"] == 1

        # Load again with schema evolution.
        @dlt.resource(name="items", write_disposition="append")
        def items2() -> Iterator[TDataItem]:
            yield {
                "id": 1,
                "name": "item",
                "new_field": "hello",
                "sub_items": [
                    {
                        "id": 101,
                        "name": "sub item 101",
                        "other_new_field": "hello 101",
                    },
                    {
                        "id": 101,
                        "name": "sub item 102",
                        "other_new_field": "hello 102",
                    },
                ],
            }

        pipeline.run(items2, **destination_config.run_kwargs)
        table_counts = load_table_counts(
            pipeline,
            *[t["name"] for t in pipeline.default_schema._schema_tables.values()],
        )
        assert table_counts["items"] == 2
        assert table_counts["items__sub_items"] == 4
        assert table_counts["_dlt_loads"] == 2

    except Exception as e:
        raise e

    finally:
        with pipeline.sql_client() as client:
            client.drop_dataset()


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
def test_clickhouse_destination_merge_dicts(
    pipeline: Any,
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(
        write_disposition="merge",
        primary_key="id",
    )
    def items(test_records):
        yield test_records

    initial_test_records = [
        {"id": 1, "name": "item 1"},
        {"id": 2, "name": "item 2"},
        {"id": 3, "name": "item 3"},
    ]

    load_info = pipeline.run(
        items(initial_test_records),
    )
    assert_load_info(load_info)
    table_counts = load_table_counts(pipeline, "items")
    assert table_counts["items"] == 3
    tables = load_tables_to_dicts(pipeline, "items", exclude_system_cols=True)

    assert_records_as_set(
        tables["items"],
        [
            {"id": 1, "name": "item 1"},
            {"id": 2, "name": "item 2"},
            {"id": 3, "name": "item 3"},
        ],
    )

    # Update the records
    updated_test_records = [
        {"id": 1, "name": "updated item 1"},
        {"id": 2, "name": "updated item 2"},
    ]

    load_info = pipeline.run(
        items(updated_test_records),
    )

    assert_load_info(load_info)
    table_counts = load_table_counts(pipeline, "items")
    assert table_counts["items"] == 3

    tables = load_tables_to_dicts(pipeline, "items", exclude_system_cols=True)

    assert_records_as_set(
        tables["items"],
        [
            {"id": 1, "name": "updated item 1"},
            {"id": 2, "name": "updated item 2"},
            {"id": 3, "name": "item 3"},
        ],
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
def test_clickhouse_destination_merge_arrow(
    pipeline: Any,
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(
        write_disposition="merge",
        primary_key="id",
    )
    def arrow_items(rows, schema_columns, timezone="UTC"):
        yield row_tuples_to_arrow(
            rows,
            DestinationCapabilitiesContext.generic_capabilities(),
            columns=schema_columns,
            tz=timezone,
        )

    schema_columns = {
        "id": {"name": "id", "nullable": False, "data_type": "bigint"},
        "name": {"name": "name", "nullable": True, "data_type": "text"},
    }
    test_rows = [(1, "foo"), (2, "bar")]

    load_info = pipeline.run(
        arrow_items(test_rows, schema_columns),
    )
    assert_load_info(load_info)
    table_counts = load_table_counts(pipeline, "arrow_items")
    assert table_counts["arrow_items"] == 2

    tables = load_tables_to_dicts(pipeline, "arrow_items")

    assert_records_as_set(
        tables["arrow_items"],
        [
            {"id": 1, "name": "foo"},
            {"id": 2, "name": "bar"},
        ],
    )

    # Update the records
    test_rows = [(1, "foo"), (2, "updated bar")]

    load_info = pipeline.run(
        arrow_items(test_rows, schema_columns),
    )

    assert_load_info(load_info)

    table_counts = load_table_counts(pipeline, "arrow_items")
    assert table_counts["arrow_items"] == 2

    tables = load_tables_to_dicts(pipeline, "arrow_items")

    assert_records_as_set(
        tables["arrow_items"],
        [
            {"id": 1, "name": "foo"},
            {"id": 2, "name": "updated bar"},
        ],
    )
