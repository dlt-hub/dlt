import json
from typing import cast

import pytest
from pytest_mock import MockerFixture

import dlt
from dlt.common.utils import uniq_id
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.pipeline.pipeline import Pipeline

from dlt.common import logger
from tests.pipeline.utils import assert_load_info
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)


def _drop_column_in_filesystem(
    pipeline: Pipeline, table_name: str, col: str, table_format: str
) -> None:
    client = cast(FilesystemClient, pipeline._fs_client())
    if table_format == "iceberg":
        tbl = client.load_open_table("iceberg", table_name)
        with tbl.update_schema(allow_incompatible_changes=True) as upd:
            upd.delete_column(col)
    elif table_format == "delta":
        from dlt.common.libs.deltalake import (
            write_deltalake,
            get_table_columns,
            deltalake_storage_options,
        )

        delta_tbl = client.load_open_table("delta", table_name)
        keep_cols = [c for c in get_table_columns(delta_tbl) if c != col]
        arrow_tbl = delta_tbl.to_pyarrow_table(columns=keep_cols)
        storage_options = deltalake_storage_options(client.config)
        write_deltalake(
            table_or_uri=client.get_open_table_location("delta", table_name),
            data=arrow_tbl,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=storage_options,
        )
    else:
        # We don't simulate removal of a column in a plain filesystem destination,
        # because it's unlikely that users do it.
        # Additionally, the dlt schema sync doesn't support it.
        return


def _drop_table_in_filesystem(
    pipeline: Pipeline,
    destination_config: DestinationTestConfiguration,
    table_name: str,
) -> None:
    client = cast(FilesystemClient, pipeline._fs_client())
    client.drop_tables(table_name)


def _drop_column_in_sql(
    pipeline: Pipeline,
    destination_config: DestinationTestConfiguration,
    table_name: str,
    col: str,
) -> None:
    """All non-filesystem destinations end up here."""
    with pipeline.sql_client() as client:
        destination_type, table_format = (
            destination_config.destination_type,
            destination_config.table_format,
        )

        if destination_type == "athena" and table_format != "iceberg":
            # Athena Hive table need REPLACE COLUMNS syntax
            col_defs = [
                f"{client.escape_ddl_identifier('id')} bigint",
                f"{client.escape_ddl_identifier('name')} string",
            ]
            ddl = (
                f"ALTER TABLE {client.make_qualified_ddl_table_name(table_name)} "
                f"REPLACE COLUMNS ({','.join(col_defs)})"
            )
        elif destination_type == "databricks":
            # Enable column-mapping once, then DROP
            client.execute_sql(
                f"ALTER TABLE {client.make_qualified_table_name(table_name)} "
                "SET TBLPROPERTIES("
                "'delta.columnMapping.mode'='name',"
                "'delta.minReaderVersion'='2',"
                "'delta.minWriterVersion'='5')"
            )
            ddl = (
                f"ALTER TABLE {client.make_qualified_table_name(table_name)} "
                f"DROP COLUMN {client.escape_column_name(col)}"
            )
        else:
            qualitified_tbl_name = (
                client.make_qualified_ddl_table_name(table_name)
                if destination_type == "athena" and table_format == "iceberg"
                else client.make_qualified_table_name(table_name)
            )
            qualified_col_name = (
                client.escape_ddl_identifier(col)
                if destination_type == "athena" and table_format == "iceberg"
                else client.escape_column_name(col)
            )
            ddl = f"ALTER TABLE {qualitified_tbl_name} DROP COLUMN {qualified_col_name}"

        client.execute_sql(ddl)


def _drop_table_in_sql(
    pipeline: Pipeline,
    destination_config: DestinationTestConfiguration,
    table_name: str,
) -> None:
    """All non-filesystem destinations end up here."""
    with pipeline.sql_client() as client:
        destination_type = destination_config.destination_type
        qualified = (
            client.make_qualified_ddl_table_name(table_name)
            if destination_type == "athena"
            else client.make_qualified_table_name(table_name)
        )
        if destination_type == "clickhouse":
            query = f"DROP TABLE {qualified} SYNC;"
        else:
            query = f"DROP TABLE {qualified};"

        client.execute_sql(query)


@dlt.resource(table_name="my_table")
def my_resource(with_col: bool = True):
    row = {"id": 1, "name": "Liuwen"}
    if with_col:
        row["age"] = 40
    yield row


@dlt.resource(table_name="my_other_table")
def my_other_resource():
    row = {"id": 1, "name": "Liuwen", "height": 180}
    yield row


@dlt.resource(table_name="my_last_table")
def my_last_resource():
    row = {
        "id": 1,
        "name": "Liuwen",
        "children": [{"id": 2, "name": "Dawei"}, {"id": 3, "name": "Xiaoyun"}],
    }
    yield row


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_filesystem_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_sync_dlt_schema(
    destination_config: DestinationTestConfiguration, mocker: MockerFixture
) -> None:
    pipeline = destination_config.setup_pipeline(pipeline_name=f"pipe_{uniq_id()}")

    assert_load_info(
        pipeline.run(
            [my_resource(), my_other_resource(), my_last_resource()],
            **destination_config.run_kwargs,
        )
    )

    # Simulate a scenario where the user manually drops
    # 1. a column in a table
    # 2. a table with a child table
    if destination_config.destination_type == "filesystem":
        _drop_column_in_filesystem(pipeline, "my_table", "age", destination_config.table_format)
        _drop_table_in_filesystem(pipeline, destination_config, "my_last_table")
    else:
        _drop_column_in_sql(pipeline, destination_config, "my_table", "age")
        _drop_table_in_sql(pipeline, destination_config, "my_last_table")

    # Make sure the warning about orphaned tables is emitted
    logger_spy = mocker.spy(logger, "warning")

    schema_drops = pipeline.sync_schema_from_destination()

    logger_spy.assert_called()
    assert logger_spy.call_count == 1
    expected_warning = (
        "Removing table 'my_last_table' from the dlt schema would leave orphan table(s):"
        " 'my_last_table__children'. Drop these child tables in the destination and sync the dlt"
        " schema again."
    )
    assert expected_warning in logger_spy.call_args_list[0][0][0]

    # Schema drop should only include the "age" column of "my_table"
    assert len(schema_drops) == 1
    assert "my_table" in schema_drops
    assert len(schema_drops["my_table"]["columns"]) == 1
    assert "age" in schema_drops["my_table"]["columns"]

    # ensure schema doesn't have the "age" column in "my_table" anymore
    assert "age" not in pipeline.default_schema.tables["my_table"]["columns"]
    # ensure "my_other_table" was NOT dropped from schema
    assert "my_last_table" in pipeline.default_schema.tables
    # sanity check that the child table is still there
    assert "my_last_table__children" in pipeline.default_schema.tables

    # now the user drops the child table as instructed in the warning
    if destination_config.destination_type == "filesystem":
        _drop_table_in_filesystem(pipeline, destination_config, "my_last_table__children")
    else:
        _drop_table_in_sql(pipeline, destination_config, "my_last_table__children")

    schema_drops = pipeline.sync_schema_from_destination()
    # Schema drop should include the "my_last_table" with the child table
    assert len(schema_drops) == 2
    assert "my_last_table" in schema_drops
    assert "my_last_table__children" in schema_drops

    assert "my_last_table" not in pipeline.default_schema.tables
    assert "my_last_table__children" not in pipeline.default_schema.tables
