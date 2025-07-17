import json
from typing import cast
from collections.abc import Mapping

import pytest
import dlt
from dlt.common import pendulum
from dlt.common.utils import uniq_id
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.load import Load
from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.state_sync import load_pipeline_state_from_destination, state_resource

from dlt.common.libs.deltalake import write_deltalake, get_table_columns, deltalake_storage_options
from tests.utils import TEST_STORAGE_ROOT
from tests.cases import JSON_TYPED_DICT, JSON_TYPED_DICT_DECODED
from tests.common.utils import IMPORTED_VERSION_HASH_ETH_V10, yml_case_path as common_yml_case_path
from tests.common.configuration.utils import environment
from tests.pipeline.utils import assert_query_column, assert_load_info
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)


def _drop_column_in_filesystem(
    pipeline: Pipeline, table_name: str, col: str, table_format: str
) -> None:
    """Iceberg / Delta live under the Filesystem destination."""
    client = cast(FilesystemClient, pipeline._fs_client())
    if table_format == "iceberg":
        tbl = client.load_open_table("iceberg", table_name)
        with tbl.update_schema(allow_incompatible_changes=True) as upd:
            upd.delete_column(col)
    elif table_format == "delta":
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
        file_path = client.list_table_files(table_name)[0]
        new_lines = []
        with client.fs_client.open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    data = json.loads(line)
                    if isinstance(data, dict) and col in data:
                        del data[col]
                    new_lines.append(json.dumps(data))

        # Write back the modified content
        with client.fs_client.open(file_path, "w", encoding="utf-8") as f:
            for line in new_lines:
                f.write(line + "\n")


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
            qualified = (
                client.make_qualified_ddl_table_name(table_name)
                if destination_type == "athena" and table_format == "iceberg"
                else client.make_qualified_table_name(table_name)
            )
            ddl = f"ALTER TABLE {qualified} DROP COLUMN {client.escape_column_name(col)}"

        client.execute_sql(ddl)


@dlt.resource(table_name="my_table")
def my_resource(with_col: bool = True):
    row = {"id": 1, "name": "Liuwen"}
    if with_col:
        row["age"] = 40
    yield row


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_staging_configs=True,
        default_sql_configs=True,
        table_format_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_sync_schema_destructively(destination_config: DestinationTestConfiguration) -> None:
    table, col_to_drop = "my_table", "age"

    pipeline = destination_config.setup_pipeline(pipeline_name=f"pipe_{uniq_id()}")

    assert_load_info(pipeline.run(my_resource(), **destination_config.run_kwargs))

    # simulate "user" manually dropped the column”
    if destination_config.destination_type == "filesystem":
        _drop_column_in_filesystem(pipeline, table, col_to_drop, destination_config.table_format)
    else:
        _drop_column_in_sql(pipeline, destination_config, table, col_to_drop)

    # sync schema with destination make sure next run succeeds
    pipeline.sync_schema_destructively()
    assert_load_info(pipeline.run(my_resource(with_col=False), **destination_config.run_kwargs))

    # ensure schema doesn't have the dropped column
    assert col_to_drop not in pipeline.default_schema.tables[table]["columns"]
