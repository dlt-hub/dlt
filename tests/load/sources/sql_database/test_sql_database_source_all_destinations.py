import os
from typing import Any, List

import humanize
import pytest

import dlt
from dlt.sources import DltResource
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.common.exceptions import MissingDependencyException

from tests.load.utils import (
    DestinationTestConfiguration,
    destinations_configs,
)
from tests.pipeline.utils import (
    assert_load_info,
    load_table_counts,
)

try:
    from dlt.sources.sql_database import TableBackend, sql_database, sql_table
    from tests.load.sources.sql_database.test_helpers import mock_json_column
    from tests.load.sources.sql_database.test_sql_database_source import (
        assert_row_counts,
        convert_time_to_us,
        default_test_callback,
    )
    from tests.load.sources.sql_database.sql_source import SQLAlchemySourceDB
    from dlt.common.libs.sql_alchemy import IS_SQL_ALCHEMY_20
except MissingDependencyException:
    pytest.skip("Tests require sql alchemy", allow_module_level=True)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_sql_schema_loads_all_tables(
    sql_source_db: SQLAlchemySourceDB,
    destination_config: DestinationTestConfiguration,
    backend: TableBackend,
    request: Any,
) -> None:
    pipeline = destination_config.setup_pipeline(request.node.name, dev_mode=True)

    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        backend=backend,
        reflection_level="minimal",
        type_adapter_callback=default_test_callback(destination_config.destination_type, backend),
    )

    if destination_config.destination_type == "bigquery" and backend == "connectorx":
        # connectorx generates nanoseconds time which bigquery cannot load
        source.has_precision.add_map(convert_time_to_us)
        source.has_precision_nullable.add_map(convert_time_to_us)

    if backend != "sqlalchemy":
        # always use mock json
        source.has_precision.add_map(mock_json_column("json_col"))
        source.has_precision_nullable.add_map(mock_json_column("json_col"))

    assert "chat_message_view" not in source.resources  # Views are not reflected by default

    load_info = pipeline.run(source)
    print(humanize.precisedelta(pipeline.last_trace.finished_at - pipeline.last_trace.started_at))
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_sql_schema_loads_all_tables_parallel(
    sql_source_db: SQLAlchemySourceDB,
    destination_config: DestinationTestConfiguration,
    backend: TableBackend,
    request: Any,
) -> None:
    pipeline = destination_config.setup_pipeline(request.node.name, dev_mode=True)
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        backend=backend,
        reflection_level="minimal",
        type_adapter_callback=default_test_callback(destination_config.destination_type, backend),
    ).parallelize()

    if destination_config.destination_type == "bigquery" and backend == "connectorx":
        # connectorx generates nanoseconds time which bigquery cannot load
        source.has_precision.add_map(convert_time_to_us)
        source.has_precision_nullable.add_map(convert_time_to_us)

    if backend != "sqlalchemy":
        # always use mock json
        source.has_precision.add_map(mock_json_column("json_col"))
        source.has_precision_nullable.add_map(mock_json_column("json_col"))

    load_info = pipeline.run(source)
    print(humanize.precisedelta(pipeline.last_trace.finished_at - pipeline.last_trace.started_at))
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_sql_table_names(
    sql_source_db: SQLAlchemySourceDB,
    destination_config: DestinationTestConfiguration,
    backend: TableBackend,
    request: Any,
) -> None:
    pipeline = destination_config.setup_pipeline(request.node.name, dev_mode=True)
    tables = ["chat_channel", "chat_message"]
    load_info = pipeline.run(
        sql_database(
            credentials=sql_source_db.credentials,
            schema=sql_source_db.schema,
            table_names=tables,
            reflection_level="minimal",
            backend=backend,
        )
    )
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, tables)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_sql_table_incremental(
    sql_source_db: SQLAlchemySourceDB,
    destination_config: DestinationTestConfiguration,
    backend: TableBackend,
    request: Any,
) -> None:
    """Run pipeline twice. Insert more rows after first run
    and ensure only those rows are stored after the second run.
    """
    os.environ["SOURCES__SQL_DATABASE__CHAT_MESSAGE__INCREMENTAL__CURSOR_PATH"] = "updated_at"

    if not IS_SQL_ALCHEMY_20 and backend == "connectorx":
        pytest.skip("Test will not run on sqlalchemy 1.4 with connectorx")

    pipeline = destination_config.setup_pipeline(request.node.name, dev_mode=True)
    tables = ["chat_message"]

    def make_source():
        return sql_database(
            credentials=sql_source_db.credentials,
            schema=sql_source_db.schema,
            table_names=tables,
            reflection_level="minimal",
            backend=backend,
        )

    load_info = pipeline.run(make_source())
    assert_load_info(load_info)
    sql_source_db.fake_messages(n=100)
    load_info = pipeline.run(make_source())
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, tables)


@pytest.mark.skip(reason="Skipping this test temporarily")
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_mysql_data_load(
    destination_config: DestinationTestConfiguration, backend: TableBackend, request: Any
) -> None:
    # reflect a database
    credentials = ConnectionStringCredentials(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    )
    database = sql_database(credentials)
    assert "family" in database.resources

    if backend == "connectorx":
        # connector-x has different connection string format
        backend_kwargs = {"conn": "mysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"}
    else:
        backend_kwargs = {}

    # no longer needed: asdecimal used to infer decimal or not
    # def _double_as_decimal_adapter(table: sa.Table) -> sa.Table:
    #     for column in table.columns.values():
    #         if isinstance(column.type, sa.Double):
    #             column.type.asdecimal = False

    # load a single table
    family_table = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="family",
        backend=backend,
        reflection_level="minimal",
        backend_kwargs=backend_kwargs,
        # table_adapter_callback=_double_as_decimal_adapter,
    )

    pipeline = destination_config.setup_pipeline(request.node.name, dev_mode=True)
    load_info = pipeline.run(family_table, write_disposition="merge")
    assert_load_info(load_info)
    counts_1 = load_table_counts(pipeline, "family")

    # load again also with merge
    family_table = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="family",
        backend=backend,
        reflection_level="minimal",
        # we also try to remove dialect automatically
        backend_kwargs={},
        # table_adapter_callback=_double_as_decimal_adapter,
    )
    load_info = pipeline.run(family_table, write_disposition="merge")
    assert_load_info(load_info)
    counts_2 = load_table_counts(pipeline, "family")
    # no duplicates
    assert counts_1 == counts_2


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
def test_load_sql_table_resource_loads_data(
    sql_source_db: SQLAlchemySourceDB,
    destination_config: DestinationTestConfiguration,
    backend: TableBackend,
    request: Any,
) -> None:
    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
                reflection_level="minimal",
                backend=backend,
            )
        ]

    pipeline = destination_config.setup_pipeline(request.node.name, dev_mode=True)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, ["chat_message"])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_load_sql_table_resource_incremental(
    sql_source_db: SQLAlchemySourceDB,
    destination_config: DestinationTestConfiguration,
    backend: TableBackend,
    request: Any,
) -> None:
    if not IS_SQL_ALCHEMY_20 and backend == "connectorx":
        pytest.skip("Test will not run on sqlalchemy 1.4 with connectorx")

    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
                incremental=dlt.sources.incremental("updated_at"),
                reflection_level="minimal",
                backend=backend,
            )
        ]

    pipeline = destination_config.setup_pipeline(request.node.name, dev_mode=True)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)
    sql_source_db.fake_messages(n=100)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)

    assert_row_counts(pipeline, sql_source_db, ["chat_message"])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_load_sql_table_resource_incremental_initial_value(
    sql_source_db: SQLAlchemySourceDB,
    destination_config: DestinationTestConfiguration,
    backend: TableBackend,
    request: Any,
) -> None:
    if not IS_SQL_ALCHEMY_20 and backend == "connectorx":
        pytest.skip("Test will not run on sqlalchemy 1.4 with connectorx")

    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
                incremental=dlt.sources.incremental(
                    "updated_at",
                    sql_source_db.table_infos["chat_message"]["created_at"].start_value,
                ),
                reflection_level="minimal",
                backend=backend,
            )
        ]

    pipeline = destination_config.setup_pipeline(request.node.name, dev_mode=True)
    load_info = pipeline.run(sql_table_source())
    assert_load_info(load_info)
    assert_row_counts(pipeline, sql_source_db, ["chat_message"])
