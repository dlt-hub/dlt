from copy import deepcopy
from pathlib import Path
from typing import Any, Optional

import pytest

import dlt
from dlt.common.destination import TDestinationReferenceArg
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DatabaseException, DatabaseUndefinedRelation
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

from tests.load.utils import (
    DestinationTestConfiguration,
    destinations_configs,
)
from tests.pipeline.utils import assert_load_info


pytestmark = pytest.mark.essential


SAME_DATABASE_JOIN_COMPATIBILITY_CONFIGS = destinations_configs(
    default_sql_configs=True,
    local_filesystem_configs=True,
    subset=[
        "clickhouse",
        "dremio",
        "duckdb",
        "ducklake",
        "filesystem",
        "postgres",
        "snowflake",
        "sqlalchemy",
    ],
)

FILESYSTEM_DIFFERENT_LOCATION_JOIN_COMPATIBILITY_CONFIGS = destinations_configs(
    local_filesystem_configs=True,
    subset=["filesystem"],
)

CROSS_DATABASE_PROBE_CONFIGS = destinations_configs(
    default_sql_configs=True,
    subset=["clickhouse", "fabric", "snowflake", "synapse"],
)

# query referencing a non-existent database via each engine's cross-database name form
CROSS_DATABASE_PROBE_QUERIES = {
    "clickhouse": "SELECT 1 FROM dlt_nonexistent_db_xyz.some_table_abc LIMIT 1",
    "snowflake": "SELECT 1 FROM dlt_nonexistent_db_xyz.public.some_table_abc LIMIT 1",
    "synapse": "SELECT TOP 1 1 FROM [dlt_nonexistent_db_xyz].[dbo].[some_table_abc]",
    "fabric": "SELECT TOP 1 1 FROM [dlt_nonexistent_db_xyz].[dbo].[some_table_abc]",
}


def _load_table(
    pipeline: dlt.Pipeline,
    destination_config: DestinationTestConfiguration,
    table_name: str,
    rows: list[dict[str, Any]],
) -> None:
    info = pipeline.run(rows, table_name=table_name, **destination_config.run_kwargs)
    assert_load_info(info)


def _make_same_database_destinations(
    destination_config: DestinationTestConfiguration,
    tmp_path: Path,
    test_id: str,
) -> tuple[Optional[TDestinationReferenceArg], Optional[TDestinationReferenceArg]]:
    if destination_config.destination_type == "duckdb":
        database_path = tmp_path / f"join_compat_{test_id}.duckdb"
        return dlt.destinations.duckdb(str(database_path)), dlt.destinations.duckdb(
            str(database_path)
        )

    if destination_config.destination_type == "ducklake":
        credentials = DuckLakeCredentials(
            ducklake_name=f"join_compat_{test_id}",
            catalog=f"sqlite:///{tmp_path / f'join_compat_{test_id}.sqlite'}",
            storage=str(tmp_path / f"join_compat_{test_id}.files"),
        )
        return (
            dlt.destinations.ducklake(credentials=deepcopy(credentials)),
            dlt.destinations.ducklake(credentials=deepcopy(credentials)),
        )

    if destination_config.destination_name == "sqlalchemy_sqlite":
        connection_string = f"sqlite:///{tmp_path / f'join_compat_{test_id}.sqlite'}"
        return (
            dlt.destinations.sqlalchemy(credentials=connection_string),
            dlt.destinations.sqlalchemy(credentials=connection_string),
        )

    return None, None


def _engine_supports_cross_database(pipeline: dlt.Pipeline, probe_query: str) -> bool:
    """Returns True if the engine accepts a cross-database reference.

    The probe targets a non-existent database. An `undefined relation` error means the engine
    resolved the cross-database name and only failed because the object is missing (cross-database
    queries are supported). Any other database error means the engine rejected the cross-database
    name itself (e.g. Azure SQL Database / Synapse error 40515).
    """
    sql_client = pipeline.sql_client()
    try:
        with sql_client:
            sql_client.execute_sql(probe_query)
        return True
    except DatabaseUndefinedRelation:
        return True
    except DatabaseException:
        return False


def _make_filesystem_different_location_destinations(
    tmp_path: Path,
    test_id: str,
) -> tuple[TDestinationReferenceArg, TDestinationReferenceArg]:
    return (
        dlt.destinations.filesystem(str(tmp_path / f"join_compat_first_{test_id}")),
        dlt.destinations.filesystem(str(tmp_path / f"join_compat_second_{test_id}")),
    )


def _run_two_pipeline_check(
    destination_config: DestinationTestConfiguration,
    first_destination: Optional[TDestinationReferenceArg],
    second_destination: Optional[TDestinationReferenceArg],
    expected: bool,
    expected_write: Optional[bool] = None,
) -> None:
    # by default SQL write capability follows read capability
    if expected_write is None:
        expected_write = expected
    test_id = uniq_id()
    first_pipeline = destination_config.setup_pipeline(
        "join_first_" + test_id,
        dataset_name="join_compat_first_" + test_id,
        destination=first_destination,
    )
    second_pipeline = destination_config.setup_pipeline(
        "join_second_" + test_id,
        dataset_name="join_compat_second_" + test_id,
        destination=second_destination,
    )

    _load_table(
        first_pipeline,
        destination_config,
        "join_items",
        [{"id": 1, "name": "first"}],
    )
    _load_table(
        second_pipeline,
        destination_config,
        "join_items",
        [{"id": 1, "name": "second"}],
    )

    first_config = first_pipeline.dataset().destination_client.config
    second_config = second_pipeline.dataset().destination_client.config
    assert first_config.can_read_from(second_config) is expected
    assert second_config.can_read_from(first_config) is expected
    assert first_config.can_write_from(second_config) is expected_write
    assert second_config.can_write_from(first_config) is expected_write


@pytest.mark.parametrize(
    "destination_config",
    SAME_DATABASE_JOIN_COMPATIBILITY_CONFIGS,
    ids=lambda x: x.name,
)
def test_same_database_join_compatibility(
    destination_config: DestinationTestConfiguration,
    tmp_path: Path,
) -> None:
    test_id = uniq_id()
    first_destination, second_destination = _make_same_database_destinations(
        destination_config, tmp_path, test_id
    )
    # filesystem at the same location is readable but dlt is the only writing engine
    expected_write = False if destination_config.destination_type == "filesystem" else None
    _run_two_pipeline_check(
        destination_config, first_destination, second_destination, True, expected_write
    )


@pytest.mark.parametrize(
    "destination_config",
    FILESYSTEM_DIFFERENT_LOCATION_JOIN_COMPATIBILITY_CONFIGS,
    ids=lambda x: x.name,
)
def test_filesystem_different_location_not_compatible(
    destination_config: DestinationTestConfiguration,
    tmp_path: Path,
) -> None:
    # reading across filesystem locations requires auto ATTACH in the duckdb view layer
    first_destination, second_destination = _make_filesystem_different_location_destinations(
        tmp_path, uniq_id()
    )
    _run_two_pipeline_check(destination_config, first_destination, second_destination, False)


@pytest.mark.parametrize(
    "destination_config",
    CROSS_DATABASE_PROBE_CONFIGS,
    ids=lambda x: x.name,
)
def test_cross_database_join_capability(
    destination_config: DestinationTestConfiguration,
) -> None:
    # load a table so the connection and dataset are live
    test_id = uniq_id()
    pipeline = destination_config.setup_pipeline(
        "join_xdb_" + test_id,
        dataset_name="join_compat_xdb_" + test_id,
        dev_mode=True,
    )
    _load_table(pipeline, destination_config, "join_items", [{"id": 1, "name": "first"}])

    config = pipeline.dataset().destination_client.config
    # a sibling config that differs only by (non-existent) database name on the same host
    sibling = deepcopy(config)
    sibling.credentials.database = "dlt_nonexistent_db_xyz"  # type: ignore[attr-defined]
    claims_cross_database = config.can_read_from(sibling)

    probe_query = CROSS_DATABASE_PROBE_QUERIES[destination_config.destination_type]
    engine_supports_cross_database = _engine_supports_cross_database(pipeline, probe_query)

    # the config's joinability claim across databases must match what the engine actually allows
    assert claims_cross_database == engine_supports_cross_database, (
        f"{destination_config.destination_type}: can_read_from across databases ="
        f" {claims_cross_database}, but engine cross-database support ="
        f" {engine_supports_cross_database}"
    )
