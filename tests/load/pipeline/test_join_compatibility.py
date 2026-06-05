from copy import deepcopy
from pathlib import Path
from typing import Any, Optional

import pytest

import dlt
from dlt.common.destination import TDestinationReferenceArg
from dlt.common.utils import uniq_id
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials
from dlt.destinations.impl.snowflake.configuration import SnowflakeCredentials

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

# Same-host/different-database compatibility needs a pre-existing second database.
SAME_HOST_DIFFERENT_DATABASE_JOIN_COMPATIBILITY_CONFIGS = destinations_configs(
    default_sql_configs=True,
    subset=["snowflake"],
)


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


def _make_same_host_different_database_destinations(
    destination_config: DestinationTestConfiguration,
) -> tuple[Optional[TDestinationReferenceArg], Optional[TDestinationReferenceArg]]:
    if destination_config.destination_type == "snowflake":
        second_database: Optional[str] = dlt.secrets.get(
            "destination.snowflake.join_compatibility_database"
        )
        if not second_database:
            pytest.skip("Second Snowflake database not configured")

        destination_config.setup()
        base_credentials = dlt.secrets.get(
            "destination.snowflake.credentials", SnowflakeCredentials
        )

        first_credentials = deepcopy(base_credentials)
        second_credentials = deepcopy(base_credentials)
        second_credentials.database = second_database

        return (
            dlt.destinations.snowflake(credentials=first_credentials),
            dlt.destinations.snowflake(credentials=second_credentials),
        )

    return None, None


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
    SAME_HOST_DIFFERENT_DATABASE_JOIN_COMPATIBILITY_CONFIGS,
    ids=lambda x: x.name,
)
def test_same_host_different_database_join_compatibility(
    destination_config: DestinationTestConfiguration,
) -> None:
    first_destination, second_destination = _make_same_host_different_database_destinations(
        destination_config
    )
    _run_two_pipeline_check(destination_config, first_destination, second_destination, True)
