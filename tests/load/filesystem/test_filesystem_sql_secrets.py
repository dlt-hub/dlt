from typing import Any

import pytest
import os
from pytest_mock import MockerFixture

from tests.utils import TEST_STORAGE_ROOT
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    AWS_BUCKET,
)
from dlt.common.utils import uniq_id
from dlt.common import logger


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True, bucket_subset=(AWS_BUCKET,)),
    ids=lambda x: x.name,
)
def test_secrets_management(
    destination_config: DestinationTestConfiguration, mocker: MockerFixture
) -> None:
    """Test the handling of secrets by the sql_client, we only need to do this on s3
    as the other destinations work accordingly"""

    # we can use fake keys
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = "secret_key"
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"] = "key"

    warning_mesage = "You are persisting duckdb secrets but are storing them in the default folder"

    logger_spy = mocker.spy(logger, "warn")

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="read_test",
    )

    import duckdb
    from duckdb import HTTPException
    from dlt.destinations.impl.filesystem.sql_client import (
        FilesystemSqlClient,
        DuckDbCredentials,
    )

    duck_db_location = TEST_STORAGE_ROOT + "/" + uniq_id()
    secrets_dir = f"{TEST_STORAGE_ROOT}/duck_secrets_{uniq_id()}"

    def _external_duckdb_connection() -> duckdb.DuckDBPyConnection:
        external_db = duckdb.connect(duck_db_location)
        external_db.sql(f"SET secret_directory = '{secrets_dir}';")
        external_db.execute("CREATE SCHEMA IF NOT EXISTS first;")
        return external_db

    def _fs_sql_client_for_external_db(
        connection: duckdb.DuckDBPyConnection,
    ) -> FilesystemSqlClient:
        return FilesystemSqlClient(
            dataset_name="second",
            fs_client=pipeline.destination_client(),  #  type: ignore
            credentials=DuckDbCredentials(connection),
        )

    def _secrets_exist() -> bool:
        return os.path.isdir(secrets_dir) and len(os.listdir(secrets_dir)) > 0

    # first test what happens if there are no external secrets
    external_db = _external_duckdb_connection()
    fs_sql_client = _fs_sql_client_for_external_db(external_db)
    with fs_sql_client as sql_client:
        sql_client.create_views_for_tables({"items": "items"})
    external_db.close()
    assert not _secrets_exist()

    # add secrets and check that they are there
    external_db = _external_duckdb_connection()
    fs_sql_client = _fs_sql_client_for_external_db(external_db)
    with fs_sql_client as sql_client:
        fs_sql_client.create_authentication(persistent=True)
    assert _secrets_exist()

    # remove secrets and check that they are removed
    with fs_sql_client as sql_client:
        fs_sql_client.drop_authentication()
    assert not _secrets_exist()
    external_db.close()

    # prevent creating persistent secrets on in mem databases
    fs_sql_client = FilesystemSqlClient(
        dataset_name="second",
        fs_client=pipeline.destination_client(),  #  type: ignore
    )
    with pytest.raises(Exception):
        with fs_sql_client as sql_client:
            fs_sql_client.create_authentication(persistent=True)

    # check that no warning was logged
    logger_spy.assert_not_called()

    # check that warning is logged when secrets are persisted in the default folder
    duck_db_location = TEST_STORAGE_ROOT + "/" + uniq_id()
    secrets_dir = f"{TEST_STORAGE_ROOT}/duck_secrets_{uniq_id()}"
    duck_db = duckdb.connect(duck_db_location)
    fs_sql_client = _fs_sql_client_for_external_db(duck_db)
    with fs_sql_client as sql_client:
        sql_client.create_authentication(persistent=True)
    logger_spy.assert_called_once()
    assert warning_mesage in logger_spy.call_args_list[0][0][0]
    duck_db.close()
