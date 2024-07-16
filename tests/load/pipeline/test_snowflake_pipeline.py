import os
import pytest
from pytest_mock import MockerFixture

import dlt

from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from dlt.destinations.impl.snowflake.sql_client import SnowflakeSqlClient
from tests.load.snowflake.test_snowflake_client import QUERY_TAG
from tests.pipeline.utils import assert_load_info
from tests.load.utils import destinations_configs, DestinationTestConfiguration

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_case_sensitive_identifiers(
    destination_config: DestinationTestConfiguration, mocker: MockerFixture
) -> None:
    # enable query tagging
    snow_ = dlt.destinations.snowflake(naming_convention="sql_cs_v1")
    # we make sure that session was not tagged (lack of query tag in config)
    tag_query_spy = mocker.spy(SnowflakeSqlClient, "_tag_session")

    dataset_name = "CaseSensitive_Dataset_" + uniq_id()
    pipeline = destination_config.setup_pipeline(
        "test_snowflake_case_sensitive_identifiers", dataset_name=dataset_name, destination=snow_
    )
    caps = pipeline.destination.capabilities()
    assert caps.naming_convention == "sql_cs_v1"

    destination_client = pipeline.destination_client()
    # assert snowflake caps to be in case sensitive mode
    assert destination_client.capabilities.casefold_identifier is str

    # load some case sensitive data
    info = pipeline.run([{"Id": 1, "Capital": 0.0}], table_name="Expenses")
    assert_load_info(info)
    tag_query_spy.assert_not_called()
    with pipeline.sql_client() as client:
        assert client.has_dataset()
        # use the same case sensitive dataset
        with client.with_alternative_dataset_name(dataset_name):
            assert client.has_dataset()
        # make it case insensitive (upper)
        with client.with_alternative_dataset_name(dataset_name.upper()):
            assert not client.has_dataset()
        # keep case sensitive but make lowercase
        with client.with_alternative_dataset_name(dataset_name.lower()):
            assert not client.has_dataset()

        # must use quoted identifiers
        rows = client.execute_sql('SELECT "Id", "Capital" FROM "Expenses"')
        print(rows)
        with pytest.raises(DatabaseUndefinedRelation):
            client.execute_sql('SELECT "Id", "Capital" FROM Expenses')


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_query_tagging(
    destination_config: DestinationTestConfiguration, mocker: MockerFixture
):
    os.environ["DESTINATION__SNOWFLAKE__QUERY_TAG"] = QUERY_TAG
    tag_query_spy = mocker.spy(SnowflakeSqlClient, "_tag_session")
    pipeline = destination_config.setup_pipeline("test_snowflake_case_sensitive_identifiers")
    info = pipeline.run([1, 2, 3], table_name="digits")
    assert_load_info(info)
    assert tag_query_spy.call_count == 2
