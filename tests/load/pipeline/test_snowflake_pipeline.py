import os
import pytest

import dlt

from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DatabaseUndefinedRelation

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
    destination_config: DestinationTestConfiguration,
) -> None:
    # enable query tagging
    os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__QUERY_TAG"] = QUERY_TAG
    snow_ = dlt.destinations.snowflake(naming_convention="sql_cs_v1")

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
