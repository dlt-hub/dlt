from typing import cast
import pytest

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["sqlalchemy"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("create_unique_indexes", (True, False))
@pytest.mark.parametrize("create_primary_keys", (True, False))
def test_sqlalchemy_create_indexes(
    destination_config: DestinationTestConfiguration,
    create_unique_indexes: bool,
    create_primary_keys: bool,
) -> None:
    from dlt.destinations import sqlalchemy
    from dlt.common.libs.sql_alchemy import Table, MetaData
    from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyCredentials

    alchemy_ = sqlalchemy(
        create_unique_indexes=create_unique_indexes,
        create_primary_keys=create_primary_keys,
        credentials=cast(SqlalchemyCredentials, destination_config.credentials),
    )

    pipeline = destination_config.setup_pipeline(
        "test_snowflake_case_sensitive_identifiers", dev_mode=True, destination=alchemy_
    )
    # load table with indexes
    pipeline.run([{"id": 1, "value": "X"}], table_name="with_pk", primary_key="id")
    # load without indexes
    pipeline.run([{"id": 1, "value": "X"}], table_name="without_pk")

    dataset_ = pipeline.dataset()
    assert len(dataset_.with_pk.fetchall()) == 1
    assert len(dataset_.without_pk.fetchall()) == 1

    from sqlalchemy import inspect

    with pipeline.sql_client() as client:
        with_pk: Table = client.reflect_table("with_pk", metadata=MetaData())
        assert (with_pk.c.id.primary_key or False) is create_primary_keys
        if client.dialect.name != "sqlite":
            # reflection does not show unique constraints
            # assert (with_pk.c._dlt_id.unique or False) is create_unique_indexes
            inspector = inspect(client.engine)
            indexes = inspector.get_indexes("with_pk", schema=client.dataset_name)
            if create_unique_indexes:
                assert indexes[0]["column_names"][0] == "_dlt_id"
            else:
                assert len(indexes) == 0
