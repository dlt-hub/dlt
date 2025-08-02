from typing import Type, cast
import pytest

from dlt.common.typing import TypeAlias

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


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["sqlalchemy"]),
    ids=lambda x: x.name,
)
def test_custom_type_mapper(destination_config: DestinationTestConfiguration) -> None:
    from dlt.common import json

    import dlt
    import sqlalchemy as sa
    from dlt.destinations.type_mapping import DataTypeMapper

    class JSONString(sa.TypeDecorator):
        """
        A custom SQLAlchemy type that stores JSON data as a string in the database.
        Automatically serializes Python objects to JSON strings on write and
        deserializes JSON strings back to Python objects on read.
        """

        impl = sa.String
        cache_ok = True

        def process_bind_param(self, value, dialect):
            if value is None:
                return None

            return json.dumps(value)

        def process_result_value(self, value, dialect):
            if value is None:
                return None

            return json.loads(value)

    pipeline = destination_config.setup_pipeline("test_custom_type_mapper")
    with pipeline._maybe_destination_capabilities() as caps:
        type_mapper_: TypeAlias[Type[DataTypeMapper]] = caps.type_mapper  # type: ignore

    class TrinoTypeMapper(type_mapper_):
        """Example mapper that plugs custom string type that serialized to from/json

        Note that instance of TypeMapper contains dialect and destination capabilities instance
        for a deeper integration
        """

        def to_destination_type(self, column, table=None):
            if column["data_type"] == "json":
                return JSONString(length=345)
            return super().to_destination_type(column, table)

    # pass dest_ in `destination` argument to dlt.pipeline
    dest_ = dlt.destinations.sqlalchemy(type_mapper=TrinoTypeMapper)

    pipeline = destination_config.setup_pipeline(
        "test_custom_type_mapper", destination=dest_, dev_mode=True
    )

    # run pipeline with resource that has json data type hint
    @dlt.resource(columns={"json_field": {"data_type": "json"}})
    def json_data():
        yield {
            "id": 1,
            "json_field": {"key": "value", "nested": {"num": 42}},
            "regular_field": "some text",
        }

    pipeline.run(json_data(), table_name="test_json_mapping")

    # read data with pipeline.dataset()
    dataset = pipeline.dataset()
    result = dataset.test_json_mapping.select("id", "json_field", "regular_field").fetchall()
    assert len(result) == 1
    assert result[0][0] == 1
    assert result[0][2] == "some text"

    # The json field should be stored as a string representation of the JSON
    json_value = result[0][1]
    if isinstance(json_value, str):
        parsed_json = json.loads(json_value)
        assert parsed_json["key"] == "value"
        assert parsed_json["nested"]["num"] == 42
    else:
        raise NotImplementedError("must be string")

    # reflect table and check if string field length is exactly 345
    from dlt.common.libs.sql_alchemy import Table, MetaData

    with pipeline.sql_client() as client:
        reflected_table: Table = client.reflect_table("test_json_mapping", metadata=MetaData())
        json_column = reflected_table.c.json_field

        # Check that the json field was mapped to String with length 345
        assert isinstance(json_column.type, sa.String)
        assert json_column.type.length == 345
