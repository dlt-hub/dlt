import os
from copy import deepcopy
from typing import Iterator, Dict

import pytest
import sqlfluff

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import GcpServiceAccountCredentialsWithoutDefaults
from dlt.common.pendulum import pendulum
from dlt.common.schema import Schema
from dlt.common.utils import custom_environ
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DestinationSchemaWillNotUpdate
from dlt.destinations.impl.bigquery.bigquery import BigQueryClient
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.extract import DltResource
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from tests.load.utils import TABLE_UPDATE


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


def test_configuration() -> None:
    os.environ["MYBG__CREDENTIALS__CLIENT_EMAIL"] = "1234"
    os.environ["MYBG__CREDENTIALS__PRIVATE_KEY"] = "1234"
    os.environ["MYBG__CREDENTIALS__PROJECT_ID"] = "1234"

    # check names normalised
    with custom_environ({"MYBG__CREDENTIALS__PRIVATE_KEY": "---NO NEWLINE---\n"}):
        c = resolve_configuration(GcpServiceAccountCredentialsWithoutDefaults(), sections=("mybg",))
        assert c.private_key == "---NO NEWLINE---\n"

    with custom_environ({"MYBG__CREDENTIALS__PRIVATE_KEY": "---WITH NEWLINE---\n"}):
        c = resolve_configuration(GcpServiceAccountCredentialsWithoutDefaults(), sections=("mybg",))
        assert c.private_key == "---WITH NEWLINE---\n"


@pytest.fixture
def gcp_client(schema: Schema) -> BigQueryClient:
    # return a client without opening connection
    creds = GcpServiceAccountCredentialsWithoutDefaults()
    creds.project_id = "test_project_id"
    # noinspection PydanticTypeChecker
    return BigQueryClient(
        schema,
        BigQueryClientConfiguration(
            dataset_name=f"test_{uniq_id()}", credentials=creds  # type: ignore[arg-type]
        ),
    )


def test_create_table(gcp_client: BigQueryClient) -> None:
    # non existing table
    sql = gcp_client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert sql.startswith("CREATE TABLE")
    assert "event_test_table" in sql
    assert "`col1` INTEGER NOT NULL" in sql
    assert "`col2` FLOAT64 NOT NULL" in sql
    assert "`col3` BOOLEAN NOT NULL" in sql
    assert "`col4` TIMESTAMP NOT NULL" in sql
    assert "`col5` STRING " in sql
    assert "`col6` NUMERIC(38,9) NOT NULL" in sql
    assert "`col7` BYTES" in sql
    assert "`col8` BIGNUMERIC" in sql
    assert "`col9` JSON NOT NULL" in sql
    assert "`col10` DATE" in sql
    assert "`col11` TIME" in sql
    assert "`col1_precision` INTEGER NOT NULL" in sql
    assert "`col4_precision` TIMESTAMP NOT NULL" in sql
    assert "`col5_precision` STRING(25) " in sql
    assert "`col6_precision` NUMERIC(6,2) NOT NULL" in sql
    assert "`col7_precision` BYTES(19)" in sql
    assert "`col11_precision` TIME NOT NULL" in sql
    assert "CLUSTER BY" not in sql
    assert "PARTITION BY" not in sql


def test_alter_table(gcp_client: BigQueryClient) -> None:
    # existing table has no columns
    sql = gcp_client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert sql.startswith("ALTER TABLE")
    assert sql.count("ALTER TABLE") == 1
    assert "event_test_table" in sql
    assert "ADD COLUMN `col1` INTEGER NOT NULL" in sql
    assert "ADD COLUMN `col2` FLOAT64 NOT NULL" in sql
    assert "ADD COLUMN `col3` BOOLEAN NOT NULL" in sql
    assert "ADD COLUMN `col4` TIMESTAMP NOT NULL" in sql
    assert "ADD COLUMN `col5` STRING" in sql
    assert "ADD COLUMN `col6` NUMERIC(38,9) NOT NULL" in sql
    assert "ADD COLUMN `col7` BYTES" in sql
    assert "ADD COLUMN `col8` BIGNUMERIC" in sql
    assert "ADD COLUMN `col9` JSON NOT NULL" in sql
    assert "ADD COLUMN `col10` DATE" in sql
    assert "ADD COLUMN `col11` TIME" in sql
    assert "ADD COLUMN `col1_precision` INTEGER NOT NULL" in sql
    assert "ADD COLUMN `col4_precision` TIMESTAMP NOT NULL" in sql
    assert "ADD COLUMN `col5_precision` STRING(25)" in sql
    assert "ADD COLUMN `col6_precision` NUMERIC(6,2) NOT NULL" in sql
    assert "ADD COLUMN `col7_precision` BYTES(19)" in sql
    assert "ADD COLUMN `col11_precision` TIME NOT NULL" in sql
    # table has col1 already in storage
    mod_table = deepcopy(TABLE_UPDATE)
    mod_table.pop(0)
    sql = gcp_client._get_table_update_sql("event_test_table", mod_table, True)[0]
    assert "ADD COLUMN `col1` INTEGER NOT NULL" not in sql
    assert "ADD COLUMN `col2` FLOAT64 NOT NULL" in sql


def test_create_table_with_partition_and_cluster(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[9]["partition"] = True
    mod_update[4]["cluster"] = True
    mod_update[1]["cluster"] = True
    sql = gcp_client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    # clustering must be the last
    assert sql.endswith("CLUSTER BY `col2`,`col5`")
    assert "PARTITION BY `col10`" in sql


def test_double_partition_exception(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["partition"] = True
    mod_update[4]["partition"] = True
    # double partition
    with pytest.raises(DestinationSchemaWillNotUpdate) as excc:
        gcp_client._get_table_update_sql("event_test_table", mod_update, False)
    assert excc.value.columns == ["`col4`", "`col5`"]


def test_create_table_with_time_partition(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    mod_update[3]["partition"] = True
    sql = gcp_client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert "PARTITION BY DATE(`col4`)" in sql


def test_create_table_with_date_partition(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    mod_update[9]["partition"] = True
    sql = gcp_client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert "PARTITION BY `col10`" in sql


def test_create_table_with_integer_partition(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    mod_update[0]["partition"] = True
    sql = gcp_client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert "PARTITION BY RANGE_BUCKET(`col1`, GENERATE_ARRAY(-172800000, 691200000, 86400))" in sql


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_date(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", full_refresh=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_date_column",
        columns={"my_date_column": {"data_type": "date", "partition": True, "nullable": False}},
    )
    def demo_resource() -> Iterator[Dict[str, pendulum.Date]]:
        for i in range(10):
            yield {
                "my_date_column": pendulum.from_timestamp(1700784000 + i * 50_000).date(),
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert has_partitions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_no_partition_by_date(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", full_refresh=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_date_column",
        columns={"my_date_column": {"data_type": "date", "partition": False, "nullable": False}},
    )
    def demo_resource() -> Iterator[Dict[str, pendulum.Date]]:
        for i in range(10):
            yield {
                "my_date_column": pendulum.from_timestamp(1700784000 + i * 50_000).date(),
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert not has_partitions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_timestamp(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", full_refresh=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_timestamp_column",
        columns={
            "my_timestamp_column": {"data_type": "timestamp", "partition": True, "nullable": False}
        },
    )
    def demo_resource() -> Iterator[Dict[str, pendulum.DateTime]]:
        for i in range(10):
            yield {
                "my_timestamp_column": pendulum.from_timestamp(1700784000 + i * 50_000),
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert has_partitions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_no_partition_by_timestamp(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", full_refresh=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_timestamp_column",
        columns={
            "my_timestamp_column": {"data_type": "timestamp", "partition": False, "nullable": False}
        },
    )
    def demo_resource() -> Iterator[Dict[str, pendulum.DateTime]]:
        for i in range(10):
            yield {
                "my_timestamp_column": pendulum.from_timestamp(1700784000 + i * 50_000),
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert not has_partitions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_integer(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", full_refresh=True)

    @dlt.resource(
        columns={"some_int": {"data_type": "bigint", "partition": True, "nullable": False}},
    )
    def demo_resource() -> Iterator[Dict[str, int]]:
        for i in range(10):
            yield {
                "some_int": i,
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert has_partitions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_no_partition_by_integer(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", full_refresh=True)

    @dlt.resource(
        columns={"some_int": {"data_type": "bigint", "partition": False, "nullable": False}},
    )
    def demo_resource() -> Iterator[Dict[str, int]]:
        for i in range(10):
            yield {
                "some_int": i,
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(
            "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS WHERE partition_id IS NOT"
            " NULL);"
        ) as cur:
            has_partitions = cur.fetchone()[0]
            assert isinstance(has_partitions, bool)
            assert not has_partitions
