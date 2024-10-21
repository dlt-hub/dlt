import os
from copy import deepcopy
from typing import Iterator, Dict, Any, List

import google
import pytest
import sqlfluff

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import (
    GcpServiceAccountCredentialsWithoutDefaults,
    GcpServiceAccountCredentials,
)
from dlt.common.destination.exceptions import DestinationSchemaTampered
from dlt.common.pendulum import pendulum
from dlt.common.schema import Schema, utils
from dlt.common.schema.exceptions import SchemaIdentifierNormalizationCollision
from dlt.common.utils import custom_environ
from dlt.common.utils import uniq_id
from dlt.destinations import bigquery
from dlt.destinations.adapters import bigquery_adapter
from dlt.destinations.exceptions import DestinationSchemaWillNotUpdate
from dlt.destinations.impl.bigquery.bigquery import BigQueryClient
from dlt.destinations.impl.bigquery.bigquery_adapter import (
    PARTITION_HINT,
    CLUSTER_HINT,
)
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.extract import DltResource
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    drop_active_pipeline_data,
    TABLE_UPDATE,
    sequence_generator,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


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
def gcp_client(empty_schema: Schema) -> BigQueryClient:
    return create_client(empty_schema)


@pytest.fixture
def ci_gcp_client(empty_schema: Schema) -> BigQueryClient:
    empty_schema._normalizers_config["names"] = "tests.common.cases.normalizers.title_case"
    empty_schema.update_normalizers()
    # make the destination case insensitive
    return create_client(empty_schema, has_case_sensitive_identifiers=False)


def create_client(schema: Schema, has_case_sensitive_identifiers: bool = True) -> BigQueryClient:
    # return a client without opening connection
    creds = GcpServiceAccountCredentials()
    creds.project_id = "test_project_id"
    # noinspection PydanticTypeChecker
    return bigquery().client(
        schema,
        BigQueryClientConfiguration(
            credentials=creds,
            has_case_sensitive_identifiers=has_case_sensitive_identifiers,
            # let modify destination caps
            should_set_case_sensitivity_on_new_dataset=True,
        )._bind_dataset_name(dataset_name=f"test_{uniq_id()}"),
    )


def test_create_table(gcp_client: BigQueryClient) -> None:
    # non existing table
    # Add BIGNUMERIC column
    table_update = TABLE_UPDATE + [
        {
            "name": "col_high_p_decimal",
            "data_type": "decimal",
            "precision": 76,
            "scale": 0,
            "nullable": False,
        },
        {
            "name": "col_high_s_decimal",
            "data_type": "decimal",
            "precision": 38,
            "scale": 24,
            "nullable": False,
        },
    ]
    sql = gcp_client._get_table_update_sql("event_test_table", table_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert sql.startswith("CREATE TABLE")
    assert "event_test_table" in sql
    assert "`col1` INT64 NOT NULL" in sql
    assert "`col2` FLOAT64 NOT NULL" in sql
    assert "`col3` BOOL NOT NULL" in sql
    assert "`col4` TIMESTAMP NOT NULL" in sql
    assert "`col5` STRING " in sql
    assert "`col6` NUMERIC(38,9) NOT NULL" in sql
    assert "`col7` BYTES" in sql
    assert "`col8` BIGNUMERIC" in sql
    assert "`col9` JSON NOT NULL" in sql
    assert "`col10` DATE" in sql
    assert "`col11` TIME" in sql
    assert "`col1_precision` INT64 NOT NULL" in sql
    assert "`col4_precision` TIMESTAMP NOT NULL" in sql
    assert "`col5_precision` STRING(25) " in sql
    assert "`col6_precision` NUMERIC(6,2) NOT NULL" in sql
    assert "`col7_precision` BYTES(19)" in sql
    assert "`col11_precision` TIME NOT NULL" in sql
    assert "`col_high_p_decimal` BIGNUMERIC(76,0) NOT NULL" in sql
    assert "`col_high_s_decimal` BIGNUMERIC(38,24) NOT NULL" in sql
    assert "CLUSTER BY" not in sql
    assert "PARTITION BY" not in sql


def test_alter_table(gcp_client: BigQueryClient) -> None:
    # existing table has no columns
    sql = gcp_client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    assert sql.startswith("ALTER TABLE")
    assert sql.count("ALTER TABLE") == 1
    assert "event_test_table" in sql
    assert "ADD COLUMN `col1` INT64 NOT NULL" in sql
    assert "ADD COLUMN `col2` FLOAT64 NOT NULL" in sql
    assert "ADD COLUMN `col3` BOOL NOT NULL" in sql
    assert "ADD COLUMN `col4` TIMESTAMP NOT NULL" in sql
    assert "ADD COLUMN `col5` STRING" in sql
    assert "ADD COLUMN `col6` NUMERIC(38,9) NOT NULL" in sql
    assert "ADD COLUMN `col7` BYTES" in sql
    assert "ADD COLUMN `col8` BIGNUMERIC" in sql
    assert "ADD COLUMN `col9` JSON NOT NULL" in sql
    assert "ADD COLUMN `col10` DATE" in sql
    assert "ADD COLUMN `col11` TIME" in sql
    assert "ADD COLUMN `col1_precision` INT64 NOT NULL" in sql
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


def test_create_table_case_insensitive(ci_gcp_client: BigQueryClient) -> None:
    # in case insensitive mode
    assert ci_gcp_client.capabilities.has_case_sensitive_identifiers is False
    # case sensitive naming convention
    assert ci_gcp_client.sql_client.dataset_name.startswith("Test")
    with ci_gcp_client.with_staging_dataset():
        assert ci_gcp_client.sql_client.dataset_name.endswith("staginG")
    assert ci_gcp_client.sql_client.staging_dataset_name.endswith("staginG")

    ci_gcp_client.schema.update_table(
        utils.new_table("event_test_table", columns=deepcopy(TABLE_UPDATE))
    )
    sql = ci_gcp_client._get_table_update_sql(
        "Event_test_tablE",
        list(ci_gcp_client.schema.get_table_columns("Event_test_tablE").values()),
        False,
    )[0]
    sqlfluff.parse(sql, dialect="bigquery")
    # everything capitalized

    # every line starts with "Col"
    for line in sql.split("\n")[1:]:
        assert line.startswith("`Col")

    # generate collision
    ci_gcp_client.schema.update_table(
        utils.new_table("event_TEST_table", columns=deepcopy(TABLE_UPDATE))
    )
    assert "Event_TEST_tablE" in ci_gcp_client.schema.tables
    with pytest.raises(SchemaIdentifierNormalizationCollision) as coll_ex:
        ci_gcp_client.verify_schema()
    assert coll_ex.value.conflict_identifier_name == "Event_test_tablE"
    assert coll_ex.value.table_name == "Event_TEST_tablE"

    # make it case sensitive
    ci_gcp_client.capabilities.has_case_sensitive_identifiers = True
    # now the check passes, we are stopped because it is not allowed to change schema in the loader
    with pytest.raises(DestinationSchemaTampered):
        ci_gcp_client.verify_schema()
        ci_gcp_client.update_stored_schema([])


def test_create_table_with_partition_and_cluster(gcp_client: BigQueryClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[9]["partition"] = True
    mod_update[4]["cluster"] = True
    mod_update[1]["cluster"] = True
    sql = gcp_client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="bigquery")
    # clustering must be the last
    assert sql.endswith("CLUSTER BY `col2`, `col5`")
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
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_date(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_date_column",
        columns={
            "my_date_column": {
                "data_type": "date",
                "partition": True,
                "nullable": False,
            }
        },
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
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_no_partition_by_date(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_date_column",
        columns={
            "my_date_column": {
                "data_type": "date",
                "partition": False,
                "nullable": False,
            }
        },
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
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_timestamp(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_timestamp_column",
        columns={
            "my_timestamp_column": {
                "data_type": "timestamp",
                "partition": True,
                "nullable": False,
            }
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
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_no_partition_by_timestamp(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_timestamp_column",
        columns={
            "my_timestamp_column": {
                "data_type": "timestamp",
                "partition": False,
                "nullable": False,
            }
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
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_integer(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

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
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_no_partition_by_integer(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", dev_mode=True)

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


@pytest.fixture(autouse=True)
def drop_bigquery_schema() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def test_adapter_no_hints_parsing() -> None:
    @dlt.resource(columns=[{"name": "int_col", "data_type": "bigint"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    assert some_data.columns == {
        "int_col": {"name": "int_col", "data_type": "bigint"},
    }


def test_adapter_hints_parsing_partitioning_more_than_one_column() -> None:
    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "bigint"},
            {"name": "col2", "data_type": "bigint"},
        ]
    )
    def some_data() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": str(i), "col2": i} for i in range(3)]

    assert some_data.columns == {
        "col1": {"data_type": "bigint", "name": "col1"},
        "col2": {"data_type": "bigint", "name": "col2"},
    }

    with pytest.raises(ValueError, match="^`partition` must be a single column name as a string.$"):
        bigquery_adapter(some_data, partition=["col1", "col2"])


def test_adapter_hints_parsing_partitioning() -> None:
    @dlt.resource(columns=[{"name": "int_col", "data_type": "bigint"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    bigquery_adapter(some_data, partition="int_col", partition_expiration_days=4)
    assert some_data.columns == {
        "int_col": {
            "name": "int_col",
            "data_type": "bigint",
            "x-bigquery-partition": True,
        },
    }
    table_schema = some_data.compute_table_schema()
    assert table_schema["x-bigquery-partition-expiration-days"] == 4  # type: ignore[typeddict-item]


def test_adapter_on_data() -> None:
    hints = bigquery_adapter([{"col2": "ABC"}], partition="col2")
    assert hints.name == "content"
    assert hints._pipe.gen == [{"col2": "ABC"}]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_hints_partitioning(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "bigint"}])
    def no_hints() -> Iterator[Dict[str, int]]:
        yield from [{"col1": i} for i in range(10)]

    @dlt.resource(columns=[{"name": "col1", "data_type": "date"}])
    def date_no_hints() -> Iterator[Dict[str, pendulum.Date]]:
        yield from [{"col1": pendulum.now().add(days=i).date()} for i in range(10)]

    hints = bigquery_adapter(no_hints.with_name(new_name="hints"), partition="col1")
    date_hints = bigquery_adapter(
        date_no_hints.with_name(new_name="date_hints"),
        partition="col1",
        partition_expiration_days=3,
    )

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints, date_hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        fqtn_no_hints = c.make_qualified_table_name("no_hints", escape=False)
        fqtn_hints = c.make_qualified_table_name("hints", escape=False)
        fqtn_date_hints = c.make_qualified_table_name("date_hints", escape=False)

        no_hints_table = nc.get_table(fqtn_no_hints)
        hints_table = nc.get_table(fqtn_hints)
        date_hints_table = nc.get_table(fqtn_date_hints)

        assert not no_hints_table.range_partitioning, "`no_hints` table IS clustered on a column."
        assert date_hints_table.time_partitioning.expiration_ms == 3 * 24 * 60 * 60 * 1000

        if not hints_table.range_partitioning:
            raise ValueError("`hints` table IS NOT clustered on a column.")
        else:
            assert (
                hints_table.range_partitioning.field == "col1"
            ), "`hints` table IS NOT clustered on column `col1`."


def test_adapter_hints_parsing_round_half_away_from_zero() -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "wei"}])
    def hints() -> Iterator[Dict[str, float]]:
        yield from [{"col1": float(i)} for i in range(10)]

    bigquery_adapter(hints, round_half_away_from_zero="col1")

    assert hints.columns == {
        "col1": {
            "name": "col1",
            "data_type": "wei",
            "x-bigquery-round-half-away-from-zero": True,
        },
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_hints_round_half_away_from_zero(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "wei"}])
    def no_hints() -> Iterator[Dict[str, float]]:
        yield from [{"col1": float(i)} for i in range(10)]

    hints = bigquery_adapter(no_hints.with_name(new_name="hints"), round_half_away_from_zero="col1")

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        with c.execute_query("""
                SELECT table_name, rounding_mode
                FROM `INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name IN ('no_hints', 'hints')
                  AND column_name = 'col1';""") as cur:
            results = cur.fetchall()

            hints_rounding_mode = None
            no_hints_rounding_mode = None

            for row in results:
                if row["table_name"] == "no_hints":  # type: ignore
                    no_hints_rounding_mode = row["rounding_mode"]  # type: ignore
                elif row["table_name"] == "hints":  # type: ignore
                    hints_rounding_mode = row["rounding_mode"]  # type: ignore

            assert (no_hints_rounding_mode is None) and (
                hints_rounding_mode == "ROUND_HALF_AWAY_FROM_ZERO"
            )


def test_adapter_hints_parsing_round_half_even() -> None:
    @dlt.resource(columns=[{"name": "double_col", "data_type": "double"}])
    def some_data() -> Iterator[Dict[str, float]]:
        yield from [{"double_col": float(i)} for i in range(3)]

    bigquery_adapter(some_data, round_half_even="double_col")
    assert some_data.columns == {
        "double_col": {
            "name": "double_col",
            "data_type": "double",
            "x-bigquery-round-half-even": True,
        },
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_hints_round_half_even(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "wei"}])
    def no_hints() -> Iterator[Dict[str, float]]:
        yield from [{"col1": float(i)} for i in range(10)]

    hints = bigquery_adapter(no_hints.with_name(new_name="hints"), round_half_even="col1")

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        with c.execute_query("""
                SELECT table_name, rounding_mode
                FROM `INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name IN ('no_hints', 'hints')
                  AND column_name = 'col1';""") as cur:
            results = cur.fetchall()

            hints_rounding_mode = None
            no_hints_rounding_mode = None

            for row in results:
                if row["table_name"] == "no_hints":  # type: ignore
                    no_hints_rounding_mode = row["rounding_mode"]  # type: ignore
                elif row["table_name"] == "hints":  # type: ignore
                    hints_rounding_mode = row["rounding_mode"]  # type: ignore

            assert (no_hints_rounding_mode is None) and (hints_rounding_mode == "ROUND_HALF_EVEN")


def test_adapter_hints_parsing_clustering() -> None:
    @dlt.resource(columns=[{"name": "int_col", "data_type": "bigint"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    bigquery_adapter(some_data, cluster="int_col")
    assert some_data.columns == {
        "int_col": {
            "name": "int_col",
            "data_type": "bigint",
            "x-bigquery-cluster": True,
        },
    }


def test_adapter_hints_parsing_multiple_clustering() -> None:
    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "bigint"},
            {"name": "col2", "data_type": "text"},
        ]
    )
    def some_data() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": i, "col2": str(i)} for i in range(10)]

    bigquery_adapter(some_data, cluster=["col1", "col2"])
    assert some_data.columns == {
        "col1": {"name": "col1", "data_type": "bigint", "x-bigquery-cluster": True},
        "col2": {"name": "col2", "data_type": "text", "x-bigquery-cluster": True},
    }


def test_adapter_hints_merge() -> None:
    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "text"},
            {"name": "col2", "data_type": "bigint"},
        ]
    )
    def hints() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": str(i), "col2": i} for i in range(10)]

    bigquery_adapter(hints, cluster=["col1"])
    bigquery_adapter(hints, partition="col2")

    assert hints.columns == {
        "col1": {"name": "col1", "data_type": "text", CLUSTER_HINT: True},
        "col2": {"name": "col2", "data_type": "bigint", PARTITION_HINT: True},
    }


def test_adapter_hints_unset() -> None:
    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "text"},
            {"name": "col2", "data_type": "bigint"},
        ]
    )
    def hints() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": str(i), "col2": i} for i in range(10)]

    bigquery_adapter(hints, partition="col1")
    bigquery_adapter(hints, partition="col2")

    assert hints.columns == {
        "col1": {"name": "col1", "data_type": "text"},
        "col2": {"name": "col2", "data_type": "bigint", PARTITION_HINT: True},
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_hints_multiple_clustering(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "bigint"},
            {"name": "col2", "data_type": "text"},
            {"name": "col3", "data_type": "text"},
            {"name": "col4", "data_type": "text"},
        ]
    )
    def no_hints() -> Iterator[Dict[str, Any]]:
        yield from [
            {
                "col1": i,
                "col2": str(i),
                "col3": str(i),
                "col4": str(i),
            }
            for i in range(10)
        ]

    hints = bigquery_adapter(
        no_hints.with_name(new_name="hints"), cluster=["col1", "col2", "col3", "col4"]
    )

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        fqtn_no_hints = c.make_qualified_table_name("no_hints", escape=False)
        fqtn_hints = c.make_qualified_table_name("hints", escape=False)

        no_hints_table = nc.get_table(fqtn_no_hints)
        hints_table = nc.get_table(fqtn_hints)

        no_hints_cluster_fields = (
            [] if no_hints_table.clustering_fields is None else no_hints_table.clustering_fields
        )
        hints_cluster_fields = (
            [] if hints_table.clustering_fields is None else hints_table.clustering_fields
        )

        assert not no_hints_cluster_fields, "`no_hints` table IS clustered some column."
        assert [
            "col1",
            "col2",
            "col3",
            "col4",
        ] == hints_cluster_fields


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_hints_clustering(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "text"}])
    def no_hints() -> Iterator[Dict[str, str]]:
        yield from [{"col1": str(i)} for i in range(10)]

    hints = bigquery_adapter(no_hints.with_name(new_name="hints"), cluster="col1")

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        fqtn_no_hints = c.make_qualified_table_name("no_hints", escape=False)
        fqtn_hints = c.make_qualified_table_name("hints", escape=False)

        no_hints_table = nc.get_table(fqtn_no_hints)
        hints_table = nc.get_table(fqtn_hints)

        no_hints_cluster_fields = (
            [] if no_hints_table.clustering_fields is None else no_hints_table.clustering_fields
        )
        hints_cluster_fields = (
            [] if hints_table.clustering_fields is None else hints_table.clustering_fields
        )

        assert not no_hints_cluster_fields, "`no_hints` table IS clustered by `col1`."
        assert ["col1"] == hints_cluster_fields, "`hints` table IS NOT clustered by `col1`."


def test_adapter_hints_empty() -> None:
    @dlt.resource(columns=[{"name": "int_col", "data_type": "bigint"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    with pytest.raises(
        ValueError,
        match="^AT LEAST one of `partition`, `cluster`, `round_half_away_from_zero`",
    ):
        bigquery_adapter(some_data)


def test_adapter_hints_round_mutual_exclusivity_requirement() -> None:
    @dlt.resource(columns=[{"name": "double_col", "data_type": "double"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    with pytest.raises(
        ValueError,
        match=(
            "are present in both `round_half_away_from_zero` and `round_half_even` "
            "which is not allowed. They must be mutually exclusive.$"
        ),
    ):
        bigquery_adapter(
            some_data,
            round_half_away_from_zero="double_col",
            round_half_even="double_col",
        )


def test_adapter_additional_table_hints_parsing_table_description() -> None:
    @dlt.resource(columns=[{"name": "double_col", "data_type": "double"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    table_description = "Once upon a time a small table got hinted."
    bigquery_adapter(some_data, table_description=table_description)

    assert some_data._hints["x-bigquery-table-description"] == table_description  # type: ignore


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_additional_table_hints_table_description(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "text"}])
    def no_hints() -> Iterator[Dict[str, str]]:
        yield from [{"col1": str(i)} for i in range(10)]

    hints = bigquery_adapter(
        no_hints.with_name(new_name="hints"),
        table_description="Once upon a time a small table got hinted.",
    )

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        fqtn_no_hints = c.make_qualified_table_name("no_hints", escape=False)
        fqtn_hints = c.make_qualified_table_name("hints", escape=False)

        no_hints_table = nc.get_table(fqtn_no_hints)
        hints_table = nc.get_table(fqtn_hints)

        assert not no_hints_table.description
        assert hints_table.description == "Once upon a time a small table got hinted."


@pytest.mark.skip("Alter OPTION schema migrations not implemented yet.")
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_additional_table_hints_table_description_with_alter_table(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(columns=[{"name": "col1", "data_type": "text"}])
    def no_hints() -> Iterator[Dict[str, str]]:
        yield from [{"col1": str(i)} for i in range(10)]

    hints = bigquery_adapter(
        no_hints.with_name(new_name="hints"),
        table_description="Once upon a time a small table got hinted.",
    )

    @dlt.source(max_table_nesting=0)
    def sources() -> List[DltResource]:
        return [no_hints, hints]

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(sources())

    mod_hints = bigquery_adapter(
        dlt.resource([{"col2": "ABC"}], name="hints"),
        table_description="Once upon a time a small table got hinted twice.",
    )
    pipeline.run(mod_hints)
    assert pipeline.last_trace.last_normalize_info.row_counts["hints"] == 1

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        fqtn_no_hints = c.make_qualified_table_name("no_hints", escape=False)
        fqtn_hints = c.make_qualified_table_name("hints", escape=False)

        no_hints_table = nc.get_table(fqtn_no_hints)
        hints_table = nc.get_table(fqtn_hints)

        assert not no_hints_table.description
        assert hints_table.description == "Once upon a time a small table got hinted twice."


def test_adapter_additional_table_hints_parsing_table_expiration() -> None:
    @dlt.resource(columns=[{"name": "double_col", "data_type": "double"}])
    def some_data() -> Iterator[Dict[str, str]]:
        yield from next(sequence_generator())

    bigquery_adapter(some_data, table_expiration_datetime="2030-01-01")

    assert some_data._hints["x-bigquery-table-expiration"] == pendulum.datetime(2030, 1, 1)  # type: ignore
