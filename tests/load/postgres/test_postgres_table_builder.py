from copy import deepcopy
from typing import Generator, Any, Dict, List

import pytest
import sqlfluff

import dlt
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import Schema, utils
from dlt.common.typing import DictStrStr
from dlt.common.utils import uniq_id
from dlt.destinations import postgres
from dlt.destinations.impl.postgres.configuration import (
    PostgresClientConfiguration,
    PostgresCredentials,
)
from dlt.destinations.impl.postgres.postgres import PostgresClient
from dlt.destinations.impl.postgres.postgres_adapter import (
    postgres_adapter,
    SRID_HINT,
    GEOMETRY_HINT,
)
from dlt.extract import DltResource
from tests.cases import (
    TABLE_UPDATE,
    TABLE_UPDATE_ALL_INT_PRECISIONS,
)
from tests.load.postgres.utils import generate_sample_geometry_records
from tests.load.utils import destinations_configs, DestinationTestConfiguration, sequence_generator
from tests.utils import assert_load_info

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def client(empty_schema: Schema) -> PostgresClient:
    return create_client(empty_schema)


@pytest.fixture
def cs_client(empty_schema: Schema) -> PostgresClient:
    # change normalizer to case sensitive
    empty_schema._normalizers_config["names"] = "tests.common.cases.normalizers.title_case"
    empty_schema.update_normalizers()
    return create_client(empty_schema)


def create_client(empty_schema: Schema) -> PostgresClient:
    # return client without opening connection
    config = PostgresClientConfiguration(credentials=PostgresCredentials())._bind_dataset_name(
        dataset_name="test_" + uniq_id()
    )
    return postgres().client(empty_schema, config)


def test_create_table(client: PostgresClient) -> None:
    # make sure we are in case insensitive mode
    assert client.capabilities.generates_case_sensitive_identifiers() is False
    # check if dataset name is properly folded
    assert client.sql_client.dataset_name == client.config.dataset_name  # identical to config
    assert (
        client.sql_client.staging_dataset_name
        == client.config.staging_dataset_name_layout % client.config.dataset_name
    )
    # non existing table
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]
    sqlfluff.parse(sql, dialect="postgres")
    qualified_name = client.sql_client.make_qualified_table_name("event_test_table")
    assert f"CREATE TABLE {qualified_name}" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" bytea' in sql
    assert '"col8" numeric(156,78)' in sql
    assert '"col9" jsonb  NOT NULL' in sql
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time without time zone  NOT NULL' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    assert '"col4_precision" timestamp (3) with time zone  NOT NULL' in sql
    assert '"col5_precision" varchar(25)' in sql
    assert '"col6_precision" numeric(6,2)  NOT NULL' in sql
    assert '"col7_precision" bytea' in sql
    assert '"col11_precision" time (3) without time zone  NOT NULL' in sql


def test_create_table_all_precisions(client: PostgresClient) -> None:
    # 128 bit integer will fail
    table_update = list(TABLE_UPDATE_ALL_INT_PRECISIONS)
    with pytest.raises(TerminalValueError) as tv_ex:
        sql = client._get_table_update_sql("event_test_table", table_update, False)[0]
    assert "128" in str(tv_ex.value)

    # remove col5 HUGEINT which is last
    table_update.pop()
    sql = client._get_table_update_sql("event_test_table", table_update, False)[0]
    sqlfluff.parse(sql, dialect="duckdb")
    assert '"col1_int" smallint ' in sql
    assert '"col2_int" smallint ' in sql
    assert '"col3_int" integer ' in sql
    assert '"col4_int" bigint ' in sql


def test_alter_table(client: PostgresClient) -> None:
    # existing table has no columns
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)[0]
    sqlfluff.parse(sql, dialect="postgres")
    canonical_name = client.sql_client.make_qualified_table_name("event_test_table")
    # must have several ALTER TABLE statements
    assert sql.count(f"ALTER TABLE {canonical_name}\nADD COLUMN") == 1
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" bytea' in sql
    assert '"col8" numeric(156,78)' in sql
    assert '"col9" jsonb  NOT NULL' in sql
    assert '"col10" date  NOT NULL' in sql
    assert '"col11" time without time zone  NOT NULL' in sql
    assert '"col1_precision" smallint  NOT NULL' in sql
    assert '"col4_precision" timestamp (3) with time zone  NOT NULL' in sql
    assert '"col5_precision" varchar(25)' in sql
    assert '"col6_precision" numeric(6,2)  NOT NULL' in sql
    assert '"col7_precision" bytea' in sql
    assert '"col11_precision" time (3) without time zone  NOT NULL' in sql


def test_create_table_with_hints(client: PostgresClient, empty_schema: Schema) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[0]["primary_key"] = True
    mod_update[0]["sort"] = True
    mod_update[1]["unique"] = True
    mod_update[4]["parent_key"] = True
    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="postgres")
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision UNIQUE NOT NULL' in sql
    assert '"col5" varchar ' in sql
    # no hints
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql

    # same thing without indexes
    client = postgres().client(
        empty_schema,
        PostgresClientConfiguration(
            create_indexes=False,
            credentials=PostgresCredentials(),
        )._bind_dataset_name(dataset_name="test_" + uniq_id()),
    )
    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql, dialect="postgres")
    assert '"col2" double precision  NOT NULL' in sql


def test_create_table_case_sensitive(cs_client: PostgresClient) -> None:
    # did we switch to case sensitive
    assert cs_client.capabilities.generates_case_sensitive_identifiers() is True
    # check dataset names
    assert cs_client.sql_client.dataset_name.startswith("Test")
    with cs_client.with_staging_dataset():
        assert cs_client.sql_client.dataset_name.endswith("staginG")
    assert cs_client.sql_client.staging_dataset_name.endswith("staginG")
    # check tables
    cs_client.schema.update_table(
        utils.new_table("event_test_table", columns=deepcopy(TABLE_UPDATE))
    )
    sql = cs_client._get_table_update_sql(
        "Event_test_tablE",
        list(cs_client.schema.get_table_columns("Event_test_tablE").values()),
        False,
    )[0]
    sqlfluff.parse(sql, dialect="postgres")
    # everything capitalized
    assert cs_client.sql_client.fully_qualified_dataset_name(escape=False)[0] == "T"  # Test
    # every line starts with "Col"
    for line in sql.split("\n")[1:]:
        assert line.startswith('"Col')


def test_create_dlt_table(client: PostgresClient) -> None:
    # non existing table
    sql = client._get_table_update_sql("_dlt_version", TABLE_UPDATE, False)[0]
    sqlfluff.parse(sql, dialect="postgres")
    qualified_name = client.sql_client.make_qualified_table_name("_dlt_version")
    assert f"CREATE TABLE IF NOT EXISTS {qualified_name}" in sql


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_adapter_geometry_hint_config(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(columns=[{"name": "content", "data_type": "text"}])
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(sequence_generator())

    assert some_data.columns["content"] == {"name": "content", "data_type": "text"}  # type: ignore[index]

    # Default SRID.
    postgres_adapter(some_data, geometry=["content"])

    assert some_data.columns["content"] == {  # type: ignore
        "name": "content",
        "data_type": "text",
        GEOMETRY_HINT: True,
        SRID_HINT: 4326,
    }

    # Nonstandard SRID.
    postgres_adapter(some_data, geometry="content", srid=8232)

    assert some_data.columns["content"] == {  # type: ignore
        "name": "content",
        "data_type": "text",
        GEOMETRY_HINT: True,
        SRID_HINT: 8232,
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_geometry_types(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource
    def geodata_default() -> Generator[List[Dict[str, Any]], Any, Any]:
        yield generate_sample_geometry_records()

    @dlt.resource
    def geodata_default_3857() -> Generator[List[Dict[str, Any]], Any, Any]:
        yield generate_sample_geometry_records()

    @dlt.resource
    def geodata_default_2163() -> Generator[List[Dict[str, Any]], Any, Any]:
        yield generate_sample_geometry_records()

    postgres_adapter(geodata_default, geometry=["geom"])
    postgres_adapter(geodata_default_3857, geometry=["geom"], srid=3857)
    postgres_adapter(geodata_default_2163, geometry=["geom"], srid=2163)

    @dlt.source
    def geodata() -> List[DltResource]:
        return [geodata_default, geodata_default_3857, geodata_default_2163]

    pipeline = destination_config.setup_pipeline("test_geometry_types", dev_mode=True)
    info = pipeline.run(
        geodata(),
    )
    assert_load_info(info)

    with pipeline.sql_client() as c:
        fqtn = c.make_qualified_table_name("geodata", escape=False)
        with c.execute_query(f"""
            SELECT f_geometry_column
            FROM geometry_columns
            WHERE f_table_name = '{fqtn}';
            """) as cur:
            records = cur.fetchall()
            assert records
            assert {record[0] for record in records} == {"geom"}

    # TODO: assert round trip deserialization to client is correct


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_read_from_geopandas_with_native_geodata_type(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Test geopandas geo columns are automatically identified as such and cast to postgis geotype."""
    import geopandas as gpd  # type: ignore

    pipeline = destination_config.setup_pipeline("geodata_pandas_pipeline", dev_mode=True)
    gdf = gpd.read_file("tests/load/cases/loading/sample_geodata.xml")

    info = pipeline.run(gdf, table_name="geodata_pandas")
    assert_load_info(info)

    # Check the 'geometry' field has been cast to a PostGIS geometry type.
    with pipeline.sql_client() as c:
        fqtn_geodata_pandas = c.make_qualified_table_name("geodata_pandas", escape=False)
        with c.execute_query(f"""
            SELECT f_geometry_column, type
            FROM geometry_columns
            WHERE f_table_name = '{fqtn_geodata_pandas}' AND f_geometry_column = 'geometry';
            """) as cur:
            records = cur.fetchone()
            assert records
            assert records[0] == "geometry"
