import os
from copy import deepcopy
from typing import Generator, Any, Dict, List

import pytest
import sqlfluff
from shapely import wkb, wkt

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
from dlt.destinations.impl.postgres.postgres import (
    PostgresClient,
    PostgresInsertValuesWithGeometryTypesLoadJob,
)
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


def test_parse_geometry_func():
    geom_func = PostgresInsertValuesWithGeometryTypesLoadJob._parse_geometry

    # Geometric representations
    valid_point_wkt = "POINT(0 0)"
    result = geom_func(valid_point_wkt)
    assert result is not None
    assert isinstance(result, str)
    expected_hex = wkb.dumps(wkt.loads(valid_point_wkt)).hex()
    assert result.lower() == expected_hex.lower()

    valid_linestring_wkt = "LINESTRING(0 0, 1 1, 2 2)"
    linestring_wkb = wkb.dumps(wkt.loads(valid_linestring_wkt))
    result = geom_func(linestring_wkb)
    assert result is not None
    assert isinstance(result, str)
    assert result.lower() == linestring_wkb.hex().lower()

    valid_polygon_wkt = "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))"
    result = geom_func(valid_polygon_wkt)
    assert result is not None
    assert isinstance(result, str)
    expected_hex = wkb.dumps(wkt.loads(valid_polygon_wkt)).hex()
    assert result.lower() == expected_hex.lower()

    valid_multipolygon_wkt = "MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))"
    result = geom_func(valid_multipolygon_wkt)
    assert result is not None
    assert isinstance(result, str)
    expected_hex = wkb.dumps(wkt.loads(valid_multipolygon_wkt)).hex()
    assert result.lower() == expected_hex.lower()

    # Non-geometric literals
    invalid_wkt_string = "NOT A VALID WKT STRING"
    result = geom_func(invalid_wkt_string)
    assert result is None

    null_input = None
    result = geom_func(null_input)
    assert result is None

    empty_string_input = ""
    result = geom_func(empty_string_input)
    assert result is None

    non_geometric_integer = 42
    result = geom_func(non_geometric_integer)
    assert result is None

    empty_bytes_input = b''
    result = geom_func(empty_bytes_input)
    assert result is None



@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_geometry_types(
    destination_config: DestinationTestConfiguration,
) -> None:
    from shapely import wkt, wkb
    from shapely import LinearRing

    os.environ["LOAD__WORKERS"] = "1"

    @dlt.resource
    def geodata_default() -> Generator[List[Dict[str, Any]], Any, Any]:
        yield generate_sample_geometry_records()

    @dlt.resource
    def geodata_3857() -> Generator[List[Dict[str, Any]], Any, Any]:
        yield generate_sample_geometry_records()

    @dlt.resource
    def geodata_2163() -> Generator[List[Dict[str, Any]], Any, Any]:
        yield generate_sample_geometry_records()

    postgres_adapter(geodata_default, geometry=["geom"])
    postgres_adapter(geodata_3857, geometry=["geom"], srid=3857)
    postgres_adapter(geodata_2163, geometry=["geom"], srid=2163)

    @dlt.source
    def geodata() -> List[DltResource]:
        return [geodata_default, geodata_3857, geodata_2163]

    pipeline = destination_config.setup_pipeline("test_geometry_types", dev_mode=True)
    info = pipeline.run(
        geodata(),
    )
    assert_load_info(info)

    # Assert that types were read in as PostGIS geometry types.
    with pipeline.sql_client() as c:
        with c.execute_query(f"""SELECT f_geometry_column
            FROM geometry_columns
            WHERE f_table_name in ('geodata_3857', 'geodata_2163', 'geodata_default')
              AND f_table_schema = '{pipeline.default_schema.name}' """) as cur:
            records = cur.fetchall()
            assert records
            assert {record[0] for record in records} == {"geom"}

    # Verify round-trip integrity.
    with pipeline.sql_client() as c:
        for resource in ["geodata_default", "geodata_3857", "geodata_2163"]:
            srid = 4326 if resource == "geodata_default" else int(resource.split("_")[1])

            query = f"""
             SELECT type, ST_AsText(geom) as wkt, ST_SRID(geom) as srid, ST_AsBinary(geom) as wkb
             FROM {pipeline.default_schema.name}.{resource}
             """

            with c.execute_query(query) as cur:
                results = cur.fetchall()

            original_geometries = generate_sample_geometry_records()

            for result in results:
                db_type, db_wkt, db_srid, db_wkb = result
                orig_geom = next((g for g in original_geometries if g["type"] == db_type), None)

                assert orig_geom is not None, f"No matching original geometry found for {db_type}"

                assert (
                    db_srid == srid
                ), f"SRID mismatch for {db_type}: expected {srid}, got {db_srid}"

                if "Empty" in db_type:
                    assert wkt.loads(db_wkt).is_empty, f"Expected empty geometry for {db_type}"
                else:
                    if "_wkt" in db_type:
                        orig_geom = wkt.loads(orig_geom["geom"])
                        db_geom = wkt.loads(db_wkt)
                    elif "_wkb" in db_type:
                        orig_geom = wkb.loads(orig_geom["geom"])
                        db_geom = wkb.loads(bytes(db_wkb))
                    elif "_wkb_hex" in db_type:
                        orig_geom = wkb.loads(bytes.fromhex(orig_geom["geom"]))
                        db_geom = wkb.loads(bytes(db_wkb))

                    tolerance = 1e-8
                    if isinstance(orig_geom, LinearRing):
                        assert LinearRing(db_geom.exterior.coords).equals_exact(
                            orig_geom, tolerance
                        ), f"Geometry mismatch for {db_type}"
                    else:
                        assert orig_geom.equals_exact(
                            db_geom, tolerance
                        ), f"Geometry mismatch for {db_type}"


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

    # TODO: read dataframe back into gpd frame and assert equal to original
