from typing import Any, Dict, Iterator
from git import List
import pytest
import io

import dlt
from dlt.common import Decimal, json, pendulum
from dlt.common.typing import TLoaderFileFormat

from dlt.common.utils import uniq_id
from dlt.destinations.adapters import bigquery_adapter
from dlt.extract.resource import DltResource
from tests.pipeline.utils import assert_load_info
from tests.load.utils import destinations_configs, DestinationTestConfiguration

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_numeric_types(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("test_bigquery_numeric_types", dev_mode=True)

    columns = [
        {"name": "col_big_numeric", "data_type": "decimal", "precision": 47, "scale": 9},
        {"name": "col_numeric", "data_type": "decimal", "precision": 38, "scale": 9},
    ]

    data = [
        {
            # Valid BIGNUMERIC and NUMERIC values
            "col_big_numeric": Decimal("12345678901234567890123456789012345678.123456789"),
            "col_numeric": Decimal("12345678901234567890123456789.123456789"),
        },
    ]

    info = pipeline.run(iter(data), table_name="big_numeric", columns=columns, **destination_config.run_kwargs)  # type: ignore[arg-type]
    assert_load_info(info)

    with pipeline.sql_client() as client:
        with client.execute_query("SELECT col_big_numeric, col_numeric FROM big_numeric;") as q:
            row = q.fetchone()
            assert row[0] == data[0]["col_big_numeric"]
            assert row[1] == data[0]["col_numeric"]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("file_format", ("parquet", "jsonl"))
def test_bigquery_autodetect_schema(
    destination_config: DestinationTestConfiguration, file_format: TLoaderFileFormat
) -> None:
    from dlt.destinations.adapters import bigquery_adapter
    from dlt.destinations.impl.bigquery.sql_client import BigQuerySqlClient

    @dlt.resource(name="cve", max_table_nesting=0, file_format=file_format)
    def load_cve(stage: int):
        with open("tests/load/cases/loading/cve.json", "rb") as f:
            cve = json.load(f)
            if stage == 0:
                # remove a whole struct field
                del cve["references"]
            if stage == 1:
                # remove a field from struct
                for item in cve["references"]["reference_data"]:
                    del item["refsource"]
            if file_format == "jsonl":
                yield cve
            else:
                import pyarrow.json as paj

                table = paj.read_json(io.BytesIO(json.dumpb(cve)))
                yield table

    pipeline = destination_config.setup_pipeline("test_bigquery_autodetect_schema", dev_mode=True)
    # run without one nested field
    cve = bigquery_adapter(load_cve(0), autodetect_schema=True)
    info = pipeline.run(cve)
    assert_load_info(info)
    client: BigQuerySqlClient
    with pipeline.sql_client() as client:  # type: ignore[assignment]
        table = client.native_connection.get_table(
            client.make_qualified_table_name("cve", escape=False)
        )
    field = next(field for field in table.schema if field.name == "source")
    # not repeatable
    assert field.field_type == "RECORD"
    assert field.mode == "NULLABLE"
    field = next(field for field in table.schema if field.name == "credit")
    if file_format == "parquet":
        # parquet wraps struct into repeatable list
        field = field.fields[0]
        assert field.name == "list"
    assert field.field_type == "RECORD"
    assert field.mode == "REPEATED"
    # no references
    field = next((field for field in table.schema if field.name == "references"), None)
    assert field is None

    # evolve schema - add nested field
    cve = bigquery_adapter(load_cve(1), autodetect_schema=True)
    info = pipeline.run(cve)
    assert_load_info(info)
    with pipeline.sql_client() as client:  # type: ignore[assignment]
        table = client.native_connection.get_table(
            client.make_qualified_table_name("cve", escape=False)
        )
    field = next(field for field in table.schema if field.name == "references")
    field = field.fields[0]
    assert field.name == "reference_data"
    if file_format == "parquet":
        # parquet wraps struct into repeatable list
        field = field.fields[0]
        assert field.name == "list"
        assert field.mode == "REPEATED"
        # and enclosed in another type 🤷
        field = field.fields[0]
    else:
        assert field.mode == "REPEATED"
    # make sure url is there
    nested_field = next(f for f in field.fields if f.name == "url")
    assert nested_field.field_type == "STRING"
    # refsource not there
    nested_field = next((f for f in field.fields if f.name == "refsource"), None)
    assert nested_field is None

    # evolve schema - add field to a nested struct
    cve = bigquery_adapter(load_cve(2), autodetect_schema=True)
    info = pipeline.run(cve)
    assert_load_info(info)
    with pipeline.sql_client() as client:  # type: ignore[assignment]
        table = client.native_connection.get_table(
            client.make_qualified_table_name("cve", escape=False)
        )
    field = next(field for field in table.schema if field.name == "references")
    field = field.fields[0]
    if file_format == "parquet":
        # parquet wraps struct into repeatable list
        field = field.fields[0]
        assert field.name == "list"
        assert field.mode == "REPEATED"
        # and enclosed in another type 🤷
        field = field.fields[0]
    # it looks like BigQuery can evolve structs and the field is added
    nested_field = next(f for f in field.fields if f.name == "refsource")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_additional_table_hints_table_expiration(
    destination_config: DestinationTestConfiguration,
) -> None:
    import google

    @dlt.resource(columns=[{"name": "col1", "data_type": "text"}])
    def no_hints() -> Iterator[Dict[str, str]]:
        yield from [{"col1": str(i)} for i in range(10)]

    hints = bigquery_adapter(
        no_hints.with_name(new_name="hints"), table_expiration_datetime="2030-01-01"
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

        assert not no_hints_table.expires
        assert hints_table.expires == pendulum.datetime(2030, 1, 1, 0)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_merge_behaviour(
    destination_config: DestinationTestConfiguration,
) -> None:
    import google
    from google.cloud.bigquery import Table

    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "text"},
            {"name": "col2", "data_type": "bigint"},
            {"name": "col3", "data_type": "double"},
        ]
    )
    def hints() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": str(i), "col2": i, "col3": float(i)} for i in range(10)]

    bigquery_adapter(hints, table_expiration_datetime="2030-01-01", cluster=["col1"])
    bigquery_adapter(
        hints,
        table_description="A small table somewhere in the cosmos...",
        partition="col2",
    )

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(hints)

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        table_fqtn = c.make_qualified_table_name("hints", escape=False)

        table: Table = nc.get_table(table_fqtn)

        table_cluster_fields = [] if table.clustering_fields is None else table.clustering_fields

        # Test merging behaviour.
        assert table.expires == pendulum.datetime(2030, 1, 1, 0)
        assert ["col1"] == table_cluster_fields, "`hints` table IS NOT clustered by `col1`."
        assert table.description == "A small table somewhere in the cosmos..."

        if not table.range_partitioning:
            raise ValueError("`hints` table IS NOT clustered on a column.")
        else:
            assert (
                table.range_partitioning.field == "col2"
            ), "`hints` table IS NOT clustered on column `col2`."


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_adapter_autodetect_schema_with_hints(
    destination_config: DestinationTestConfiguration,
) -> None:
    import google
    from google.cloud.bigquery import Table

    @dlt.resource(
        columns=[
            {"name": "col1", "data_type": "text"},
            {"name": "col2", "data_type": "bigint"},
            {"name": "col3", "data_type": "double"},
        ]
    )
    def general_types() -> Iterator[Dict[str, Any]]:
        yield from [{"col1": str(i), "col2": i, "col3": float(i)} for i in range(10)]

    @dlt.resource(
        columns=[
            {"name": "my_time_column", "data_type": "timestamp"},
        ]
    )
    def partition_time() -> Iterator[Dict[str, Any]]:
        for i in range(10):
            yield {
                "my_time_column": pendulum.from_timestamp(1700784000 + i * 50_000),
            }

    @dlt.resource(
        columns=[
            {"name": "my_date_column", "data_type": "date"},
        ]
    )
    def partition_date() -> Iterator[Dict[str, Any]]:
        for i in range(10):
            yield {
                "my_date_column": pendulum.from_timestamp(1700784000 + i * 50_000).date(),
            }

    bigquery_adapter(
        general_types,
        table_description="A small table somewhere in the cosmos...",
        partition="col2",
        cluster=["col1"],
        autodetect_schema=True,
    )

    pipeline = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline.run(general_types)

    bigquery_adapter(
        partition_time,
        partition="my_time_column",
        autodetect_schema=True,
    )

    pipeline_time = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline_time.run(partition_time)

    bigquery_adapter(
        partition_date,
        partition="my_date_column",
        autodetect_schema=True,
    )

    pipeline_date = destination_config.setup_pipeline(
        f"bigquery_{uniq_id()}",
        dev_mode=True,
    )

    pipeline_date.run(partition_date)

    with pipeline.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection

        table_fqtn = c.make_qualified_table_name("general_types", escape=False)

        table: Table = nc.get_table(table_fqtn)

        table_cluster_fields = [] if table.clustering_fields is None else table.clustering_fields
        assert ["col1"] == table_cluster_fields, "NOT clustered by `col1`."

        assert table.description == "A small table somewhere in the cosmos..."
        assert table.range_partitioning.field == "col2", "NOT partitioned on column `col2`."

    with pipeline_time.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection  # type: ignore[no-redef]
        table_fqtn = c.make_qualified_table_name("partition_time", escape=False)
        table: Table = nc.get_table(table_fqtn)  # type: ignore[no-redef]
        assert table.time_partitioning.field == "my_time_column"

    with pipeline_date.sql_client() as c:
        nc: google.cloud.bigquery.client.Client = c.native_connection  # type: ignore[no-redef]
        table_fqtn = c.make_qualified_table_name("partition_date", escape=False)
        table: Table = nc.get_table(table_fqtn)  # type: ignore[no-redef]
        assert table.time_partitioning.field == "my_date_column"
        assert table.time_partitioning.type_ == "DAY"
