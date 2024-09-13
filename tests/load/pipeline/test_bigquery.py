import pytest
import io

import dlt
from dlt.common import Decimal, json
from dlt.common.typing import TLoaderFileFormat

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
