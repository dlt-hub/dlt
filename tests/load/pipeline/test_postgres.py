import os
import hashlib
import random
from string import ascii_lowercase
import pytest

import dlt
from dlt.common.destination.reference import Destination
from dlt.common.schema.exceptions import CannotCoerceColumnException
from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id

from dlt.destinations import filesystem, redshift

from dlt.pipeline.exceptions import PipelineStepFailed

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    drop_active_pipeline_data,
)
from tests.pipeline.utils import assert_load_info, load_table_counts, load_tables_to_dicts
from tests.utils import TestDataItemFormat


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("item_type", ["object", "table"])
def test_postgres_encoded_binary(
    destination_config: DestinationTestConfiguration, item_type: TestDataItemFormat
) -> None:
    import pyarrow

    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    blob = hashlib.sha3_256(random.choice(ascii_lowercase).encode()).digest()
    # encode as \x... which postgres understands
    blob_table = pyarrow.Table.from_pylist([{"hash": b"\\x" + blob.hex().encode("ascii")}])
    if item_type == "object":
        blob_table = blob_table.to_pylist()
        print(blob_table)

    pipeline = destination_config.setup_pipeline("postgres_" + uniq_id(), dev_mode=True)
    load_info = pipeline.run(blob_table, table_name="table", loader_file_format="csv")
    assert_load_info(load_info)
    job = load_info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    # assert if column inferred correctly
    assert pipeline.default_schema.get_table_columns("table")["hash"]["data_type"] == "binary"

    data = load_tables_to_dicts(pipeline, "table")
    # print(bytes(data["table"][0]["hash"]))
    # data in postgres equals unencoded blob
    assert data["table"][0]["hash"].tobytes() == blob


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_pipeline_explicit_destination_credentials(
    destination_config: DestinationTestConfiguration,
) -> None:
    from dlt.destinations import postgres
    from dlt.destinations.impl.postgres.configuration import PostgresCredentials

    # explicit credentials resolved
    p = dlt.pipeline(
        destination=Destination.from_reference(
            "postgres",
            destination_name="mydest",
            credentials="postgresql://loader:loader@localhost:7777/dlt_data",
        ),
    )
    c = p._get_destination_clients(Schema("s"), p._get_destination_client_initial_config())[0]
    assert c.config.credentials.port == 7777  # type: ignore[attr-defined]

    # TODO: may want to clear the env completely and ignore/mock config files somehow to avoid side effects
    # explicit credentials resolved ignoring the config providers
    os.environ["DESTINATION__MYDEST__CREDENTIALS__HOST"] = "HOST"
    p = dlt.pipeline(
        destination=Destination.from_reference(
            "postgres",
            destination_name="mydest",
            credentials="postgresql://loader:loader@localhost:5432/dlt_data",
        ),
    )
    c = p._get_destination_clients(Schema("s"), p._get_destination_client_initial_config())[0]
    assert c.config.credentials.host == "localhost"  # type: ignore[attr-defined]

    # explicit partial credentials will use config providers
    os.environ["DESTINATION__MYDEST__CREDENTIALS__USERNAME"] = "UN"
    os.environ["DESTINATION__MYDEST__CREDENTIALS__PASSWORD"] = "PW"
    p = dlt.pipeline(
        destination=Destination.from_reference(
            "postgres",
            destination_name="mydest",
            credentials="postgresql://localhost:5432/dlt_data",
        ),
    )
    c = p._get_destination_clients(Schema("s"), p._get_destination_client_initial_config())[0]
    assert c.config.credentials.username == "UN"  # type: ignore[attr-defined]
    # host is taken form explicit credentials
    assert c.config.credentials.host == "localhost"  # type: ignore[attr-defined]

    # instance of credentials will be simply passed
    cred = PostgresCredentials("postgresql://user:pass@localhost/dlt_data")
    p = dlt.pipeline(destination=postgres(credentials=cred))
    inner_c = p.destination_client()
    assert inner_c.config.credentials is cred

    # with staging
    p = dlt.pipeline(
        pipeline_name="postgres_pipeline",
        staging=filesystem("_storage"),
        destination=redshift(credentials="redshift://loader:password@localhost:5432/dlt_data"),
    )
    config = p.destination_client().config
    assert config.credentials.is_resolved()
    assert (
        config.credentials.to_native_representation()
        == "redshift://loader:password@localhost:5432/dlt_data?connect_timeout=15"
    )


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_pipeline_with_sources_sharing_schema(
    destination_config: DestinationTestConfiguration,
) -> None:
    schema = Schema("shared")

    @dlt.source(schema=schema, max_table_nesting=1)
    def source_1():
        @dlt.resource(primary_key="user_id")
        def gen1():
            dlt.current.source_state()["source_1"] = True
            dlt.current.resource_state()["source_1"] = True
            yield {"id": "Y", "user_id": "user_y"}

        @dlt.resource(columns={"col": {"data_type": "bigint"}})
        def conflict():
            yield "conflict"

        return gen1, conflict

    @dlt.source(schema=schema, max_table_nesting=2)
    def source_2():
        @dlt.resource(primary_key="id")
        def gen1():
            dlt.current.source_state()["source_2"] = True
            dlt.current.resource_state()["source_2"] = True
            yield {"id": "X", "user_id": "user_X"}

        def gen2():
            yield from "CDE"

        @dlt.resource(columns={"col": {"data_type": "bool"}}, selected=False)
        def conflict():
            yield "conflict"

        return gen2, gen1, conflict

    # all selected tables with hints should be there
    discover_1 = source_1().discover_schema()
    assert "gen1" in discover_1.tables
    assert discover_1.tables["gen1"]["columns"]["user_id"]["primary_key"] is True
    assert "data_type" not in discover_1.tables["gen1"]["columns"]["user_id"]
    assert "conflict" in discover_1.tables
    assert discover_1.tables["conflict"]["columns"]["col"]["data_type"] == "bigint"

    discover_2 = source_2().discover_schema()
    assert "gen1" in discover_2.tables
    assert "gen2" in discover_2.tables
    # conflict deselected
    assert "conflict" not in discover_2.tables

    p = dlt.pipeline(pipeline_name="multi", destination="duckdb", dev_mode=True)
    p.extract([source_1(), source_2()], table_format=destination_config.table_format)
    default_schema = p.default_schema
    gen1_table = default_schema.tables["gen1"]
    assert "user_id" in gen1_table["columns"]
    assert "id" in gen1_table["columns"]
    assert "conflict" in default_schema.tables
    assert "gen2" in default_schema.tables
    p.normalize(loader_file_format=destination_config.file_format)
    assert "gen2" in default_schema.tables
    p.load()
    table_names = [t["name"] for t in default_schema.data_tables()]
    counts = load_table_counts(p, *table_names)
    assert counts == {"gen1": 2, "gen2": 3, "conflict": 1}
    # both sources share the same state
    assert p.state["sources"] == {
        "shared": {
            "source_1": True,
            "resources": {"gen1": {"source_1": True, "source_2": True}},
            "source_2": True,
        }
    }
    drop_active_pipeline_data()

    # same pipeline but enable conflict
    p = dlt.pipeline(pipeline_name="multi", destination="duckdb", dev_mode=True)
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.extract([source_1(), source_2().with_resources("conflict")])
    assert isinstance(py_ex.value.__context__, CannotCoerceColumnException)


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_many_pipelines_single_dataset(destination_config: DestinationTestConfiguration) -> None:
    schema = Schema("shared")

    @dlt.source(schema=schema, max_table_nesting=1)
    def source_1():
        @dlt.resource(primary_key="user_id")
        def gen1():
            dlt.current.source_state()["source_1"] = True
            dlt.current.resource_state()["source_1"] = True
            yield {"id": "Y", "user_id": "user_y"}

        return gen1

    @dlt.source(schema=schema, max_table_nesting=2)
    def source_2():
        @dlt.resource(primary_key="id")
        def gen1():
            dlt.current.source_state()["source_2"] = True
            dlt.current.resource_state()["source_2"] = True
            yield {"id": "X", "user_id": "user_X"}

        def gen2():
            yield from "CDE"

        return gen2, gen1

    # load source_1 to common dataset
    p = dlt.pipeline(
        pipeline_name="source_1_pipeline", destination="duckdb", dataset_name="shared_dataset"
    )
    p.run(source_1(), credentials="duckdb:///_storage/test_quack.duckdb")
    counts = load_table_counts(p, *p.default_schema.tables.keys())
    assert counts.items() >= {"gen1": 1, "_dlt_pipeline_state": 1, "_dlt_loads": 1}.items()
    p._wipe_working_folder()
    p.deactivate()

    p = dlt.pipeline(
        pipeline_name="source_2_pipeline", destination="duckdb", dataset_name="shared_dataset"
    )
    p.run(source_2(), credentials="duckdb:///_storage/test_quack.duckdb")
    # table_names = [t["name"] for t in p.default_schema.data_tables()]
    counts = load_table_counts(p, *p.default_schema.tables.keys())
    # gen1: one record comes from source_1, 1 record from source_2
    assert counts.items() >= {"gen1": 2, "_dlt_pipeline_state": 2, "_dlt_loads": 2}.items()
    # assert counts == {'gen1': 2, 'gen2': 3}
    p._wipe_working_folder()
    p.deactivate()

    # restore from destination, check state
    p = dlt.pipeline(
        pipeline_name="source_1_pipeline",
        destination=dlt.destinations.duckdb(credentials="duckdb:///_storage/test_quack.duckdb"),
        dataset_name="shared_dataset",
    )
    p.sync_destination()
    # we have our separate state
    assert p.state["sources"]["shared"] == {
        "source_1": True,
        "resources": {"gen1": {"source_1": True}},
    }
    # but the schema was common so we have the earliest one
    assert "gen2" in p.default_schema.tables
    p._wipe_working_folder()
    p.deactivate()

    p = dlt.pipeline(
        pipeline_name="source_2_pipeline",
        destination=dlt.destinations.duckdb(credentials="duckdb:///_storage/test_quack.duckdb"),
        dataset_name="shared_dataset",
    )
    p.sync_destination()
    # we have our separate state
    assert p.state["sources"]["shared"] == {
        "source_2": True,
        "resources": {"gen1": {"source_2": True}},
    }


# TODO: uncomment and finalize when we implement encoding for psycopg2
# @pytest.mark.parametrize(
#     "destination_config",
#     destinations_configs(default_sql_configs=True, subset=["postgres"]),
#     ids=lambda x: x.name,
# )
# def test_postgres_encoding(destination_config: DestinationTestConfiguration):
#     from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient
#     pipeline = destination_config.setup_pipeline("postgres_" + uniq_id(), dev_mode=True)
#     client: Psycopg2SqlClient = pipeline.sql_client()
#     # client.credentials.query["encoding"] = "ru"
#     with client:
#         print(client.native_connection.encoding)
