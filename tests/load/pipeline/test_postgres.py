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
    c = p._get_destination_clients(Schema("s"))[0]
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
    c = p._get_destination_clients(Schema("s"))[0]
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
    c = p._get_destination_clients(Schema("s"))[0]
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
