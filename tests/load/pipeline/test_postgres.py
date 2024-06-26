import os
import hashlib
import random
from string import ascii_lowercase
import pytest

from dlt.common.utils import uniq_id

from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info, load_tables_to_dicts
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
