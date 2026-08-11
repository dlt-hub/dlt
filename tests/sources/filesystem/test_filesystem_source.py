from typing import cast
import pytest

import dlt

from dlt.sources.filesystem.helpers import TFilesystemDataLocation
from dlt.sources.filesystem import filesystem

from tests.load.utils import HTTP_BUCKET
from tests.pipeline.utils import assert_load_info
from tests.utils import public_http_server


@pytest.mark.parametrize(
    "bucket_url",
    [
        HTTP_BUCKET,
    ],
)
def test_http_filesystem(public_http_server, bucket_url: str):
    public_resource = filesystem(bucket_url=bucket_url, file_glob="parquet/mlb_players.parquet")
    pipeline = dlt.pipeline("test_http_load", dev_mode=True, destination="duckdb")
    # just execute iterator
    load_info = pipeline.run(
        [
            public_resource.with_name("http_parquet_example"),
        ]
    )
    assert_load_info(load_info)
    assert pipeline.last_trace.last_normalize_info.row_counts["http_parquet_example"] == 1

    # the bucket and glob it listed are recorded as the input of the run
    extract_info = pipeline.last_trace.last_extract_info
    inputs = extract_info.metrics[extract_info.loads_ids[0]][0]["inputs"]
    assert len(inputs) == 1
    assert inputs[0]["kind"] == "filesystem"
    assert inputs[0]["location"] == bucket_url
    assert cast(TFilesystemDataLocation, inputs[0])["glob"] == "parquet/mlb_players.parquet"
    assert inputs[0]["resource_name"] == "http_parquet_example"
