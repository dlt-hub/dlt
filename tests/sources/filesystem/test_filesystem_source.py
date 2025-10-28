import pytest

import dlt

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
