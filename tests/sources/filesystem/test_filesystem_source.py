import pytest

from tests.common.storages.utils import TEST_SAMPLE_FILES


@pytest.mark.parametrize(
    "example_name",
    (
        "read_custom_file_type_excel",
        "stream_and_merge_csv",
        "read_csv_with_duckdb",
        "read_csv_duckdb_compressed",
        "read_parquet_and_jsonl_chunked",
        "read_files_incrementally_mtime",
    ),
)
def test_all_examples(example_name: str) -> None:
    from dlt.sources import filesystem_pipeline

    filesystem_pipeline.TESTS_BUCKET_URL = TEST_SAMPLE_FILES

    getattr(filesystem_pipeline, example_name)()
