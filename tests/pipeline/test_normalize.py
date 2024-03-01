import pytest
import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.pipeline.utils import load_table_counts


def test_normalize_retry() -> None:
    # Create a new pipeline
    pipe = dlt.pipeline(pipeline_name="pipe", destination="duckdb", full_refresh=True)

    # run once
    pipe.run(
        [{"id": 1}],
        table_name="items",
        schema_contract={"columns": "freeze"},
    )

    # extract data that violated ths contract
    pipe.extract(
        [{"id": 1, "new_col": 6}],
        table_name="items",
        schema_contract={"columns": "freeze"},
    )
    assert load_table_counts(pipe, "items")["items"] == 1

    # normalize fails, violation of frozen columns
    with pytest.raises(PipelineStepFailed):
        pipe.normalize()

    # remove the schema contract from the table
    normalize_storage = pipe._get_normalize_storage()
    pending_load_id = normalize_storage.extracted_packages.list_packages()[0]
    schema = normalize_storage.extracted_packages.load_schema(pending_load_id)
    schema.tables["items"]["schema_contract"] = {}
    schema = normalize_storage.extracted_packages.save_schema(pending_load_id, schema)  # type: ignore

    # now the normalization should pass
    pipe.normalize()
    pipe.load()
    assert load_table_counts(pipe, "items")["items"] == 2
