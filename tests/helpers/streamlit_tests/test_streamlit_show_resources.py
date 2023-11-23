"""
Create a pipeline with multiple resources for streamlit to show.

Run streamlit showing this pipeline like this:

    dlt pipeline test_resources_pipeline show
"""

import dlt


@dlt.source
def source1(nr):
    def get_resource(nr):
        for i in range(nr):
            yield {"id": i, "column_1": f"abc_{i}"}

    resource = dlt.resource(
        get_resource(nr),
        name="One",
        write_disposition="merge",
        primary_key="column_1",
        merge_key=["column_1"],
    )
    yield resource


@dlt.source
def source2(nr):
    def get_resource2(nr):
        for i in range(nr):
            yield {"id": i, "column_2": f"xyz_{i}"}

    def get_resource3(nr):
        for i in range(nr):
            yield {"id": i, "column_3": f"pqr_{i}", "column_4": f"pqrr_{i}"}

    yield dlt.resource(
        get_resource2(nr),
        name="Two",
        write_disposition="merge",
        primary_key="column_2",
        merge_key=["column_2"],
    )
    yield dlt.resource(
        get_resource3(nr),
        name="Three",
        write_disposition="merge",
        primary_key=["column_3", "column_4"],
        merge_key=["column_3"],
    )


def test_multiple_resources_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="test_resources_pipeline", destination="duckdb", dataset_name="rows_data2"
    )
    load_info = pipeline.run([source1(10), source2(20)])

    source1_schema = load_info.pipeline.schemas.get("source1")

    assert load_info.pipeline.schema_names == ["source2", "source1"]  # type: ignore[attr-defined]

    assert source1_schema.data_tables()[0]["name"] == "one"
    assert source1_schema.data_tables()[0]["columns"]["column_1"].get("primary_key") is True
    assert source1_schema.data_tables()[0]["columns"]["column_1"].get("merge_key") is True
    assert source1_schema.data_tables()[0]["write_disposition"] == "merge"

    # The rest should be inspected using the streamlit tool.
