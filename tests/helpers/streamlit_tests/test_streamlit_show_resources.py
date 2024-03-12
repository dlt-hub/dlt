"""
Create a pipeline with multiple resources for streamlit to show.

Run streamlit showing this pipeline like this:

    dlt pipeline test_resources_pipeline show
"""
import os
from pathlib import Path

import dlt

from streamlit.testing.v1 import AppTest  # type: ignore

here = Path(__file__).parent
dlt_root = here.parent.parent.parent.absolute()
streamlit_app_path = dlt_root / "dlt/helpers/streamlit_app"


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


@dlt.source()
def source2(nr):
    def get_resource2(nr):
        for i in range(nr):
            yield {"id": i, "column_2": f"xyz_{i}"}

    @dlt.resource(
        name="Three",
        write_disposition="merge",
        primary_key=["column_3", "column_4"],
        merge_key=["column_3"],
    )
    def get_resource3(
        nr,
        id_inc: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id",
            initial_value=0,
        ),
    ):
        for i in range(nr):
            yield {"id": i, "column_3": f"pqr_{i}", "column_4": f"pqrr_{i}"}

    yield dlt.resource(
        get_resource2(nr),
        name="Two",
        write_disposition="merge",
        primary_key="column_2",
        merge_key=["column_2"],
    )
    yield get_resource3(nr)


def test_multiple_resources_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="test_resources_pipeline",
        destination="duckdb",
        dataset_name="rows_data2",
    )
    load_info = pipeline.run([source1(10), source2(20)])

    source1_schema = load_info.pipeline.schemas.get("source1")

    assert set(load_info.pipeline.schema_names) == set(["source2", "source1"])  # type: ignore[attr-defined]

    assert source1_schema.data_tables()[0]["name"] == "one"
    assert source1_schema.data_tables()[0]["columns"]["column_1"].get("primary_key") is True
    assert source1_schema.data_tables()[0]["columns"]["column_1"].get("merge_key") is True
    assert source1_schema.data_tables()[0]["write_disposition"] == "merge"

    os.environ["DLT_TEST_PIPELINE_NAME"] = "test_resources_pipeline"
    streamlit_app = AppTest.from_file(str(streamlit_app_path / "dashboard.py"), default_timeout=5)
    streamlit_app.run()
    assert not streamlit_app.exception

    # Check color mode switching updates session stats
    streamlit_app.sidebar.button[0].click().run()
    assert not streamlit_app.exception
    streamlit_app.session_state["color_mode"] == "light"

    streamlit_app.sidebar.button[1].click().run()
    assert not streamlit_app.exception
    streamlit_app.session_state["color_mode"] == "dark"

    # Check page links in sidebar
    assert "Explore data" in streamlit_app.sidebar[2].label
    assert "Load info" in streamlit_app.sidebar[3].label

    # Check Explore data page
    assert streamlit_app.subheader[0].value == "Schemas and tables"
    assert streamlit_app.subheader[1].value == "Schema: source1"
    assert streamlit_app.subheader[2].value == "Table: one"
    assert streamlit_app.subheader[3].value == "Run your query"
    assert streamlit_app.subheader[4].value == f"Pipeline {pipeline.pipeline_name}"
    assert streamlit_app.subheader[5].value == "State info"
    assert streamlit_app.subheader[6].value == "Last load info"
    assert 3 < len(streamlit_app.subheader) < 10
