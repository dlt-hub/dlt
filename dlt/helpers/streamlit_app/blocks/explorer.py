import dlt
import pygwalker
import streamlit as st
from pygwalker.api.streamlit import StreamlitRenderer

st.set_page_config(layout="wide")





@st.cache_resource
def pygwalker_renderer(pipeline_name: str, table_name: str) -> StreamlitRenderer:
    pipeline = dlt.attach(pipeline_name)
    dataset = pipeline.dataset()
    df = dataset[table_name].df()
    return StreamlitRenderer(
        df,
        kernel_computation=True,  # use duckdb under the hood
        default_tab="vis"
    )


def select_table_name(pipeline: dlt.Pipeline) -> str:
    current_schema = pipeline.default_schema
    if schema_name := st.session_state.get("schema_name"):
        current_schema = pipeline.schemas[schema_name]

    selected_table = st.selectbox(
        label="Active table",
        options=current_schema.data_tables(),
        format_func=lambda t: t["name"]
    )
    return selected_table["name"]


def show_explorer(pipeline: dlt.Pipeline) -> None:
    selected_table_name = select_table_name(pipeline)
    renderer = pygwalker_renderer(
        pipeline_name=pipeline.pipeline_name,
        table_name=selected_table_name,
    )
    renderer.explorer()
