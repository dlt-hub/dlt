import sys


import dlt
import streamlit as st

from dlt.helpers.streamlit_app.blocks.query import maybe_run_query
from dlt.helpers.streamlit_app.blocks.table_hints import list_table_hints
from dlt.helpers.streamlit_app.menu import menu
from dlt.helpers.streamlit_app.widgets import schema_picker
from dlt.pipeline import Pipeline

# use right caching function to disable deprecation message
if hasattr(st, "cache_data"):
    cache_data = st.cache_data
else:
    cache_data = st.experimental_memo


def write_data_explorer_page(
    pipeline: Pipeline,
    schema_name: str = None,
    example_query: str = "",
    show_charts: bool = True,
) -> None:
    """Writes Streamlit app page with a schema and live data preview.

    #### Args:
        pipeline (Pipeline): Pipeline instance to use.
        schema_name (str, optional): Name of the schema to display. If None, default schema is used.
        example_query (str, optional): Example query to be displayed in the SQL Query box.
        show_charts (bool, optional): Should automatically show charts for the queries from SQL Query box. Defaults to True.

    Raises:
        MissingDependencyException: Raised when a particular python dependency is not installed
    """

    st.subheader("Schemas and tables", divider="rainbow")
    schema_picker(pipeline)

    tables = sorted(
        st.session_state["schema"].data_tables(),
        key=lambda table: table["name"],
    )

    list_table_hints(pipeline, tables)
    maybe_run_query(
        pipeline,
        show_charts=show_charts,
        example_query=example_query,
    )


def display(pipeline_name: str) -> None:
    pipeline = dlt.attach(pipeline_name)
    st.session_state["pipeline_name"] = pipeline_name
    with st.sidebar:
        menu(pipeline)

    write_data_explorer_page(pipeline)


if __name__ == "__main__":
    display(sys.argv[1])
