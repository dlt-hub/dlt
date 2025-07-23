import dlt
import streamlit as st

from dlt.helpers.streamlit_app.blocks.query import maybe_run_query
from dlt.helpers.streamlit_app.blocks.table_hints import list_table_hints
from dlt.helpers.streamlit_app.blocks.menu import menu
from dlt.helpers.streamlit_app.utils import render_with_pipeline
from dlt.helpers.streamlit_app.widgets import schema_picker
from dlt.pipeline import Pipeline


def write_data_explorer_page(
    pipeline: Pipeline,
    schema_name: str = None,
    example_query: str = "",
    show_charts: bool = True,
) -> None:
    """Writes Streamlit app page with a schema and live data preview.

    Args:
        pipeline (Pipeline): Pipeline instance to use.
        schema_name (str, optional): Name of the schema to display. If None, default schema is used.
        example_query (str, optional): Example query to be displayed in the SQL Query box.
        show_charts (bool, optional): Should automatically show charts for the queries from SQL Query box. Defaults to True.

    Raises:
        MissingDependencyException: Raised when a particular python dependency is not installed
    """

    st.subheader("Schemas and tables", divider="rainbow")
    schema_picker(pipeline)
    if schema_name := st.session_state.get("schema_name"):
        schema = pipeline.schemas.get(schema_name)
        tables = sorted(
            schema.data_tables(),
            key=lambda table: table["name"],
        )

        list_table_hints(pipeline, tables)
    else:
        st.warning("No schemas found")

    maybe_run_query(
        pipeline,
        show_charts=show_charts,
        example_query=example_query,
    )


def show(pipeline: dlt.Pipeline) -> None:
    with st.sidebar:
        menu(pipeline)

    write_data_explorer_page(pipeline)


if __name__ == "__main__":
    render_with_pipeline(show)
