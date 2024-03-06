import sys

from typing import List, Iterator

import streamlit as st

from dlt.common.exceptions import MissingDependencyException
from dlt.common.utils import flatten_list_or_items
from dlt.common.libs.pandas import pandas as pd
from dlt.helpers.streamlit_app.menu import menu
from dlt.helpers.streamlit_app.widgets import tag
from dlt.pipeline import Pipeline
from dlt.pipeline.exceptions import SqlClientNotAvailable


# use right caching function to disable deprecation message
if hasattr(st, "cache_data"):
    cache_data = st.cache_data
else:
    cache_data = st.experimental_memo


def write_data_explorer_page(
    pipeline: Pipeline,
    schema_name: str = None,
    show_dlt_tables: bool = False,
    example_query: str = "",
    show_charts: bool = True,
) -> None:
    """Writes Streamlit app page with a schema and live data preview.

    #### Args:
        pipeline (Pipeline): Pipeline instance to use.
        schema_name (str, optional): Name of the schema to display. If None, default schema is used.
        show_dlt_tables (bool, optional): Should show dlt internal tables. Defaults to False.
        example_query (str, optional): Example query to be displayed in the SQL Query box.
        show_charts (bool, optional): Should automatically show charts for the queries from SQL Query box. Defaults to True.

    Raises:
        MissingDependencyException: Raised when a particular python dependency is not installed
    """

    @cache_data(ttl=60)
    def _query_data(query: str, chunk_size: int = None) -> pd.DataFrame:
        try:
            with pipeline.sql_client(schema_name) as client:
                with client.execute_query(query) as curr:
                    return curr.df(chunk_size=chunk_size)
        except SqlClientNotAvailable:
            st.error("Cannot load data - SqlClient not available")

    st.subheader("Schemas and tables", divider="rainbow")

    num_schemas = len(pipeline.schema_names)
    if num_schemas == 1:
        schema_name = pipeline.schema_names[0]
        selected_schema = pipeline.schemas.get(schema_name)
        st.text(f"Schema: {schema_name}")
    elif num_schemas > 1:
        st.subheader("Schema:")
        text = "Pick a schema name to see all its tables below"
        selected_schema_name = st.selectbox(text, sorted(pipeline.schema_names))
        selected_schema = pipeline.schemas.get(selected_schema_name)

    for table in sorted(selected_schema.data_tables(), key=lambda table: table["name"]):
        table_name = table["name"]
        tag(table_name, label="Table")
        if "description" in table:
            st.text(table["description"])
        table_hints: List[str] = []
        if "parent" in table:
            table_hints.append("parent: **%s**" % table["parent"])
        if "resource" in table:
            table_hints.append("resource: **%s**" % table["resource"])
        if "write_disposition" in table:
            table_hints.append("write disposition: **%s**" % table["write_disposition"])
        columns = table["columns"]
        primary_keys: Iterator[str] = flatten_list_or_items(
            [
                col_name
                for col_name in columns.keys()
                if not col_name.startswith("_") and columns[col_name].get("primary_key") is not None
            ]
        )
        table_hints.append("primary key(s): **%s**" % ", ".join(primary_keys))
        merge_keys = flatten_list_or_items(
            [
                col_name
                for col_name in columns.keys()
                if not col_name.startswith("_")
                and not columns[col_name].get("merge_key") is None  # noqa: E714
            ]
        )
        table_hints.append("merge key(s): **%s**" % ", ".join(merge_keys))

        st.markdown(" | ".join(table_hints))

        # table schema contains various hints (like clustering or partition options) that we do not want to show in basic view
        def essentials_f(c):
            return {k: v for k, v in c.items() if k in ["name", "data_type", "nullable"]}

        st.table(map(essentials_f, table["columns"].values()))
        # add a button that when pressed will show the full content of a table
        if st.button("SHOW DATA", key=table_name):
            df = _query_data(f"SELECT * FROM {table_name}", chunk_size=2048)
            if df is None:
                st.text("No rows returned")
            else:
                rows_count = df.shape[0]
                if df.shape[0] < 2048:
                    st.text(f"All {rows_count} row(s)")
                else:
                    st.text(f"Top {rows_count} row(s)")
                st.dataframe(df)

    st.header("Run your query")
    sql_query = st.text_area("Enter your SQL query", value=example_query)
    if st.button("Run Query"):
        if sql_query:
            try:
                # run the query from the text area
                df = _query_data(sql_query)
                if df is None:
                    st.text("No rows returned")
                else:
                    rows_count = df.shape[0]
                    st.text(f"{rows_count} row(s) returned")
                    st.dataframe(df)
                    try:
                        # now if the dataset has supported shape try to display the bar or altair chart
                        if df.dtypes.shape[0] == 1 and show_charts:
                            # try barchart
                            st.bar_chart(df)
                        if df.dtypes.shape[0] == 2 and show_charts:
                            # try to import altair charts
                            try:
                                import altair as alt
                            except ModuleNotFoundError:
                                raise MissingDependencyException(
                                    "DLT Streamlit Helpers",
                                    ["altair"],
                                    "DLT Helpers for Streamlit should be run within a streamlit"
                                    " app.",
                                )

                            # try altair
                            bar_chart = (
                                alt.Chart(df)
                                .mark_bar()
                                .encode(
                                    x=f"{df.columns[1]}:Q", y=alt.Y(f"{df.columns[0]}:N", sort="-x")
                                )
                            )
                            st.altair_chart(bar_chart, use_container_width=True)
                    except Exception as ex:
                        st.error(f"Chart failed due to: {ex}")
            except Exception as ex:
                st.text("Exception when running query")
                st.exception(ex)


def display(pipeline_name: str) -> None:
    import dlt

    pipeline = dlt.attach(pipeline_name)
    st.session_state["pipeline_name"] = pipeline_name
    with st.sidebar:
        menu(pipeline)

    write_data_explorer_page(pipeline)


if __name__ == "__main__":
    display(sys.argv[1])
