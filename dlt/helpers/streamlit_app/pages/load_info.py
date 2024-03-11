import dlt
import streamlit as st

from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.libs.pandas import pandas as pd
from dlt.helpers.streamlit_app.menu import menu
from dlt.helpers.streamlit_app.blocks.schema_tabs import show_schema_tabs
from dlt.helpers.streamlit_app.widgets import pipeline_summary
from dlt.pipeline import Pipeline
from dlt.pipeline.exceptions import CannotRestorePipelineException, SqlClientNotAvailable

# use right caching function to disable deprecation message
if hasattr(st, "cache_data"):
    cache_data = st.cache_data
else:
    cache_data = st.experimental_memo


def write_load_status_page(pipeline: Pipeline) -> None:
    """Display pipeline loading information. Will be moved to dlt package once tested"""

    @cache_data(ttl=600)
    def _query_data(query: str, schema_name: str = None) -> pd.DataFrame:
        try:
            with pipeline.sql_client(schema_name) as client:
                with client.execute_query(query) as curr:
                    return curr.df()
        except SqlClientNotAvailable:
            st.error("Cannot load data - SqlClient not available")

    @cache_data(ttl=5)
    def _query_data_live(query: str, schema_name: str = None) -> pd.DataFrame:
        try:
            with pipeline.sql_client(schema_name) as client:
                with client.execute_query(query) as curr:
                    return curr.df()
        except SqlClientNotAvailable:
            st.error("Cannot load data - SqlClient not available")

    try:
        pipeline_summary(pipeline)

        loads_df = _query_data_live(
            f"SELECT load_id, inserted_at FROM {pipeline.default_schema.loads_table_name} WHERE"
            " status = 0 ORDER BY inserted_at DESC LIMIT 101 "
        )
        st.json(st.session_state)
        if loads_df is not None:
            selected_load_id = st.selectbox("Select load id", loads_df)
            schema = pipeline.default_schema

            # st.json(st.session_state["schema"].data_tables(), expanded=False)
            st.markdown("**Number of loaded rows:**")

            # construct a union query
            query_parts = []
            for table in schema.data_tables():
                if "parent" in table:
                    continue
                table_name = table["name"]
                query_parts.append(
                    f"SELECT '{table_name}' as table_name, COUNT(1) As rows_count FROM"
                    f" {table_name} WHERE _dlt_load_id = '{selected_load_id}'"
                )
                query_parts.append("UNION ALL")

            query_parts.pop()
            rows_counts_df = _query_data("\n".join(query_parts))

            st.markdown(f"Rows loaded in **{selected_load_id}**")
            st.dataframe(rows_counts_df)

            st.markdown("**Last 100 loads**")
            st.dataframe(loads_df)

            st.subheader("Schema updates")
            schemas_df = _query_data_live(
                "SELECT schema_name, inserted_at, version, version_hash FROM"
                f" {pipeline.default_schema.version_table_name} ORDER BY inserted_at DESC LIMIT"
                " 101 "
            )
            st.markdown("**100 recent schema updates**")
            st.dataframe(schemas_df)
    except CannotRestorePipelineException as restore_ex:
        st.error("Seems like the pipeline does not exist. Did you run it at least once?")
        st.exception(restore_ex)

    except ConfigFieldMissingException as cf_ex:
        st.error(
            "Pipeline credentials/configuration is missing. This most often happen when you run the"
            " streamlit app from different folder than the `.dlt` with `toml` files resides."
        )
        st.text(str(cf_ex))

    except Exception as ex:
        st.error("Pipeline info could not be prepared. Did you load the data at least once?")
        st.exception(ex)


def show() -> None:
    if not st.session_state.get("pipeline_name"):
        st.switch_page("dashboard.py")

    pipeline = dlt.attach(st.session_state["pipeline_name"])
    st.subheader("Load info", divider="rainbow")
    write_load_status_page(pipeline)
    with st.sidebar:
        menu(pipeline)


if __name__ == "__main__":
    show()
