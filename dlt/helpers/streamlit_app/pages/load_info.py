import os
import sys

import dlt
import streamlit as st

from dlt.cli import echo as fmt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.destination.reference import WithStateSync
from dlt.common.libs.pandas import pandas as pd
from dlt.helpers.streamlit_app.blocks.menu import menu
from dlt.helpers.streamlit_app.widgets import stat, pipeline_summary
from dlt.helpers.streamlit_app.utils import attach_to_pipeline, cache_data
from dlt.pipeline import Pipeline
from dlt.pipeline.exceptions import CannotRestorePipelineException, SqlClientNotAvailable
from dlt.pipeline.state_sync import load_pipeline_state_from_destination


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

            st.subheader("Schema updates", divider=True)
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


def show_state_versions(pipeline: dlt.Pipeline) -> None:
    st.subheader("State info", divider=True)
    remote_state = None
    with pipeline.destination_client() as client:
        if isinstance(client, WithStateSync):
            remote_state = load_pipeline_state_from_destination(pipeline.pipeline_name, client)

    local_state = pipeline.state

    remote_state_version = "---"
    if remote_state:
        remote_state_version = str(remote_state["_state_version"])

    col1, col2 = st.columns(2)
    with col1:
        stat(
            label="Local version",
            value=local_state["_state_version"],
            display="block",
            border_left_width=4,
        )

    with col2:
        stat(
            label="Remote version",
            value=remote_state_version,
            display="block",
            border_left_width=4,
        )


def show(pipeline_name: str) -> None:
    pipeline = attach_to_pipeline(pipeline_name)

    st.subheader("Load info", divider="rainbow")
    write_load_status_page(pipeline)
    show_state_versions(pipeline)

    with st.sidebar:
        menu(pipeline)


if __name__ == "__main__":
    if test_pipeline_name := os.getenv("DLT_TEST_PIPELINE_NAME"):
        fmt.echo(f"RUNNING TEST PIPELINE: {test_pipeline_name}")
        show(test_pipeline_name)
    else:
        show(sys.argv[1])
