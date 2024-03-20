import dlt
import streamlit as st

from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.destination.reference import WithStateSync
from dlt.helpers.streamlit_app.blocks.load_info import last_load_info
from dlt.helpers.streamlit_app.blocks.menu import menu
from dlt.helpers.streamlit_app.widgets import stat
from dlt.helpers.streamlit_app.utils import (
    query_data,
    query_data_live,
    render_with_pipeline,
)
from dlt.pipeline import Pipeline
from dlt.pipeline.exceptions import CannotRestorePipelineException
from dlt.pipeline.state_sync import load_pipeline_state_from_destination


def write_load_status_page(pipeline: Pipeline) -> None:
    """Display pipeline loading information."""

    try:
        loads_df = query_data_live(
            pipeline,
            f"SELECT load_id, inserted_at FROM {pipeline.default_schema.loads_table_name} WHERE"
            " status = 0 ORDER BY inserted_at DESC LIMIT 101 ",
        )

        if loads_df is not None:
            selected_load_id = st.selectbox("Select load id", loads_df)
            schema = pipeline.default_schema

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
            rows_counts_df = query_data(pipeline, "\n".join(query_parts))

            st.markdown(f"Rows loaded in **{selected_load_id}**")
            st.dataframe(rows_counts_df)

            st.markdown("**Last 100 loads**")
            st.dataframe(loads_df)

            st.subheader("Schema updates", divider=True)
            schemas_df = query_data_live(
                pipeline,
                "SELECT schema_name, inserted_at, version, version_hash FROM"
                f" {pipeline.default_schema.version_table_name} ORDER BY inserted_at DESC LIMIT"
                " 101 ",
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

    if remote_state_version != str(local_state["_state_version"]):
        st.text("")
        st.warning(
            "Looks like that local state is not yet synchronized or synchronization is disabled",
            icon="⚠️",
        )


def show(pipeline: dlt.Pipeline) -> None:
    st.subheader("Load info", divider="rainbow")
    last_load_info(pipeline)
    write_load_status_page(pipeline)
    show_state_versions(pipeline)

    with st.sidebar:
        menu(pipeline)


if __name__ == "__main__":
    render_with_pipeline(show)
