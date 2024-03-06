import dlt
import streamlit as st

from dlt.common.destination.reference import WithStateSync
from dlt.helpers.streamlit_app.utils import HERE
from dlt.helpers.streamlit_app.widgets import logo, stat, tag
from dlt.helpers.streamlit_app.last_load_info import last_load_info
from dlt.pipeline.state_sync import load_state_from_destination


def menu(pipeline: dlt.Pipeline) -> None:
    logo()
    st.page_link(f"{HERE}/dashboard.py", label="Explore data", icon="🕹️")
    st.page_link(f"{HERE}/pages/load_info.py", label="Load info", icon="💾")
    pipeline_state_info(pipeline)
    last_load_info(pipeline)


def pipeline_state_info(pipeline: dlt.Pipeline) -> None:
    st.divider()
    tag(pipeline.destination.destination_name, label="Destination")
    st.subheader(f"Pipeline {pipeline.pipeline_name}", divider="rainbow")
    st.subheader("State info")
    remote_state = None
    with pipeline.destination_client() as client:
        if isinstance(client, WithStateSync):
            remote_state = load_state_from_destination(pipeline.pipeline_name, client)

    local_state = pipeline.state

    if remote_state:
        remote_state_version = remote_state["_state_version"]
    else:
        remote_state_version = "---"  # type: ignore

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

    if remote_state_version != local_state["_state_version"]:
        st.warning(
            "Looks like that local state is not yet synchronized or synchronization is disabled",
            icon="⚠️",
        )
