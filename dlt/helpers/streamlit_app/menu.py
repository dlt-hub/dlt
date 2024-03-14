import dlt
import streamlit as st

from dlt.common.destination.reference import WithStateSync
from dlt.helpers.streamlit_app.utils import HERE
from dlt.helpers.streamlit_app.widgets import logo, stat, tag, mode_selector
from dlt.helpers.streamlit_app.blocks.load_info import last_load_info
from dlt.pipeline.state_sync import load_pipeline_state_from_destination


def menu(pipeline: dlt.Pipeline) -> None:
    mode_selector()
    logo()
    st.page_link(f"{HERE}/dashboard.py", label="Explore data", icon="🕹️")
    st.page_link(f"{HERE}/pages/load_info.py", label="Load info", icon="💾")
    pipeline_state_info(pipeline)
    last_load_info(pipeline)


def pipeline_state_info(pipeline: dlt.Pipeline) -> None:
    st.divider()
    tag(pipeline.pipeline_name, label="Pipeline")
    tag(pipeline.destination.destination_name, label="Destination")

    remote_state = None
    with pipeline.destination_client() as client:
        if isinstance(client, WithStateSync):
            remote_state = load_pipeline_state_from_destination(pipeline.pipeline_name, client)

    local_state = pipeline.state

    if remote_state:
        remote_state_version = remote_state["_state_version"]
    else:
        remote_state_version = "---"  # type: ignore

    if remote_state_version != local_state["_state_version"]:
        st.text("")
        st.warning(
            "Looks like that local state is not yet synchronized or synchronization is disabled",
            icon="⚠️",
        )
