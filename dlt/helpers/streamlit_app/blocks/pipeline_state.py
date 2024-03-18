import dlt
import streamlit as st

from dlt.common.destination.reference import WithStateSync
from dlt.pipeline.state_sync import load_pipeline_state_from_destination


def pipeline_state_info(pipeline: dlt.Pipeline) -> None:
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
