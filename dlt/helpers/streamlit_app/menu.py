import base64
import dlt
import streamlit as st

from PIL import Image
from dlt.common.destination.reference import WithStateSync
from dlt.helpers.streamlit_app.utils import HERE
from dlt.pipeline.state_sync import load_state_from_destination
from streamlit_extras.metric_cards import style_metric_cards


def logo():
    logo_text = """
    <div class="logo">
        <span class="dlt">dlt</span>
        <span class="hub">Hub</span>
    </div>
    """
    styles = """
    <style>
        .logo {
            margin-top: -80px;
            margin-left: 30%;
            width: 60%;
            font-size: 2em;
            letter-spacing: -1.8px;
        }

        .dlt {
            position: relative;
            color: #58c1d5;
        }
        .dlt:after {
            position: absolute;
            bottom: 9px;
            right: -3px;
            content: " ";
            width: 3px;
            height: 3px;
            border-radius: 1px;
            border-top-left-radius: 2px;
            border-bottom-right-radius: 2px;
            border: 0;
            background: #c4d200;
        }

        .hub {
            color: #c4d200;
        }
        </style>
    """

    st.markdown(logo_text + styles, unsafe_allow_html=True)


def menu() -> None:
    logo()
    st.page_link(f"{HERE}/dashboard.py", label="Explore data", icon="🕹️")
    st.page_link(f"{HERE}/pages/load_info.py", label="Load info", icon="💾")


def pipeline_state_info(pipeline: dlt.Pipeline):
    st.divider()
    st.subheader("Pipeline state info")
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

    col1.metric(label="Local version", value=local_state["_state_version"])
    col2.metric(label="Remote version", value=remote_state_version)
    style_metric_cards(
        background_color="#0e1111",
        border_size_px=1,
        border_color="#272736",
        border_left_color="#007b05",
        border_radius_px=4,
    )

    if remote_state_version != local_state["_state_version"]:
        st.warning(
            "Looks like that local state is not yet synchronized or synchronization is disabled",
            icon="⚠️",
        )
