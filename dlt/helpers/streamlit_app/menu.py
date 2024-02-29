import base64
import dlt
import streamlit as st

from PIL import Image
from dlt.common.destination.reference import WithStateSync
from dlt.helpers.streamlit_app.utils import HERE
from dlt.pipeline.state_sync import load_state_from_destination
from streamlit_extras.metric_cards import style_metric_cards

LOGO = HERE / "logo.png"


@st.cache_data()
def b64_logo():
    with open(LOGO, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()


def logo():
    bin_str = b64_logo()
    page_bg_img = """<img class="logo" src="data:image/png;base64,{bin}"/>""".format(bin=bin_str)
    styles = """
    <style>
        .logo {
            margin-top: -100px;
            margin-left: 50px;
            width: 60%;
        }
        </style>
    """

    st.markdown(page_bg_img + styles, unsafe_allow_html=True)


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
            icon="⚠️"
        )
