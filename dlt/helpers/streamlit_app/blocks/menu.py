import dlt
import streamlit as st

from dlt.helpers.streamlit_app.utils import HERE
from dlt.helpers.streamlit_app.widgets import mode_selector
from dlt.helpers.streamlit_app.widgets import pipeline_summary


def menu(pipeline: dlt.Pipeline) -> None:
    mode_selector()
    st.logo(
        "https://cdn.sanity.io/images/nsq559ov/production/7f85e56e715b847c5519848b7198db73f793448d-82x25.svg?q=75&fit=clip&auto=format",
        size="large",
    )
    st.page_link(f"{HERE}/pages/dashboard.py", label="Explore data", icon="🕹️")
    st.page_link(f"{HERE}/pages/load_info.py", label="Load info", icon="💾")
    pipeline_summary(pipeline)
