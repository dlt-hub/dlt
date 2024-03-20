import dlt
import streamlit as st

from dlt.helpers.streamlit_app.utils import HERE
from dlt.helpers.streamlit_app.widgets import logo, mode_selector
from dlt.helpers.streamlit_app.widgets import pipeline_summary


def menu(pipeline: dlt.Pipeline) -> None:
    mode_selector()
    logo()
    st.page_link(f"{HERE}/pages/dashboard.py", label="Explore data", icon="🕹️")
    st.page_link(f"{HERE}/pages/load_info.py", label="Load info", icon="💾")
    pipeline_summary(pipeline)
