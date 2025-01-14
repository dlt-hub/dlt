import dlt
import streamlit as st

from dlt.helpers.streamlit_app.blocks.explorer import show_explorer
from dlt.helpers.streamlit_app.blocks.menu import menu
from dlt.helpers.streamlit_app.utils import render_with_pipeline


def show(pipeline: dlt.Pipeline) -> None:
    with st.sidebar:
        menu(pipeline)

    show_explorer(pipeline)


if __name__ == "__main__":
    render_with_pipeline(show)