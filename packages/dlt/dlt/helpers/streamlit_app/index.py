import streamlit as st

from dlt.helpers.streamlit_app.utils import HERE

if __name__ == "__main__":
    st.switch_page(f"{HERE}/pages/dashboard.py")
