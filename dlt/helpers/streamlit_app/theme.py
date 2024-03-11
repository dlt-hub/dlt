import streamlit as st


def dark_theme() -> None:
    st.config.set_option("theme.base", "dark")
    st.config.set_option("theme.primaryColor", "#191937")

    # Main background
    st.config.set_option("theme.backgroundColor", "#4C4898")

    # Sidebar
    st.config.set_option("theme.secondaryBackgroundColor", "#191937")

    # Text
    st.config.set_option("theme.textColor", "#FEFEFA")


def light_theme() -> None:
    st.config.set_option("theme.base", "light")
    st.config.set_option("theme.primaryColor", "#333")

    # Main background
    st.config.set_option("theme.backgroundColor", "#FEFEFE")

    # Sidebar
    st.config.set_option("theme.secondaryBackgroundColor", "#ededed")

    # Text
    st.config.set_option("theme.textColor", "#333")
