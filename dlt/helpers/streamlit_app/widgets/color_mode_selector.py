import streamlit as st

from typing_extensions import Callable, Literal

from dlt.helpers.streamlit_app.theme import dark_theme, light_theme

ColorMode = Literal["light", "dark"]


def set_color_mode(mode: ColorMode) -> Callable[..., None]:
    def set_mode() -> None:
        st.session_state["color_mode"] = mode
        if mode and mode == "dark":
            dark_theme()
        else:
            light_theme()

    return set_mode


def mode_selector() -> None:
    columns = st.columns(10)
    light = columns[3]
    dark = columns[5]

    # Set default theme to light if it wasn't set before
    if not st.session_state.get("color_mode"):
        st.session_state["color_mode"] = "light"
        st.config.set_option("theme.base", "light")

    with light:
        st.button("☀️", on_click=set_color_mode("light"))
    with dark:
        st.button("🌚", on_click=set_color_mode("dark"))
