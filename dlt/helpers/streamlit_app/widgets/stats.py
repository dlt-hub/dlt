from typing import Any, Optional
import streamlit as st


def stat(
    label: str,
    value: Any,
    width: Optional[str] = "100%",
    display: Optional[str] = "inline-block",
    background_color: Optional[str] = "#0e1111",
    border_radius: Optional[int] = 4,
    border_color: Optional[str] = "#272736",
    border_left_color: Optional[str] = "#007b05",
    border_left_width: Optional[int] = 0,
) -> None:
    stat_html = f"""
    <div class="stat">
        <p class="stat-label">{label}</p>
        <p class="stat-value">{value}</p>
    </div>
    """
    mode = st.session_state.get("color_mode", "dark")
    if mode == "light":
        background_color = "#FEFEFE"
        border_left_color = "#333333"

    styles = """
        .stat {
            display: %s;
            width: %s;
            border-radius: %dpx;
            border: 1px solid %s;
            background-color: %s;
            padding: 2%% 2%% 1%% 5%%;
            margin-bottom: 2%%;
        }
        .stat-label {
            font-size: 14px;
            margin-bottom: 5px;
        }
        .stat-value {
            font-size: 32px;
            margin-bottom: 0;
        }
        %s
        """ % (display, width, border_radius, border_color, background_color, "")

    if border_left_width > 1:
        styles += """
        .stat {
            border-left: %dpx solid %s !important;
        }
        """ % (border_left_width, border_left_color)

    st.markdown(
        stat_html + f"<style>{styles}</style>",
        unsafe_allow_html=True,
    )
