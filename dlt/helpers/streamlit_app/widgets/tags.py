from typing import Optional, Literal

import streamlit as st

TagType = Literal["info", "success", "warning", "error", "mute"]


def tag(
    tag_name: str,
    label: Optional[str] = None,
    border_radius: Optional[int] = 4,
    bold: Optional[bool] = False,
    tag_type: Optional[TagType] = "mute",
) -> None:
    tag_html = f"""
    {str(label)+": " if label else ""}<span class="tag">{tag_name}</span>
    """
    kinds = {
        "mute": {"text_color": "#495057", "bg_color": "#e9ecef"},
        "info": {"text_color": "#1864ab", "bg_color": "#4dabf7"},
        "success": {"text_color": "#2b8a3e", "bg_color": "#8ce99a"},
        "warning": {"text_color": "#d9480f", "bg_color": "#ffa94d"},
        "error": {"text_color": "#c92a2a", "bg_color": "#ffe3e3"},
    }
    kind = kinds[tag_type]
    bg_color = kind["bg_color"]
    text_color = kind["text_color"]

    styles = """
    <style>
    .tag {
        border-radius: %dpx;
        background: %s;
        padding: 1px 4px;
        color: %s;
        font-weight: %s;
    }
    </style>
    """ % (border_radius, bg_color, text_color, "600" if bold else "normal")

    st.markdown(tag_html + styles, unsafe_allow_html=True)
