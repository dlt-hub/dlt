import streamlit as st


def logo() -> None:
    logo_text = """
    <div class="logo">
        <span class="dlt">dlt</span>
        <span class="hub">Hub</span>
    </div>
    """
    styles = """
    <style>
        .logo {
            margin-top: -120px;
            margin-left: 36%;
            margin-bottom: 0;
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
