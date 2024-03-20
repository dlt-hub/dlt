from dlt.common.exceptions import MissingDependencyException


try:
    import streamlit
except ModuleNotFoundError:
    raise MissingDependencyException(
        "DLT Streamlit Helpers",
        ["streamlit"],
        "DLT Helpers for Streamlit should be run within a streamlit app.",
    )
