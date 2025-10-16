from dlt.common.exceptions import MissingDependencyException

# FIXME: Remove this after implementing package installer
try:
    import streamlit
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt Streamlit Helpers",
        ["streamlit"],
        "dlt Helpers for Streamlit should be run within a streamlit app.",
    )
