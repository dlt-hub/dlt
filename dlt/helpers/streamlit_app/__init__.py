from dlt.common.exceptions import MissingDependencyException


try:
    import streamlit    # type: ignore

    # from streamlit import SECRETS_FILE_LOC, secrets
except ModuleNotFoundError:
    raise MissingDependencyException(
        "DLT Streamlit Helpers",
        ["streamlit"],
        "DLT Helpers for Streamlit should be run within a streamlit app.",
    )
