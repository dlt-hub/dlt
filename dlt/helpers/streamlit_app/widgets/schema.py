import dlt
import streamlit as st
from typing_extensions import Callable, TypeAlias


def schema_picker(pipeline: dlt.Pipeline) -> None:
    schema = None
    num_schemas = len(pipeline.schema_names)
    if num_schemas == 1:
        schema_name = pipeline.schema_names[0]
        schema = pipeline.schemas.get(schema_name)
        st.text(f"Schema: {schema_name}")
    elif num_schemas > 1:
        text = "Select schema"
        selected_schema_name = st.selectbox(
            text,
            sorted(pipeline.schema_names),
        )
        schema = pipeline.schemas.get(selected_schema_name)

    if schema:
        st.session_state["schema"] = schema
