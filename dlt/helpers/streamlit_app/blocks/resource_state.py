import dlt
import streamlit as st


def resource_state_info(pipeline: dlt.Pipeline, schema_name: str, resource_name: str) -> None:
    schema = pipeline.state["sources"].get(schema_name)
    if not schema:
        st.error(f"Schema with name: {schema_name} is not found")
        return

    resource = schema["resources"].get(resource_name)
    st.text(schema["resources"])
    if not resource:
        st.error(f"Resource with name: {resource_name} is not found")
        return

    st.json(resource)
