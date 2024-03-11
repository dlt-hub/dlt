import dlt
import streamlit as st


def show_schema_tabs(schema: dlt.Schema) -> None:
    schema_map = {sc["name"]: sc for sc in schema.data_tables()}
    schemas = schema_map.keys()
    tabs = zip(schemas, st.tabs(schemas))
    for schema_name, tab in tabs:
        with tab:
            st.caption(schema_name)
