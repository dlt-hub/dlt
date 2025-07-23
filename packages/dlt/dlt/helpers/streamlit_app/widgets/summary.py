import dlt
import streamlit as st
from dlt.common.destination.exceptions import SqlClientNotAvailable


def pipeline_summary(pipeline: dlt.Pipeline) -> None:
    try:
        credentials = pipeline.sql_client().credentials
    except SqlClientNotAvailable:
        credentials = "---"
        st.error("🚨 Cannot load data - SqlClient not available")

    schema_names = ", ".join(sorted(pipeline.schema_names))
    st.subheader("Pipeline info", divider=True)
    st.markdown(f"""
        * pipeline name: **{pipeline.pipeline_name}**
        * destination: **{str(credentials)}** in **{pipeline.destination.destination_description}**
        * dataset name: **{pipeline.dataset_name}**
        * default schema name: **{pipeline.default_schema_name}**
        * all schema names: **{schema_names}**
        """)
