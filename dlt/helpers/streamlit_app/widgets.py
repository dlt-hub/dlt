import dlt
import streamlit as st


def pipeline_summary(pipeline: dlt.Pipeline):
    credentials = pipeline.sql_client().credentials
    schema_names = ", ".join(sorted(pipeline.schema_names))
    expander = st.expander("Pipeline info")
    expander.markdown(
        f"""
        * pipeline name: **{pipeline.pipeline_name}**
        * destination: **{str(credentials)}** in **{pipeline.destination.destination_description}**
        * dataset name: **{pipeline.dataset_name}**
        * default schema name: **{pipeline.default_schema_name}**
        * all schema names: **{schema_names}**
        """
    )
