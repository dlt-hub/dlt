import dlt
import streamlit as st
import yaml

from dlt.common.libs.pandas import pandas as pd
from dlt.helpers.streamlit_app.widgets.tags import tag


def resource_state_info(
    pipeline: dlt.Pipeline,
    schema_name: str,
    resource_name: str,
) -> None:
    sources = pipeline.state.get("sources") or {}
    schema = sources.get(schema_name)
    if not schema:
        st.error(f"Schema with name: {schema_name} is not found")
        return

    resource = schema["resources"].get(resource_name)
    if not resource:
        return

    if "incremental" in resource:
        for incremental_name, incremental_spec in resource["incremental"].items():
            tag(incremental_name, label="Incremental")
            spec = yaml.safe_dump(incremental_spec)
            st.code(spec, language="yaml")
