import dlt
import streamlit as st
import yaml

from dlt.common import json
from dlt.common.libs.pandas import pandas as pd
from dlt.common.pipeline import resource_state, TSourceState
from dlt.common.schema.utils import group_tables_by_resource
from dlt.helpers.streamlit_app.widgets.tags import tag


def resource_state_info(
    pipeline: dlt.Pipeline,
    schema_name: str,
    resource_name: str,
) -> None:
    sources_state = pipeline.state.get("sources") or {}
    schema = sources_state.get(schema_name)
    if not schema:
        st.error(f"Schema with name: {schema_name} is not found")
        return

    resource = schema["resources"].get(resource_name)
    with st.expander("Resource state", expanded=(resource is None)):
        if not resource:
            st.info(f"{resource_name} is missing resource state")
        else:
            spec = yaml.safe_dump(resource)
            st.code(spec, language="yaml")
