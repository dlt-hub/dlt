from typing import Union
import streamlit as st
import yaml

import dlt
from dlt.common.pendulum import pendulum


def date_to_iso(
    dumper: yaml.SafeDumper, data: Union[pendulum.Date, pendulum.DateTime]
) -> yaml.ScalarNode:
    return dumper.represent_datetime(data)  # type: ignore[arg-type]


yaml.representer.SafeRepresenter.add_representer(pendulum.Date, date_to_iso)  # type: ignore[arg-type]
yaml.representer.SafeRepresenter.add_representer(pendulum.DateTime, date_to_iso)  # type: ignore[arg-type]


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
