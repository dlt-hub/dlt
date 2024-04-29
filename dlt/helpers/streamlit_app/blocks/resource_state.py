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
    schema = sources_state.get(schema_name, {})
    resource = schema.get("resources", {}).get(resource_name)
    if not resource:
        return

    with st.expander("Resource state", expanded=False):
        spec = yaml.safe_dump(resource)
        st.code(spec, language="yaml")
