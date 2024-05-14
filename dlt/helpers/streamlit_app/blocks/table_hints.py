from typing import Any, Dict, List

import dlt
import streamlit as st

from dlt.common.schema.typing import TTableSchema
from dlt.common.utils import flatten_list_or_items
from dlt.helpers.streamlit_app.blocks.resource_state import resource_state_info
from dlt.helpers.streamlit_app.blocks.show_data import show_data_button


def list_table_hints(pipeline: dlt.Pipeline, tables: List[TTableSchema]) -> None:
    current_schema = pipeline.default_schema
    if schema_name := st.session_state.get("schema_name"):
        current_schema = pipeline.schemas[schema_name]

    for table in tables:
        table_hints: List[str] = []
        if "parent" in table:
            table_hints.append("parent: **%s**" % table["parent"])

        if "resource" in table:
            table_hints.append("resource: **%s**" % table["resource"])

        if "write_disposition" in table:
            table_hints.append("write disposition: **%s**" % table["write_disposition"])

        columns = table["columns"]
        primary_keys: List[str] = list(
            flatten_list_or_items(
                [
                    col_name
                    for col_name in columns.keys()
                    if not col_name.startswith("_")
                    and columns[col_name].get("primary_key") is not None
                ]
            )
        )
        if primary_keys:
            table_hints.append("primary key(s): **%s**" % ", ".join(primary_keys))

        merge_keys = list(
            flatten_list_or_items(
                [
                    col_name
                    for col_name in columns.keys()
                    if not col_name.startswith("_")
                    and not columns[col_name].get("merge_key") is None  # noqa: E714
                ]
            )
        )

        if merge_keys:
            table_hints.append("merge key(s): **%s**" % ", ".join(merge_keys))

        st.subheader(f"Table: {table['name']}", divider=True)
        st.markdown(" | ".join(table_hints))
        if "resource" in table:
            resource_state_info(
                pipeline,
                current_schema.name,
                table["resource"],
            )

        # table schema contains various hints (like clustering or partition options)
        # that we do not want to show in basic view
        def essentials_f(c: Any) -> Dict[str, Any]:
            essentials: Dict[str, Any] = {}
            for k, v in c.items():
                if k in ["name", "data_type", "nullable"]:
                    essentials[k] = v

            return {
                "name": essentials["name"],
                "data_type": essentials["data_type"],
                "nullable": essentials["nullable"],
            }

        st.table(map(essentials_f, table["columns"].values()))
        show_data_button(pipeline, table["name"])
