"""Schema introspection helpers: table lists, column lists, schema retrieval, and resource state."""

from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from dlt._workspace.helpers.dashboard.types import TNameValueItem, TTableListItem

import marimo as mo

import dlt

from dlt.common.destination.client import WithStateSync
from dlt.common.json import json
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import DictStrAny

from dlt._workspace.helpers.dashboard import strings
from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils.ui import BoundedDict, small


def schemas_to_table_items(
    schemas: Iterable[Schema], default_schema_name: str
) -> List[TNameValueItem]:
    """Convert a list of schemas to name/value display items, with default schema first."""
    schemas = sorted(
        schemas, key=lambda s: 0 if getattr(s, "name", None) == default_schema_name else 1
    )
    table_items: List[TNameValueItem] = []
    count = 0
    for schema in schemas:
        table_items.append(
            {
                "name": "schemas" if count == 0 else "",
                "value": (
                    schema.name
                    + f" ({schema.version}, {schema.version_hash[:8]}) "
                    + (" (default)" if schema.name == default_schema_name else "")
                ),
            }
        )
        count += 1
    return table_items


def create_table_list(
    c: DashboardConfiguration,
    pipeline: dlt.Pipeline,
    selected_schema_name: str = None,
    show_internals: bool = False,
    show_child_tables: bool = True,
    show_row_counts: bool = False,
) -> List[TTableListItem]:
    """Create a list of tables for the pipeline, optionally including internals, child tables, and row counts."""
    from dlt._workspace.helpers.dashboard.utils.queries import get_row_counts

    tables = list(
        pipeline.schemas[selected_schema_name].data_tables(
            seen_data_only=True, include_incomplete=False
        )
    )
    if not show_child_tables:
        tables = [t for t in tables if t.get("parent") is None]

    if show_internals:
        tables = tables + list(pipeline.schemas[selected_schema_name].dlt_tables())

    row_counts = get_row_counts(pipeline, selected_schema_name) if show_row_counts else {}
    table_list: List[TTableListItem] = []
    for table in tables:
        item = TTableListItem(
            name=table.get("name", ""),
            row_count=row_counts.get(table["name"], None),
        )
        for prop in c.table_list_fields:
            item[prop] = table.get(prop, None)  # type: ignore[literal-required]
        table_list.append(item)
    table_list.sort(key=lambda x: x["name"])
    return table_list


def create_column_list(
    c: DashboardConfiguration,
    pipeline: dlt.Pipeline,
    table_name: str,
    selected_schema_name: str = None,
    show_internals: bool = False,
    show_type_hints: bool = True,
    show_other_hints: bool = False,
    show_custom_hints: bool = False,
) -> List[Dict[str, Any]]:
    """Create a list of columns for a table, with configurable hint visibility."""
    column_list: List[Dict[str, Any]] = []
    for column in (
        pipeline.schemas[selected_schema_name]
        .get_table_columns(table_name, include_incomplete=False)
        .values()
    ):
        column_dict: Dict[str, Any] = {
            "name": column["name"],
        }

        if show_type_hints:
            column_dict = {
                **column_dict,
                **{hint: column.get(hint, None) for hint in c.column_type_hints},
            }

        if show_other_hints:
            column_dict = {
                **column_dict,
                **{hint: column.get(hint, None) for hint in c.column_other_hints},
            }

        if show_custom_hints:
            for key in column:
                if key.startswith("x-"):
                    column_dict[key] = column[key]  # type: ignore

        column_list.append(column_dict)

    if not show_internals:
        column_list = [c for c in column_list if not c["name"].lower().startswith("_dlt")]
    return column_list


def get_source_and_resource_state_for_table(
    table: TTableSchema, pipeline: dlt.Pipeline, schema_name: str
) -> Tuple[str, DictStrAny, DictStrAny]:
    """Return (resource_name, source_state, resource_state) for the resource that created the given table."""
    if "resource" not in table:
        return None, {}, {}

    pipeline.activate()
    resource_name = table["resource"]
    source_state = dlt.extract.state.source_state(schema_name)
    resource_state = dlt.extract.state.resource_state(resource_name, source_state)
    source_state = {k: v for k, v in source_state.items() if k != "resources"}

    return table["resource"], source_state, resource_state


def get_schema_by_version(pipeline: dlt.Pipeline, version_hash: str) -> Schema:
    """Retrieve a schema from the destination by its version hash (cached)."""
    cache_key = (pipeline.pipeline_name, version_hash)
    if cache_key not in _schema_version_cache:
        _schema_version_cache[cache_key] = _fetch_schema_by_version(pipeline, version_hash)
    result: Schema = _schema_version_cache[cache_key]
    return result


_schema_version_cache: BoundedDict = BoundedDict(32)


def _fetch_schema_by_version(pipeline: dlt.Pipeline, version_hash: str) -> Schema:
    """Retrieve a schema from the destination by its version hash."""
    with pipeline.destination_client() as client:
        if isinstance(client, WithStateSync):
            stored_schema = client.get_stored_schema_by_hash(version_hash)
            if not stored_schema:
                return None
            return Schema.from_stored_schema(json.loads(stored_schema.schema))
    return None


def build_resource_state_widget(
    pipeline: dlt.Pipeline, schema_name: str, table_name: str
) -> Optional[mo.Html]:
    """Build source/resource state accordion for the given table, or None if not applicable."""
    schema_table = pipeline.schemas[schema_name].tables[table_name]
    resource_name, source_state, resource_state = get_source_and_resource_state_for_table(
        schema_table, pipeline, schema_name
    )
    if not resource_name:
        return None

    state_content = mo.hstack(
        [
            mo.vstack(
                [
                    mo.md(small(strings.schema_source_state_label.format(schema_name))),
                    mo.json(source_state),
                ]
            ),
            mo.vstack(
                [
                    mo.md(small(strings.schema_resource_state_label.format(resource_name))),
                    mo.json(resource_state),
                ]
            ),
        ],
        justify="start",
        widths="equal",
    )

    return mo.accordion(
        {small(strings.schema_show_resource_state.format(resource_name, table_name)): state_content}
    )
