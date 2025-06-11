from typing import Union, Iterable, Optional, List, Dict, Tuple
from itertools import chain
from dataclasses import dataclass

from dlt.common.schema import Schema
from dlt.common.typing import TypedDict

from dlt.common.schema.typing import TSimpleRegex, TTableSchema, TColumnSchema
from dlt.common.schema.utils import (
    group_tables_by_resource,
    compile_simple_regexes,
    compile_simple_regex,
    has_table_seen_data,
    is_nullable_column,
)
from dlt.common import jsonpath
from dlt.common.typing import REPattern

from dlt.extract.state import (
    TPipelineState,
    _sources_state,
    _get_matching_resources,
    get_matching_sources,
    reset_resource_state,
    delete_source_state_keys,
)


class _DropInfo(TypedDict):
    tables: List[str]
    tables_with_data: List[str]
    resource_states: List[str]
    resource_names: List[str]
    state_paths: List[str]
    schema_name: str
    drop_all: bool
    resource_pattern: Optional[REPattern]
    warnings: List[str]
    drop_columns: bool


class _FromTableDropCols(TypedDict):
    from_table: str
    drop_columns: List[str]


@dataclass
class _DropResult:
    schema: Schema
    info: _DropInfo
    modified_tables: List[TTableSchema]
    state: Optional[TPipelineState] = None
    from_tables_drop_cols: Optional[List[_FromTableDropCols]] = None


def _create_modified_state(
    state: TPipelineState,
    resource_pattern: Optional[REPattern],
    source_pattern: REPattern,
    state_paths: jsonpath.TAnyJsonPath,
    info: _DropInfo,
) -> Tuple[TPipelineState, _DropInfo]:
    # if not self.drop_state:
    #     return state  # type: ignore[return-value]
    all_source_states = _sources_state(state)
    for source_name in get_matching_sources(source_pattern, state):
        source_state = all_source_states[source_name]
        # drop table states
        if resource_pattern:
            for key in _get_matching_resources(resource_pattern, source_state):
                info["resource_states"].append(key)
                reset_resource_state(key, source_state)

        # drop additional state paths
        # Don't drop 'resources' key if jsonpath is wildcard
        resolved_paths = [
            p for p in jsonpath.resolve_paths(state_paths, source_state) if p != "resources"
        ]
        if state_paths and not resolved_paths:
            info["warnings"].append(
                f"State paths {state_paths} did not select any paths in source {source_name}"
            )
        delete_source_state_keys(resolved_paths, source_state)
        info["state_paths"].extend(f"{source_name}.{p}" for p in resolved_paths)
    return state, info


def drop_resources(
    schema: Schema,
    state: TPipelineState,
    resources: Union[Iterable[Union[str, TSimpleRegex]], Union[str, TSimpleRegex]] = (),
    state_paths: jsonpath.TAnyJsonPath = (),
    drop_all: bool = False,
    state_only: bool = False,
    sources: Optional[Union[Iterable[Union[str, TSimpleRegex]], Union[str, TSimpleRegex]]] = None,
) -> _DropResult:
    """Generate a new schema and pipeline state with the requested resources removed.

    Args:
        schema: The schema to modify. Note that schema is changed in place.
        state: The pipeline state to modify. Note that state is changed in place.
        resources: Resource name(s) or regex pattern(s) matching resource names to drop.
            If empty, no resources will be dropped unless `drop_all` is True.
        state_paths: JSON path(s) relative to the source state to drop.
        drop_all: If True, all resources will be dropped (supersedes `resources`).
        state_only: If True, only modify the pipeline state, not schema
        sources: Only wipe state for sources matching the name(s) or regex pattern(s) in this list
            If not set all source states will be modified according to `state_paths` and `resources`

    Returns:
        A 3 part tuple containing the new schema, the new pipeline state, and a dictionary
        containing information about the drop operation.
    """

    if isinstance(resources, str):
        resources = [resources]
    resources = list(resources)
    if isinstance(sources, str):
        sources = [sources]
    if sources is not None:
        sources = list(sources)
    if isinstance(state_paths, str):
        state_paths = [state_paths]

    state_paths = jsonpath.compile_paths(state_paths)

    resources = set(resources)
    if drop_all:
        resource_pattern = compile_simple_regex(TSimpleRegex("re:.*"))  # Match everything
    elif resources:
        resource_pattern = compile_simple_regexes(TSimpleRegex(r) for r in resources)
    else:
        resource_pattern = None
    if sources is not None:
        source_pattern = compile_simple_regexes(TSimpleRegex(s) for s in sources)
    else:
        source_pattern = compile_simple_regex(TSimpleRegex("re:.*"))  # Match everything

    if resource_pattern:
        # (1) Don't remove _dlt tables (2) Drop all selected tables from the schema
        # (3) Mark tables that seen data to be dropped in destination
        data_tables = {t["name"]: t for t in schema.data_tables(include_incomplete=True)}
        resource_tables = group_tables_by_resource(data_tables, pattern=resource_pattern)
        resource_names = list(resource_tables.keys())
        tables_to_drop_from_schema = list(chain.from_iterable(resource_tables.values()))
        tables_to_drop_from_schema.reverse()
        tables_to_drop_from_schema_names = [t["name"] for t in tables_to_drop_from_schema]
        tables_to_drop_from_dest = [t for t in tables_to_drop_from_schema if has_table_seen_data(t)]
    else:
        tables_to_drop_from_schema_names = []
        tables_to_drop_from_dest = []
        tables_to_drop_from_schema = []
        resource_names = []

    info: _DropInfo = dict(
        tables=tables_to_drop_from_schema_names if not state_only else [],
        tables_with_data=[t["name"] for t in tables_to_drop_from_dest] if not state_only else [],
        resource_states=[],
        state_paths=[],
        resource_names=resource_names if not state_only else [],
        schema_name=schema.name,
        drop_all=drop_all,
        resource_pattern=resource_pattern,
        warnings=[],
        drop_columns=False,
    )

    new_state, info = _create_modified_state(
        state, resource_pattern, source_pattern, state_paths, info
    )
    # info["resource_names"] = resource_names if not state_only else []

    if resource_pattern and not resource_tables:
        info["warnings"].append(
            f"Specified resource(s) {str(resources)} did not select any table(s) in schema"
            f" {schema.name}. Possible resources are:"
            f" {list(group_tables_by_resource(data_tables).keys())}"
        )

    if not state_only:
        # drop only the selected tables
        schema.drop_tables(tables_to_drop_from_schema_names)
    return _DropResult(schema, info, tables_to_drop_from_dest, new_state, None)


def _get_droppable_columns(columns: Dict[str, TColumnSchema]) -> List[str]:
    disqualifying_hints = [
        "parition",
        "cluster",
        "unique",
        "sort",
        "primary_key",
        "row_key",
        "parent_key",
        "root_key",
        "merge_key",
        "variant",
        "hard_delete",
        "dedup_sort",
        "incremental",
    ]
    droppable_cols: List[str] = []
    for col_name, col_schema in columns.items():
        if not is_nullable_column(col_schema) or any(
            col_schema.get(hint, False) for hint in disqualifying_hints
        ):
            continue
        droppable_cols.append(col_name)
    return droppable_cols


def drop_columns(
    schema: Schema,
    from_resources: Union[Iterable[Union[str, TSimpleRegex]], Union[str, TSimpleRegex]],
    columns: Union[Iterable[Union[str, TSimpleRegex]], Union[str, TSimpleRegex]],
) -> _DropResult:
    """Generate a new schema and pipeline state with the requested columns removed.

    Args:
        schema: The schema to modify. Note that schema is changed in place.
        state: The pipeline state to modify. Note that state is changed in place.
        from_resources: Resources to drop columns from
        columns: Columns to drop from the specified resource

    Returns:
        A 3 part tuple containing the new schema, the new pipeline state, and a dictionary
        containing information about the drop operation.
    """
    from_resources = set(from_resources)
    resource_pattern = compile_simple_regexes(TSimpleRegex(r) for r in from_resources)
    data_tables = {t["name"]: t for t in schema.data_tables(include_incomplete=True)}
    resource_tables = group_tables_by_resource(data_tables, pattern=resource_pattern)
    resource_names = list(resource_tables.keys())
    tables_to_drop_from_schema = list(chain.from_iterable(resource_tables.values()))
    tables_to_drop_from_schema.reverse()
    tables_to_drop_from_schema_names = [t["name"] for t in tables_to_drop_from_schema]
    tables_to_drop_from_dest = [t for t in tables_to_drop_from_schema if has_table_seen_data(t)]
    tables_to_drop_from_dest_names = [t["name"] for t in tables_to_drop_from_dest]

    columns = set(columns)
    columns_pattern = compile_simple_regexes(TSimpleRegex(r) for r in columns)

    # Collect columns to drop grouped by table
    from_tables_drop_cols: List[_FromTableDropCols] = []
    for table in tables_to_drop_from_dest:
        table_name = table["name"]
        table_schema_cols = table["columns"]
        table_schema_droppable_cols = _get_droppable_columns(table_schema_cols)

        drop_cols: List[str] = []

        for col in table_schema_droppable_cols:
            if columns_pattern.match(col):
                col_schema = schema.tables[table["name"]]["columns"].pop(col)
                drop_cols.append(col_schema["name"])

        if drop_cols:
            from_tables_drop_cols.append({"from_table": table_name, "drop_columns": drop_cols})

    # Set to None if no columns need to be dropped
    from_tables_drop_cols = from_tables_drop_cols or None

    info: _DropInfo = dict(
        tables=tables_to_drop_from_schema_names if from_tables_drop_cols else [],
        tables_with_data=tables_to_drop_from_dest_names if from_tables_drop_cols else [],
        resource_states=[],
        state_paths=[],
        resource_names=resource_names if from_tables_drop_cols else [],
        schema_name=schema.name,
        drop_all=False,
        resource_pattern=resource_pattern,
        warnings=[],
        drop_columns=True,
    )

    return _DropResult(schema, info, tables_to_drop_from_dest, None, from_tables_drop_cols)
