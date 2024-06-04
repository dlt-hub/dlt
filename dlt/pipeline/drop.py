from typing import Union, Iterable, Optional, List, Dict, Any, Tuple, TypedDict
from copy import deepcopy
from itertools import chain
from dataclasses import dataclass

from dlt.common.schema import Schema
from dlt.common.pipeline import (
    TPipelineState,
    _sources_state,
    _get_matching_resources,
    _get_matching_sources,
    reset_resource_state,
    _delete_source_state_keys,
)
from dlt.common.schema.typing import TSimpleRegex, TTableSchema
from dlt.common.schema.utils import (
    group_tables_by_resource,
    compile_simple_regexes,
    compile_simple_regex,
)
from dlt.common import jsonpath
from dlt.common.typing import REPattern


class _DropInfo(TypedDict):
    tables: List[str]
    resource_states: List[str]
    resource_names: List[str]
    state_paths: List[str]
    schema_name: str
    dataset_name: Optional[str]
    drop_all: bool
    resource_pattern: Optional[REPattern]
    warnings: List[str]


@dataclass
class _DropResult:
    schema: Schema
    state: TPipelineState
    info: _DropInfo
    dropped_tables: List[TTableSchema]


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
    for source_name in _get_matching_sources(source_pattern, state):
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
        _delete_source_state_keys(resolved_paths, source_state)
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
        schema: The schema to modify.
        state: The pipeline state to modify.
        resources: Resource name(s) or regex pattern(s) matching resource names to drop.
            If empty, no resources will be dropped unless `drop_all` is True.
        state_paths: JSON path(s) relative to the source state to drop.
        drop_all: If True, all resources will be dropped (supeseeds `resources`).
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

    schema = schema.clone()
    state = deepcopy(state)

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
        data_tables = {
            t["name"]: t for t in schema.data_tables(seen_data_only=True)
        }  # Don't remove _dlt tables
        resource_tables = group_tables_by_resource(data_tables, pattern=resource_pattern)
        resource_names = list(resource_tables.keys())
        # TODO: If drop_tables
        if not state_only:
            tables_to_drop = list(chain.from_iterable(resource_tables.values()))
            tables_to_drop.reverse()
        else:
            tables_to_drop = []
    else:
        tables_to_drop = []
        resource_names = []

    info: _DropInfo = dict(
        tables=[t["name"] for t in tables_to_drop],
        resource_states=[],
        state_paths=[],
        resource_names=resource_names,
        schema_name=schema.name,
        dataset_name=None,
        drop_all=drop_all,
        resource_pattern=resource_pattern,
        warnings=[],
    )

    new_state, info = _create_modified_state(
        state, resource_pattern, source_pattern, state_paths, info
    )
    info["resource_names"] = resource_names

    if resource_pattern and not resource_tables:
        info["warnings"].append(
            f"Specified resource(s) {str(resources)} did not select any table(s) in schema"
            f" {schema.name}. Possible resources are:"
            f" {list(group_tables_by_resource(data_tables).keys())}"
        )

    dropped_tables = schema.drop_tables([t["name"] for t in tables_to_drop], seen_data_only=True)
    return _DropResult(schema, new_state, info, dropped_tables)
