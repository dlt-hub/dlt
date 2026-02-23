"""Shared TypedDicts for the dashboard."""

from typing import Union

from typing_extensions import TypedDict


class TNameValueItem(TypedDict):
    name: str
    value: str


class TPipelineListItem(TypedDict):
    name: str
    timestamp: float


class TQueryHistoryItem(TypedDict):
    query: str
    row_count: int


class _TTableListItemRequired(TypedDict):
    name: str


class TTableListItem(_TTableListItemRequired, total=False):
    """Row returned by create_table_list: required 'name' plus config-driven fields and row_count."""

    row_count: Union[int, None]


class TLoadItem(TypedDict):
    """Row from _dlt_loads after humanize_datetime_values."""

    load_id: str
    schema_name: str
    status: str
    inserted_at: str
    schema_version_hash: str
