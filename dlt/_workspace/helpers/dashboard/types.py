"""Shared TypedDicts for the dashboard."""

from typing_extensions import TypedDict


class TNameValueItem(TypedDict):
    name: str
    value: str


class TPipelineListItem(TypedDict):
    name: str
    timestamp: float
