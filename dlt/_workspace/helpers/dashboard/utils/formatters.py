"""String formatting, datetime humanization, and dict-to-table conversion utilities for the dashboard."""

from datetime import datetime  # noqa: I251
from itertools import chain
from typing import Any, Dict, List, Mapping, Union, cast

from dlt._workspace.helpers.dashboard.typing import TNameValueItem

from dlt.common.pendulum import pendulum

from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.destination.exceptions import SqlClientNotAvailable
from dlt.destinations.exceptions import DatabaseUndefinedRelation, DestinationUndefinedEntity
from dlt.pipeline.exceptions import PipelineConfigMissing

from dlt._workspace.helpers.dashboard import strings
from dlt._workspace.helpers.dashboard.config import DashboardConfiguration


def humanize_datetime(dt: Union[str, int, float, datetime], datetime_format: str) -> str:
    """Format a datetime-like value (datetime, timestamp, or ISO string) into a human-readable string."""
    if dt in ["", None, "-"]:
        return "-"
    elif isinstance(dt, datetime):
        p = pendulum.instance(dt)
    elif isinstance(dt, str) and dt.replace(".", "").isdigit():
        p = pendulum.from_timestamp(float(dt))
    elif isinstance(dt, str):
        p = cast(pendulum.DateTime, pendulum.parse(dt))
    elif isinstance(dt, (int, float)):
        p = pendulum.from_timestamp(dt)
    else:
        raise ValueError(f"Invalid datetime value: {dt}")
    return p.format(datetime_format)


def humanize_datetime_values(c: DashboardConfiguration, d: Dict[str, Any]) -> Dict[str, Any]:
    """Format well-known datetime keys (started_at, finished_at, etc.) in a dict to human-readable strings."""
    _DATETIME_KEYS = ("started_at", "finished_at", "inserted_at", "created", "last_modified")

    # compute duration before we overwrite the raw values
    started_at = d.get("started_at", "")
    finished_at = d.get("finished_at", "")
    if started_at not in ("", None, "-") and finished_at not in ("", None, "-"):
        d["duration"] = (
            pendulum.instance(finished_at).diff(pendulum.instance(started_at)).in_words()
        )

    for key in _DATETIME_KEYS:
        if d.get(key):
            d[key] = humanize_datetime(d[key], c.datetime_format)

    # load_id is a timestamp that maps to a separate display key
    if d.get("load_id"):
        d["load_package_created_at"] = humanize_datetime(d["load_id"], c.datetime_format)

    return d


def format_duration(ms: float) -> str:
    """Format a duration in milliseconds as a human-readable string (e.g. '120ms', '3.5s', '1.2m')."""
    if ms < 1000:
        return f"{int(ms)}ms"
    elif ms < 60000:
        return f"{round(ms / 100) / 10}s"
    else:
        return f"{round(ms / 6000) / 10}m"


def format_exception_message(exception: Exception) -> str:
    """Convert a pipeline/destination exception to a user-friendly error message."""
    if isinstance(exception, (PipelineConfigMissing, ConfigFieldMissingException)):
        return strings.error_config_missing
    elif isinstance(exception, (SqlClientNotAvailable)):
        return strings.error_sql_not_supported
    elif isinstance(exception, (DestinationUndefinedEntity, DatabaseUndefinedRelation)):
        return strings.error_undefined_entity
    return str(exception)


def filter_empty_values(d: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a new dict with all None and empty string values removed."""
    return {k: v for k, v in d.items() if v is not None and v != ""}


def align_dict_keys(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ensure all dicts in a list share the same keys, filling missing ones with '-' for uniform table rendering."""
    items = cast(List[Dict[str, Any]], [filter_empty_values(i) for i in items])
    all_keys = set(chain.from_iterable(i.keys() for i in items))
    for i in items:
        i.update({key: "-" for key in all_keys if key not in i})
    return items


def dict_to_table_items(d: Dict[str, Any]) -> List[TNameValueItem]:
    """Convert a dict to a list of {name, value} dicts for display in a marimo table."""
    return [{"name": k, "value": v} for k, v in d.items()]
