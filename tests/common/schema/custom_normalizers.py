from dlt.common.normalizers.json import TNormalizedRowIterator
from dlt.common.normalizers.names.snake_case import normalize_make_path, normalize_break_path, normalize_make_dataset_name  # import path functions from snake case
from dlt.common.schema.schema import Schema
from dlt.common.typing import TDataItem


def normalize_path(path: str) -> str:
    return normalize_make_path(*map(normalize_identifier, normalize_break_path(path)))


def normalize_identifier(name: str) -> str:
    if name.startswith("column_"):
        return name
    return "column_" + name.lower()


def extend_schema(schema: Schema) -> None:
    json_config = schema._normalizers_config["json"]["config"]
    d_h = schema._settings.setdefault("default_hints", {})
    d_h["not_null"] = json_config["not_null"]


def normalize_data_item(schema: Schema, source_event: TDataItem, load_id: str, table_name) -> TNormalizedRowIterator:
    yield (table_name, None), source_event
