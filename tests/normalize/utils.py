from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TSimpleRegex

from dlt.destinations import duckdb, redshift, postgres, bigquery, filesystem


# callables to capabilities
DEFAULT_CAPS = postgres().capabilities
INSERT_CAPS = [duckdb().capabilities, redshift().capabilities, DEFAULT_CAPS]
JSONL_CAPS = [bigquery().capabilities, filesystem().capabilities]
ALL_CAPABILITIES = INSERT_CAPS + JSONL_CAPS


def json_case_path(name: str) -> str:
    return f"./tests/normalize/cases/{name}.json"


def add_preferred_types(schema: Schema) -> None:
    schema._settings["preferred_types"] = {}
    schema._settings["preferred_types"][TSimpleRegex("timestamp")] = "timestamp"
    # any column with confidence should be float
    schema._settings["preferred_types"][TSimpleRegex("re:confidence")] = "double"
    # value should be wei
    schema._settings["preferred_types"][TSimpleRegex("value")] = "wei"
    # number should be decimal
    schema._settings["preferred_types"][TSimpleRegex("re:^number$")] = "decimal"

    schema._compile_settings()
