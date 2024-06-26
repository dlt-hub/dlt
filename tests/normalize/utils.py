from dlt.destinations import duckdb, redshift, postgres, bigquery, filesystem


# callables to capabilities
DEFAULT_CAPS = postgres().capabilities
INSERT_CAPS = [duckdb().capabilities, redshift().capabilities, DEFAULT_CAPS]
JSONL_CAPS = [bigquery().capabilities, filesystem().capabilities]
ALL_CAPABILITIES = INSERT_CAPS + JSONL_CAPS


def json_case_path(name: str) -> str:
    return f"./tests/normalize/cases/{name}.json"
