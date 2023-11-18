from typing import Mapping, cast

from dlt.destinations.impl.duckdb import capabilities as duck_insert_caps
from dlt.destinations.impl.redshift import capabilities as rd_insert_caps
from dlt.destinations.impl.postgres import capabilities as pg_insert_caps
from dlt.destinations.impl.bigquery import capabilities as jsonl_caps
from dlt.destinations.impl.filesystem import capabilities as filesystem_caps


DEFAULT_CAPS = pg_insert_caps
INSERT_CAPS = [duck_insert_caps, rd_insert_caps, pg_insert_caps]
JSONL_CAPS = [jsonl_caps, filesystem_caps]
ALL_CAPABILITIES = INSERT_CAPS + JSONL_CAPS


def json_case_path(name: str) -> str:
    return f"./tests/normalize/cases/{name}.json"
