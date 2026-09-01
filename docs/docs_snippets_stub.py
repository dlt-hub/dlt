"""Source code stub for type checking code snippets.

The logic in `test_snippets.py` iterates over `py` snippets found in the docs
markdown files. When type checking, it prepends the content of this file to import
some common types. This reduces the verbosity of individual snippets.
"""
from typing import Any, Callable

# allow top-level imports of common package
import dlt
import datetime  # noqa: I251
import pendulum  # noqa: I251
import json
import requests
from pandas import DataFrame

# define type annotations for variables that have self-explanatory names e.g., `pipeline: dlt.Pipeline`
# this reduces verbosity of examples, use sparingly.
# NOTE this has the effect of contaminating the global scope with `LoadInfo` for example.
from dlt.common.pipeline import LoadInfo
from dlt.common.libs.pyarrow import Table as ArrowTable
from dlt.extract.reference import SourceFactory
from dlt.extract import DltResource, DltSource

pipeline: dlt.Pipeline = None  # type: ignore
my_pipeline: dlt.Pipeline = None  # type: ignore
dataset: dlt.Dataset = None  # type: ignore
p: dlt.Pipeline = None  # type: ignore
ex: Exception = None  # type: ignore
load_info: LoadInfo = None  # type: ignore
url: str = None  # type: ignore
resource: DltResource = None  # type: ignore
data: list[Any] = None  # type: ignore
item: Any = None  # type: ignore
arrow_table: ArrowTable = None  # type: ignore
my_callable: Callable[..., Any] = None  # type: ignore
my_source: DltSource = None  # type: ignore
source: DltSource = None  # type: ignore
table_name: str = ""
schema_name: str = ""
bucket_url: str = ""
pipedrive_source: SourceFactory[Any, Any] = None  # type: ignore
zendesk_support: SourceFactory[Any, Any] = None  # type: ignore
facebook_ads_source: SourceFactory[Any, Any] = None  # type: ignore
chess_source: SourceFactory[Any, Any] = None  # type: ignore
airtable_emojis: SourceFactory[Any, Any] = None  # type: ignore
merge_source: SourceFactory[Any, Any] = None  # type: ignore
sql_source: SourceFactory[Any, Any] = None  # type: ignore
data_source: SourceFactory[Any, Any] = None  # type: ignore
my_resource: DltResource = None  # type: ignore
incremental_resource: DltResource = None  # type: ignore
df: DataFrame | None = None  # type: ignore
