# This section is imported before linting

# mypy: disable-error-code="name-defined,import-not-found,import-untyped,empty-body,no-redef"

# some universal imports
from typing import (
    Optional,
    Dict,
    List,
    Any,
    Iterable,
    Iterator,
    Tuple,
    Sequence,
    Callable,
    Union,
    Generator,
)

import os
import duckdb
import urllib
import itertools
import airflow
import datetime  # noqa: I251
import pendulum  # noqa: I251


from airflow.decorators import dag

#
# various dlt imports used by snippets
#
import dlt
from dlt.common import json, pendulum
from dlt.common.typing import (
    TimedeltaSeconds,
    TAnyDateTime,
    TDataItem,
    TDataItems,
    StrStr,
    DictStrAny,
)
from dlt.common.schema.typing import TTableSchema, TTableSchemaColumns, TColumnSchema
from dlt.common.pipeline import LoadInfo
from dlt.common.configuration.specs import (
    GcpServiceAccountCredentials,
    ConnectionStringCredentials,
    OAuth2Credentials,
    BaseConfiguration,
    AwsCredentials,
    GcpOAuthCredentials,
    GcpServiceAccountCredentials,
)
from dlt.common.libs.pyarrow import Table as ArrowTable
from dlt.common.data_writers import TDataItemFormat

from dlt.extract.source import SourceFactory
from dlt.extract.items import DataItemWithMeta
from dlt.extract import DltResource, DltSource
from dlt.common.storages.configuration import FileSystemCredentials
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.common.schema import DataValidationError

#
# dlt core sources
#
from dlt.sources.sql_database import sql_database, sql_table, Table
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
    SinglePagePaginator,
    HeaderLinkPaginator,
    JSONResponseCursorPaginator,
    OffsetPaginator,
    PageNumberPaginator,
)
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth, AuthConfigBase
from dlt.sources.helpers import requests

#
# some universal variables used by snippets
# NOTE: these are only used for typechecking, setting to None is ok
#
pipeline: dlt.Pipeline = None  # type: ignore[assignment]
p: dlt.Pipeline = None  # type: ignore[assignment]
ex: Exception = None  # type: ignore[assignment]
load_info: LoadInfo = None  # type: ignore[assignment]
url: str = None  # type: ignore[assignment]
resource: DltResource = None  # type: ignore[assignment]
data: List[Any] = None  # type: ignore[assignment]
item: Any = None  # type: ignore[assignment]
arrow_table: ArrowTable = None  # type: ignore[assignment]

my_callable: Callable[..., Any] = None  # type: ignore[assignment]

# getters for items
_get_event_pages: Callable[..., Any] = None  # type: ignore[assignment]
_get_rest_pages: Callable[..., Any] = None  # type: ignore[assignment]
_get_issues_page: Callable[..., Any] = None  # type: ignore[assignment]
_get_data: Callable[..., Any] = None  # type: ignore[assignment]
_get_data_chunked: Callable[..., Any] = None  # type: ignore[assignment]
_get_players_archives: Callable[..., Any] = None  # type: ignore[assignment]
_get_paginated: Callable[..., Any] = None  # type: ignore[assignment]
_get_users: Callable[..., Any] = None  # type: ignore[assignment]
_get_orders: Callable[..., Any] = None  # type: ignore[assignment]
_get_users: Callable[..., Any] = None  # type: ignore[assignment]
_get_details: Callable[..., Any] = None  # type: ignore[assignment]
_get_records: Callable[..., Any] = None  # type: ignore[assignment]
_get_sheet: Callable[..., Any] = None  # type: ignore[assignment]

# helpers
_hash_str: Callable[..., Any] = None  # type: ignore[assignment]
_get_batch_from_bucket: Callable[..., Any] = None  # type: ignore[assignment]
_get_primary_key: Callable[..., Any] = None  # type: ignore[assignment]
_get_path_with_retry: Callable[..., Any] = None  # type: ignore[assignment]

#
#
#

#
# Some snippet specific constants (NOTE: please only use these if you can't use one of the above)
#
SERVER_NAME: str = ""
DATABASE_NAME: str = ""
SERVICE_PRINCIPAL_ID: str = ""
SERVICE_PRINCIPAL_SECRETS: str = ""
TENANT_ID: str = ""
REPO_NAME: str = ""
MAX_PAGE_SIZE: int = 100
API_VERSION: str = ""
FIRST_DAY_OF_MILLENNIUM: TAnyDateTime = pendulum.DateTime(2000, 1, 1)
START_DATE: pendulum.DateTime = pendulum.DateTime(2024, 1, 1)
END_DATE: pendulum.DateTime = pendulum.DateTime(2024, 12, 31)
START_DATE_STRING: str = ""
API_KEY: str = ""
ITEMS_PER_PAGE: int = 100
CHUNK_SIZE: int = 500
ENDPOINTS: List[str] = []
RESOURCE_URL: str = ""
BASE_URL: str = ""

# functions
hash_string: Callable[[str], str] = None  # type: ignore[assignment]

# sources
my_source: DltSource = None  # type: ignore[assignment]
source: DltSource = None  # type: ignore[assignment]
pipedrive_source: SourceFactory[Any, Any] = None  # type: ignore[assignment]
zendesk_support: SourceFactory[Any, Any] = None  # type: ignore[assignment]
facebook_ads_source: SourceFactory[Any, Any] = None  # type: ignore[assignment]
chess_source: SourceFactory[Any, Any] = None  # type: ignore[assignment]
airtable_emojis: SourceFactory[Any, Any] = None  # type: ignore[assignment]
merge_source: SourceFactory[Any, Any] = None  # type: ignore[assignment]
sql_source: SourceFactory[Any, Any] = None  # type: ignore[assignment]
data_source: SourceFactory[Any, Any] = None  # type: ignore[assignment]

# resources
my_resource: DltResource = None  # type: ignore[assignment]
source: DltResource = None  # type: ignore[assignment]
incremental_resource: DltResource = None  # type: ignore[assignment]

# facebook ads
DEFAULT_ADCREATIVE_FIELDS: List[str] = []

# docs/website/docs/dlt-ecosystem/verified-sources/asana.md
PROJECT_FIELDS: List[str] = []
TASK_FIELDS: List[str] = []

# docs/website/docs/dlt-ecosystem/destinations/weaviate.md
vectorize: List[str] = []
tokenization: Dict[str, Any] = {}

# docs/website/docs/dlt-ecosystem/verified-sources/chess.md
players_online_status: DltResource = None  # type: ignore[assignment]
