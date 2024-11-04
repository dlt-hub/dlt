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

from datetime import datetime  # noqa: I251
from pendulum import DateTime  # noqa: I251

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
from dlt.common.schema.typing import TTableSchema, TTableSchemaColumns
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

from dlt.extract.source import SourceFactory
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
my_callable: Callable[..., Any] = None  # type: ignore[assignment]
arrow_table: ArrowTable = None  # type: ignore[assignment]

#
#
#


#
# Some snippet specific constants (NOTE: please only use these if you can't use one of the above)
#
SERVER_NAME: str = ""
DATABASE_NAME: str = ""
SERVICE_PRINCIPAL_ID: str = ""
TENANT_ID: str = ""
SERVICE_PRINCIPAL_SECRETS: str = ""
REPO_NAME: str = ""
MAX_PAGE_SIZE: int = 100
DEFAULT_API_VERSION: str = ""
FIRST_DAY_OF_MILLENNIUM: TAnyDateTime = pendulum.datetime(2000, 1, 1)
START_DATE: DateTime = pendulum.datetime(2024, 1, 1)
START_DATE_STRING: str = ""
END_DATE: DateTime = pendulum.datetime(2024, 12, 31)
DEFAULT_START_DATE: DateTime = pendulum.datetime(2024, 1, 1)
DEFAULT_START_DATE_STRING: str = ""
MY_API_KEY: str = ""
DEFAULT_ITEMS_PER_PAGE: int = 100
DEFAULT_CHUNK_SIZE: int = 500
ENDPOINTS: List[str] = []
RESOURCE_URL: str = ""

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
