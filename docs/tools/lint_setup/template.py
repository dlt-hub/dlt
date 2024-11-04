# This section is imported before linting

# mypy: disable-error-code="import-not-found,import-untyped,empty-body,no-redef"

# some universal imports
from typing import Optional, Dict, List, Any, Iterable, Iterator, Tuple, Sequence, Callable, Union, Generator

import os
import duckdb

from datetime import datetime  # noqa: I251
from pendulum import DateTime  # noqa: I251


#
# various dlt imports used by snippets
#
import dlt
from dlt.common import json, pendulum
from dlt.common.typing import TimedeltaSeconds, TAnyDateTime, TDataItem, TDataItems, DictStrAny
from dlt.common.schema.typing import TTableSchema, TTableSchemaColumns
from dlt.common.pipeline import LoadInfo
from dlt.common.configuration.specs import (
    GcpServiceAccountCredentials,
    ConnectionStringCredentials,
    OAuth2Credentials,
    BaseConfiguration,
)
from dlt.common.libs.pyarrow import Table as ArrowTable

from dlt.extract import DltResource, DltSource
from dlt.common.storages.configuration import FileSystemCredentials
from dlt.pipeline.exceptions import PipelineStepFailed

#
# dlt sources
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
my_resource: DltResource = None  # type: ignore[assignment]
source: DltSource = None  # type: ignore[assignment]
resource: DltResource = None  # type: ignore[assignment]
data: List[Dict[str, Any]] = None  # type: ignore[assignment]

arrow_table: ArrowTable = None # type: ignore[assignment]


#
# Some snippet specific vars (NOTE: please only use these if you can't use one of the above)
#

# synapse
server_name: str = None # type: ignore[assignment]
database_name: str = None # type: ignore[assignment]
service_principal_id: str = None # type: ignore[assignment]
tenant_id: str = None # type: ignore[assignment]
service_principal_secret: str = None # type: ignore[assignment]
