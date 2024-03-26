# This section is imported before linting

# mypy: disable-error-code="name-defined,import-not-found,import-untyped,empty-body,no-redef"

# some universal imports
from typing import Optional, Dict, List, Any, Iterable, Iterator, Tuple, Sequence, Callable

import os

import pendulum
from datetime import datetime  # noqa: I251
from pendulum import DateTime

import dlt
from dlt.common import json
from dlt.common.typing import TimedeltaSeconds, TAnyDateTime, TDataItem, TDataItems
from dlt.common.schema.typing import TTableSchema, TTableSchemaColumns

from dlt.common.pipeline import LoadInfo
from dlt.sources.helpers import requests
from dlt.extract import DltResource, DltSource
from dlt.common.configuration.specs import (
    GcpServiceAccountCredentials,
    ConnectionStringCredentials,
    OAuth2Credentials,
    BaseConfiguration,
)
from dlt.common.storages.configuration import FileSystemCredentials
from dlt.pipeline.exceptions import PipelineStepFailed

# some universal variables
pipeline: dlt.Pipeline = None  # type: ignore[assignment]
p: dlt.Pipeline = None  # type: ignore[assignment]
ex: Exception = None  # type: ignore[assignment]
load_info: LoadInfo = None  # type: ignore[assignment]
url: str = None  # type: ignore[assignment]
my_resource: DltResource = None  # type: ignore[assignment]
