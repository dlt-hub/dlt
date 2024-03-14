# This section is imported before linting

# some universal imports
from typing import Optional, Dict, List, Any, Iterable, Iterator, Tuple, Sequence, Callable

import os
import json

import pendulum
from pendulum import DateTime
from datetime import datetime

import dlt
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

# some universal variables
pipeline: dlt.Pipeline = None
p: dlt.Pipeline = None
ex: Exception = None
load_info: LoadInfo = None
url: str = None
