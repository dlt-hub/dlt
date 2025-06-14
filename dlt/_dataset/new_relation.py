from __future__ import annotations
from typing import Any, Generator, Optional, Sequence, Tuple, Type, TYPE_CHECKING, overload, Union
from contextlib import contextmanager

from sqlglot import exp
from sqlglot.schema import Schema as SQLGlotSchema

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import Self
from dlt.transformations import lineage
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.dataset.exceptions import (
    ReadableRelationHasQueryException,
)
from dlt.destinations.dataset.utils import normalize_query

if TYPE_CHECKING:
    from dlt._dataset import Dataset
