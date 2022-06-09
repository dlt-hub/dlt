
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Iterator, List, Literal, Sequence, Tuple, TypeVar, Union, Generic

from dlt.common.typing import DictStrAny, StrAny
from dlt.common.configuration.utils import TConfigSecret

TLoaderType = Literal["gcp", "redshift"]
TPipelineStage = Literal["extract", "unpack", "load"]

# extractor generator yields functions that returns list of items of the type (table) when called
# this allows generator to implement retry logic
# TExtractorItem = Callable[[], Iterator[StrAny]]
# # extractor generator yields tuples: (type of the item (table name), function defined above)
# TExtractorItemWithTable = Tuple[str, TExtractorItem]
# TExtractorGenerator = Callable[[DictStrAny], Iterator[TExtractorItemWithTable]]


@dataclass
class PipelineCredentials:
    CLIENT_TYPE: TLoaderType


@dataclass
class GCPPipelineCredentials(PipelineCredentials):
    PROJECT_ID: str
    DATASET: str
    BQ_CRED_CLIENT_EMAIL: str
    BQ_CRED_PRIVATE_KEY: TConfigSecret = None
    TIMEOUT: float = 30.0


@dataclass
class PostgresPipelineCredentials(PipelineCredentials):
    PG_DATABASE_NAME: str
    PG_SCHEMA_PREFIX: str
    PG_USER: str
    PG_HOST: str
    PG_PASSWORD: TConfigSecret = None
    PG_PORT: int = 5439
    PG_CONNECTION_TIMEOUT: int = 15
