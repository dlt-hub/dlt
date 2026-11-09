import datetime  # noqa: I251
from typing import List, Literal, NamedTuple, Optional

from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.storages.configuration import TSchemaFileFormat
from dlt.common.typing import DictStrAny, NotRequired, TypedDict
from dlt.pipeline.typing import TPipelineStep


TWorkspaceLocalVerb = Literal["read", "write", "execute", "network", "all"]
TWorkspaceDataVerb = Literal["read", "write", "all"]
TWorkspaceContextVerb = Literal["read", "write", "execute", "deploy", "all"]


class TWorkspaceAccess(TypedDict, total=False):
    """What may be touched in a workspace. Declared by an agent, required by a tool."""

    local: List[TWorkspaceLocalVerb]
    """What may be done on the machine the workspace lives on."""
    data: List[TWorkspaceDataVerb]
    """Access to workspace data, governed by the dlt profile in use."""
    context: List[TWorkspaceContextVerb]
    """Access to the context graph: telemetry, runs and job definitions. Read-only."""


TLocationScope = Literal["project", "global"]


class TLocationInfo(TypedDict):
    """A single config file location with its scope and presence status."""

    path: str
    present: bool
    scope: TLocationScope
    profile_name: NotRequired[str]


class TProviderInfo(TypedDict):
    """Config provider with all its file locations."""

    name: str
    is_empty: bool
    locations: List[TLocationInfo]


class TSourceItem(TypedDict):
    """A source returned by the AI context search API."""

    source_name: str
    description: Optional[str]
    description_verbose: NotRequired[str]
    sample_urls: NotRequired[str]


class TSchemaExport(TypedDict):
    """Exported schema in a requested format."""

    schema_name: str
    format_: TSchemaFileFormat
    content: str


class TTraceStepMetrics(TypedDict):
    """Files, rows and sizes of a step in a pipeline trace."""

    step: TPipelineStep
    started_at: datetime.datetime
    finished_at: NotRequired[datetime.datetime]
    load_ids: List[str]
    tables: NotRequired[List[DictStrAny]]
    """`DataWriterMetrics` of each table with `table_name`. Extract and normalize only."""
    resources: NotRequired[List[DictStrAny]]
    """`DataWriterMetrics` of each resource with `resource_name`. Extract only."""
    files: NotRequired[List[DictStrAny]]
    """`DataWriterMetrics` of each file with `job_id` and `table_name`. Extract and normalize
    only."""
    jobs: NotRequired[List[DictStrAny]]
    """`LoadJobMetrics` of each job. Load only."""
    step_exception: NotRequired[str]


class ProviderLocationInfo(NamedTuple):
    path: str
    present: bool
    scope: TLocationScope
    profile_name: Optional[str]


class ProviderInfo(NamedTuple):
    provider: ConfigProvider
    locations: List[ProviderLocationInfo]
