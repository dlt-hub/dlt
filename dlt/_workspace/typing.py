import datetime  # noqa: I251
from typing import Dict, List, Literal, NamedTuple, Optional, Union

from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.storages.configuration import TSchemaFileFormat
from dlt.common.typing import DictStrAny, NotRequired, TypedDict
from dlt.pipeline.typing import TPipelineStep


TWorkspaceLocalVerb = Literal["read", "write", "execute", "network", "all"]
TWorkspaceDataVerb = Literal["read", "write", "all"]
TWorkspaceContextVerb = Literal["read", "write", "execute", "deploy", "all"]


class TWorkspaceAccess(TypedDict, total=False):
    """What a job declares it needs to run. A tool requires it, a profile gives its `data` axis."""

    local: List[TWorkspaceLocalVerb]
    """What may be done on the machine the workspace lives on."""
    data: List[TWorkspaceDataVerb]
    """Access to workspace data. The profile of the job gives the credentials for it."""
    context: List[TWorkspaceContextVerb]
    """Access to the context graph: telemetry, runs and job definitions. Read-only."""


TProfileLocation = Literal["local", "remote"]
"""Where a profile is used: `local` profiles never leave the machine, `remote` ones deploy."""
TProfileDataAccess = Union[Literal["none"], TWorkspaceDataVerb]
"""Data access of a profile: `none` or a data verb of job access. `write` and `all` include
`read`."""


class TProfileDefinition(TypedDict):
    """A profile the workspace declares: where it is used and its data access."""

    location: TProfileLocation
    data_access: TProfileDataAccess
    env: NotRequired[str]
    """Environment of a remote profile, the one it is declared in."""
    default: NotRequired[bool]
    """The default local profile, or the default profile of its data access in an environment."""
    configured: NotRequired[bool]
    """True when the profile has settings files, or has no data access and needs none. Computed
    when the workspace definition is loaded, never stored. A profile without the flag counts as
    configured."""


class TEnvironmentDefinition(TypedDict):
    """An environment the workspace declares: what a deployment is made to, with the profiles
    deployed to it. Self-contained, so a manifest is generated from it alone."""

    name: str
    profiles: Dict[str, TProfileDefinition]
    """The remote profiles of the environment, deployed together. One profile per data access is
    its default. A deployment is made with the default profile with write data access."""
    default: NotRequired[bool]
    """The environment commands that deploy or work with a deployment assume when none is named."""
    strict_access: NotRequired[bool]
    """A strict environment: the profile of a job gives exactly the access that the job declares."""


class TWorkspaceDefinition(TypedDict):
    """The environments and the local profiles a workspace declares."""

    engine_version: int
    environments: Dict[str, TEnvironmentDefinition]
    local_profiles: Dict[str, TProfileDefinition]


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
