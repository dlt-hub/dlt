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
    """What a job declares it needs to run. A tool requires it. A profile gives its `data` axis."""

    local: List[TWorkspaceLocalVerb]
    """What may be done on the machine the workspace lives on."""
    data: List[TWorkspaceDataVerb]
    """Access to workspace data. The profile of the job gives the credentials for it."""
    context: List[TWorkspaceContextVerb]
    """Access to the context graph: telemetry, runs and job definitions. Read-only."""


TProfileLocation = Literal["local", "remote"]
"""Where a profile is used: `local` profiles never leave the machine, `remote` ones deploy with
their environment."""
TProfileDataAccess = Union[Literal["none"], TWorkspaceDataVerb]
"""Data access of a profile: `none` or a data verb of job access. `write` and `all` include
`read`."""
TProfileOptionsTakes = Literal["profile", "environment", "both", "none"]
"""What a command or an MCP tool takes to select the profile it runs with: a profile, an
environment, both, or none of them."""


DEFAULT_DEPLOYMENT_MODULE = "__deployment__"
"""The deployment module of an environment that names no other one."""


class TProfileStoredDefinition(TypedDict):
    """A profile in its stored form: its data access."""

    data_access: TProfileDataAccess
    default: NotRequired[bool]
    """The default local profile, or the default profile of its data access in an environment."""


class TProfileDefinition(TProfileStoredDefinition):
    """A declared profile with its location and, for a remote profile, its environment. dlt
    never stores it."""

    location: TProfileLocation
    env: NotRequired[str]
    """Environment of a remote profile, the one it is declared in."""
    configured: NotRequired[bool]
    """True when the profile has settings files. A profile without the flag counts as
    configured."""


class TEnvironmentStoredDefinition(TypedDict):
    """An environment in its stored form: what a deployment is made to, with the
    profiles deployed to it."""

    profiles: Dict[str, TProfileStoredDefinition]
    """The remote profiles of the environment, deployed together. One profile per data access is
    its default. A deployment is made with its default write profile."""
    default: NotRequired[bool]
    """The default environment: commands that deploy or work with a deployment assume it when
    nothing names one."""
    strict_access: NotRequired[bool]
    """A strict environment: the profile of a job gives exactly the access that the job declares."""


# NOTE: the runtime definitions narrow the stored fields, which mypy reports as overwriting
class TEnvironmentDefinition(TEnvironmentStoredDefinition):
    """A declared environment with its name and settings. It is self-contained, so dlt generates
    a deployment manifest from it alone. dlt never stores it."""

    name: str
    profiles: Dict[str, TProfileDefinition]  # type: ignore[misc]
    deployment_module: NotRequired[str]
    """The deployment module of the environment, from its config."""
    deploy_dashboard: NotRequired[bool]
    """Whether the environment deploys the workspace dashboard, from its config."""


class TWorkspaceStoredDefinition(TypedDict):
    """The environments and the local profiles in their stored form."""

    engine_version: int
    environments: Dict[str, TEnvironmentStoredDefinition]
    local_profiles: Dict[str, TProfileStoredDefinition]


class TWorkspaceDefinition(TWorkspaceStoredDefinition):
    """The environments and the local profiles that a workspace declares, with the name of each
    environment and the location and environment of each profile."""

    environments: Dict[str, TEnvironmentDefinition]  # type: ignore[misc]
    local_profiles: Dict[str, TProfileDefinition]  # type: ignore[misc]


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
