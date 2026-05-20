from typing import List, Literal, NamedTuple, Optional

from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.storages.configuration import TSchemaFileFormat
from dlt.common.typing import NotRequired, TypedDict


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


class ProviderLocationInfo(NamedTuple):
    path: str
    present: bool
    scope: TLocationScope
    profile_name: Optional[str]


class ProviderInfo(NamedTuple):
    provider: ConfigProvider
    locations: List[ProviderLocationInfo]
