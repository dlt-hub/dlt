from __future__ import annotations

import dataclasses
from typing import ClassVar, Final, List, Optional, Sequence

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import known_sections
from dlt.common.destination.client import CredentialsConfiguration, DestinationClientConfiguration


@configspec
class HotdataCredentials(CredentialsConfiguration):
    api_key: Optional[str] = None
    """Hotdata API key."""
    workspace_id: Optional[str] = None
    """Hotdata workspace ID."""

    def __str__(self) -> str:
        return f"hotdata://{self.workspace_id}"


@configspec
class HotdataClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore[assignment]
        default="hotdata", init=False, repr=False, compare=False
    )
    credentials: HotdataCredentials = None

    api_base_url: str = "https://api.hotdata.dev"
    database_name: str = "dlt"
    """Name of the managed database to load into."""
    schema: str = "public"
    """Schema within the managed database."""
    write_disposition: str = "append"
    """Default write disposition when not set on the resource."""
    declared_tables: Optional[List[str]] = None
    """Explicit list of table names for multi-table pipelines."""
    create_database_if_missing: bool = True
    """Create the managed database automatically if it does not exist."""
    max_retries: int = 5
    retry_backoff_seconds: float = 1.0
    max_table_nesting: Optional[int] = None
    """Override the default maximum table nesting depth."""
    loader_parallelism_strategy: Optional[str] = None
    """Override the default loader parallelism strategy (e.g. 'table-sequential', 'row-parallel')."""

    __config_gen_annotations__: ClassVar[List[str]] = []
    __recommended_sections__: ClassVar[Sequence[str]] = (known_sections.DESTINATION, "hotdata", "")

    def __str__(self) -> str:
        return str(self.credentials)
