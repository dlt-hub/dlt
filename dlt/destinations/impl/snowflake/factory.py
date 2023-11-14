import typing as t

from dlt.destinations.impl.snowflake.configuration import SnowflakeCredentials, SnowflakeClientConfiguration
from dlt.destinations.impl.snowflake import capabilities
from dlt.common.destination import Destination, DestinationCapabilitiesContext

if t.TYPE_CHECKING:
    from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient


class snowflake(Destination[SnowflakeClientConfiguration, "SnowflakeClient"]):

    spec = SnowflakeClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["SnowflakeClient"]:
        from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient

        return SnowflakeClient

    def __init__(
        self,
        credentials: SnowflakeCredentials = None,
        stage_name: t.Optional[str] = None,
        keep_staged_files: bool = True,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(credentials=credentials, stage_name=stage_name, keep_staged_files=keep_staged_files, **kwargs)
