import typing as t

from dlt.destinations.impl.snowflake.configuration import (
    SnowflakeCredentials,
    SnowflakeClientConfiguration,
)
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
        credentials: t.Union[SnowflakeCredentials, t.Dict[str, t.Any], str] = None,
        stage_name: t.Optional[str] = None,
        keep_staged_files: bool = True,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Snowflake destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the snowflake database. Can be an instance of `SnowflakeCredentials` or
                a connection string in the format `snowflake://user:password@host:port/database`
            stage_name: Name of an existing stage to use for loading data. Default uses implicit stage per table
            keep_staged_files: Whether to delete or keep staged files after loading
        """
        super().__init__(
            credentials=credentials,
            stage_name=stage_name,
            keep_staged_files=keep_staged_files,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
