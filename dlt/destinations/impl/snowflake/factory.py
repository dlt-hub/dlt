import typing as t

from dlt.destinations.impl.snowflake.configuration import SnowflakeCredentials, SnowflakeClientConfiguration
from dlt.destinations.impl.snowflake import capabilities
from dlt.common.configuration import with_config, known_sections
from dlt.common.destination.reference import DestinationClientConfiguration, Destination
from dlt.common.destination import DestinationCapabilitiesContext

if t.TYPE_CHECKING:
    from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient


class snowflake(Destination):

    capabilities = capabilities()
    spec = SnowflakeClientConfiguration

    @property
    def client_class(self) -> t.Type["SnowflakeClient"]:
        from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient

        return SnowflakeClient

    @with_config(spec=SnowflakeClientConfiguration, sections=(known_sections.DESTINATION, 'snowflake'), accept_partial=True)
    def __init__(
        self,
        credentials: SnowflakeCredentials = None,
        stage_name: t.Optional[str] = None,
        keep_staged_files: bool = True,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(kwargs['_dlt_config'])
