import typing as t

from dlt.destinations.impl.snowflake.configuration import SnowflakeCredentials, SnowflakeClientConfiguration
from dlt.destinations.impl import snowflake as _snowflake
from dlt.common.configuration import with_config, known_sections
from dlt.common.destination.reference import DestinationClientConfiguration, DestinationFactory


class snowflake(DestinationFactory):

    destination = _snowflake

    @with_config(spec=SnowflakeClientConfiguration, sections=(known_sections.DESTINATION, 'snowflake'), accept_partial=True)
    def __init__(
        self,
        credentials: SnowflakeCredentials = None,
        stage_name: t.Optional[str] = None,
        keep_staged_files: bool = True,
        **kwargs: t.Any,
    ) -> None:
        cfg: SnowflakeClientConfiguration = kwargs['_dlt_config']
        self.credentials = cfg.credentials
        self.config_params = {
            "stage_name": cfg.stage_name,
            "keep_staged_files": cfg.keep_staged_files,
        }
