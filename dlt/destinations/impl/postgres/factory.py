import typing as t

from dlt.common.configuration import with_config, known_sections
from dlt.common.destination.reference import DestinationClientConfiguration, DestinationFactory

from dlt.destinations.impl.postgres.configuration import PostgresCredentials, PostgresClientConfiguration
from dlt.destinations.impl import postgres as _postgres


class postgres(DestinationFactory):

    destination = _postgres

    @with_config(spec=PostgresClientConfiguration, sections=(known_sections.DESTINATION, 'postgres'), accept_partial=True)
    def __init__(
        self,
        credentials: PostgresCredentials = None,
        create_indexes: bool = True,
        **kwargs: t.Any,
    ) -> None:
        cfg: PostgresClientConfiguration = kwargs['_dlt_config']
        self.credentials = cfg.credentials
        self.config_params = {
            "created_indexes": cfg.create_indexes,
        }
