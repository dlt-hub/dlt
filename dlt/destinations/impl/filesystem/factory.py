import typing as t

from dlt.destinations.impl.filesystem.configuration import FilesystemDestinationClientConfiguration
from dlt.destinations.impl import filesystem as _filesystem
from dlt.common.configuration import with_config, known_sections
from dlt.common.destination.reference import DestinationClientConfiguration, DestinationFactory
from dlt.common.storages.configuration import FileSystemCredentials


class filesystem(DestinationFactory):

    destination = _filesystem

    @with_config(spec=FilesystemDestinationClientConfiguration, sections=(known_sections.DESTINATION, 'filesystem'), accept_partial=True)
    def __init__(
        self,
        bucket_url: str = None,
        credentials: FileSystemCredentials = None,
        **kwargs: t.Any,
    ) -> None:
        cfg: FilesystemDestinationClientConfiguration = kwargs['_dlt_config']
        self.credentials = cfg.credentials
        self.config_params = {
            "credentials": cfg.credentials,
            "bucket_url": cfg.bucket_url,
        }
