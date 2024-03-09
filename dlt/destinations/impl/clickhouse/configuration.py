from typing import TYPE_CHECKING, ClassVar, List, Optional, Final

from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration


@configspec
class ClickhouseClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = "clickhouse"  # type: ignore

    http_timeout: float = 15.0
    file_upload_timeout: float = 30 * 60.0
    retry_deadline: float = 60.0

    __config_gen_annotations__: ClassVar[List[str]] = []

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            dataset_name: str = None,
            default_schema_name: Optional[str],
            http_timeout: float = 15.0,
            file_upload_timeout: float = 30 * 60.0,
            retry_deadline: float = 60.0,
            destination_name: str = None,
            environment: str = None
        ) -> None:
            super().__init__(
                dataset_name=dataset_name,
                default_schema_name=default_schema_name,
                destination_name=destination_name,
                environment=environment,
            )
            self.retry_deadline = retry_deadline
            self.file_upload_timeout = file_upload_timeout
            self.http_timeout = http_timeout
            ...
