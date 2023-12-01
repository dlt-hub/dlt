from typing import ClassVar, Final, List, Optional, TYPE_CHECKING

from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration
from dlt.common.configuration.specs import AwsCredentials


@configspec
class AthenaClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = "athena"  # type: ignore[misc]
    query_result_bucket: str = None
    credentials: AwsCredentials = None
    athena_work_group: Optional[str] = None
    aws_data_catalog: Optional[str] = "awsdatacatalog"
    supports_truncate_command: bool = False
    force_iceberg: Optional[bool] = False

    __config_gen_annotations__: ClassVar[List[str]] = ["athena_work_group"]

    def __str__(self) -> str:
        """Return displayable destination location"""
        if self.staging_config:
            return str(self.staging_config.credentials)
        else:
            return "[no staging set]"

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            credentials: Optional[AwsCredentials] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None,
            athena_work_group: Optional[str] = None,
            aws_data_catalog: Optional[str] = None,
            supports_truncate_command: bool = False,
            force_iceberg: Optional[bool] = False,
            destination_name: str = None,
            environment: str = None,
        ) -> None: ...
