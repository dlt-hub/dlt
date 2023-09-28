from typing import ClassVar, Final, List, Optional

from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration
from dlt.common.configuration.specs import  AwsCredentials


@configspec
class AthenaClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_name: Final[str] = "athena"  # type: ignore[misc]
    query_result_bucket: str = None
    credentials: AwsCredentials = None
    athena_work_group: Optional[str] = None
    aws_data_catalog: Optional[str] = "awsdatacatalog"

    __config_gen_annotations__: ClassVar[List[str]] = ["athena_work_group"]

    def __str__(self) -> str:
        """Return displayable destination location"""
        if self.staging_config:
            return str(self.staging_config.credentials)
        else:
            return "[no staging set]"
