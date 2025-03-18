import dataclasses
from typing import ClassVar, Final, List, Optional
import warnings

from dlt.common.configuration import configspec
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.warnings import Dlt100DeprecationWarning


@configspec
class AthenaClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="athena", init=False, repr=False, compare=False)  # type: ignore[misc]
    query_result_bucket: str = None
    credentials: AwsCredentials = None
    athena_work_group: Optional[str] = None
    aws_data_catalog: Optional[str] = "awsdatacatalog"
    force_iceberg: Optional[bool] = None

    __config_gen_annotations__: ClassVar[List[str]] = ["athena_work_group"]

    def __str__(self) -> str:
        """Return displayable destination location"""
        if self.staging_config:
            return f"{self.staging_config} on {self.aws_data_catalog}"
        else:
            return "[no staging set]"
