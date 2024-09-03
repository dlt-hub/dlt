import dataclasses
from typing import ClassVar, Final, List, Optional
import warnings

from dlt.common import logger
from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration
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

    def on_resolved(self) -> None:
        if self.force_iceberg is not None:
            warnings.warn(
                "The `force_iceberg` is deprecated.If you upgraded dlt on existing pipeline and you"
                " have data already loaded, please keep this flag to make sure your data is"
                " consistent.If you are creating a new dataset and no data was loaded, please set"
                " `table_format='iceberg`` on your resources explicitly.",
                Dlt100DeprecationWarning,
                stacklevel=1,
            )

    def __str__(self) -> str:
        """Return displayable destination location"""
        if self.staging_config:
            return f"{self.staging_config} on {self.aws_data_catalog}"
        else:
            return "[no staging set]"
