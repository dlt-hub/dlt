import dataclasses
from typing import Any, ClassVar, Dict, Final, List, Optional

from dlt.common.configuration import configspec
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.configuration.specs import AwsCredentials


@configspec
class LakeformationConfig:
    enabled: bool = False
    tags: Optional[Dict[str, str]] = None


@configspec
class AthenaClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="athena", init=False, repr=False, compare=False)  # type: ignore[misc]
    query_result_bucket: str = None
    credentials: AwsCredentials = None
    athena_work_group: Optional[str] = None
    aws_data_catalog: Optional[str] = "awsdatacatalog"
    connection_params: Optional[Dict[str, Any]] = None
    force_iceberg: Optional[bool] = None
    # possible placeholders: {dataset_name}, {table_name}, {location_tag}
    table_location_layout: Optional[str] = "{dataset_name}/{table_name}"
    table_properties: Optional[Dict[str, str]] = None
    lakeformation_config: Optional[LakeformationConfig] = None
    info_tables_query_threshold: int = 90
    # athena slows down when this value is too high, see for context:
    # https://github.com/dlt-hub/dlt/issues/2529
    db_location: Optional[str] = None

    __config_gen_annotations__: ClassVar[List[str]] = [
        "athena_work_group",
        "aws_data_catalog",
        "info_tables_query_threshold",
    ]

    def to_connector_params(self) -> Dict[str, Any]:
        native_credentials = self.credentials.to_native_representation()
        return {
            "s3_staging_dir": self.query_result_bucket,
            "work_group": self.athena_work_group,
            "catalog_name": self.aws_data_catalog,
            **(self.connection_params or {}),
            **native_credentials,
        }

    def __str__(self) -> str:
        """Return displayable destination location"""
        if self.staging_config:
            return f"{self.staging_config} on {self.aws_data_catalog}"
        else:
            return "[no staging set]"
