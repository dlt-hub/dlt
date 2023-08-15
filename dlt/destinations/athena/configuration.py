from typing import ClassVar, Final, List, Optional

from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.configuration.specs import  AwsCredentials


@configspec
class AthenaClientConfiguration(DestinationClientDwhConfiguration):
    destination_name: Final[str] = "athena"  # type: ignore[misc]
    query_result_bucket: str = None
    credentials: AwsCredentials = None
    workgroup: Optional[str] = None

    __config_gen_annotations__: ClassVar[List[str]] = ["workgroup"]