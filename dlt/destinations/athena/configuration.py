from typing import Final, Optional
from dlt.common.typing import TSecretStrValue

from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.configuration.specs import  AwsCredentials


@configspec(init=True)
class AthenaClientConfiguration(DestinationClientDwhConfiguration):
    destination_name: Final[str] = "athena"  # type: ignore[misc]
    query_result_bucket: str = None
    credentials: AwsCredentials = None
    workgroup: Optional[str] = None