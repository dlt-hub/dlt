import warnings
from typing import TYPE_CHECKING, ClassVar, List, Optional, Final

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.utils import digest128

from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration


@configspec
class BigQueryClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = "bigquery"  # type: ignore
    credentials: GcpServiceAccountCredentials = None
    location: str = "US"

    http_timeout: float = 15.0  # connection timeout for http request to BigQuery api
    file_upload_timeout: float = 30 * 60.0  # a timeout for file upload when loading local files
    retry_deadline: float = (
        60.0  # how long to retry the operation in case of error, the backoff 60s
    )

    __config_gen_annotations__: ClassVar[List[str]] = ["location"]

    def get_location(self) -> str:
        if self.location != "US":
            return self.location
        # default was changed in credentials, emit deprecation message
        if self.credentials.location != "US":
            warnings.warn(
                "Setting BigQuery location in the credentials is deprecated. Please set the"
                " location directly in bigquery section ie. destinations.bigquery.location='EU'"
            )
        return self.credentials.location

    def fingerprint(self) -> str:
        """Returns a fingerprint of project_id"""
        if self.credentials and self.credentials.project_id:
            return digest128(self.credentials.project_id)
        return ""

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            credentials: Optional[GcpServiceAccountCredentials] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None,
            location: str = "US",
            http_timeout: float = 15.0,
            file_upload_timeout: float = 30 * 60.0,
            retry_deadline: float = 60.0,
            destination_name: str = None,
            environment: str = None,
        ) -> None: ...
