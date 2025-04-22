import dataclasses
import warnings
from typing import ClassVar, List, Final, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.utils import digest128

from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration


@configspec
class BigQueryClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="bigquery", init=False, repr=False, compare=False)  # type: ignore
    credentials: GcpServiceAccountCredentials = None
    location: str = "US"
    project_id: Optional[str] = None
    """Note, that this is BigQuery project_id which could be different from credentials.project_id"""
    has_case_sensitive_identifiers: bool = True
    """If True then dlt expects to load data into case sensitive dataset"""
    should_set_case_sensitivity_on_new_dataset: bool = False
    """If True, dlt will set case sensitivity flag on created datasets that corresponds to naming convention"""

    http_timeout: float = 15.0
    """connection timeout for http request to BigQuery api"""
    file_upload_timeout: float = 30 * 60.0
    """a timeout for file upload when loading local files"""
    retry_deadline: float = 60.0
    """How long to retry the operation in case of error, the backoff 60 s."""
    batch_size: int = 500
    """Number of rows in streaming insert batch"""
    autodetect_schema: bool = False
    """Allow BigQuery to autodetect schemas and create data tables"""
    ignore_unknown_values: bool = False
    """Ignore unknown values in the data"""

    __config_gen_annotations__: ClassVar[List[str]] = ["location"]

    def get_location(self) -> str:
        return self.location

    def fingerprint(self) -> str:
        """Returns a fingerprint of project_id"""
        if self.credentials and self.credentials.project_id:
            return digest128(self.credentials.project_id)
        return ""
