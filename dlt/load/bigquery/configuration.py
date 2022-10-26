from typing import Optional
from google.auth import default as default_credentials
from google.auth.exceptions import DefaultCredentialsError

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import GcpClientCredentials
from dlt.common.configuration.exceptions import ConfigEntryMissingException
from dlt.common.destination import DestinationClientDwhConfiguration


@configspec(init=True)
class BigQueryClientConfiguration(DestinationClientDwhConfiguration):
    destination_name: str = "bigquery"
    credentials: Optional[GcpClientCredentials] = None

    def check_integrity(self) -> None:
        if not self.credentials.is_resolved():
           # if config is missing check if credentials can be obtained from defaults
            try:
                _, project_id = default_credentials()
                # set the project id - it needs to be known by the client
                self.credentials.project_id = self.credentials.project_id or project_id
            except DefaultCredentialsError:
                print("DefaultCredentialsError")
                # re-raise preventing exception
                raise self.credentials.__exception__
