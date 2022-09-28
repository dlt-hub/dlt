from typing import Tuple
from google.auth import default as default_credentials
from google.auth.exceptions import DefaultCredentialsError

from dlt.common.typing import StrAny
from dlt.common.configuration import make_configuration, GcpClientCredentials, configspec
from dlt.common.configuration.exceptions import ConfigEntryMissingException

from dlt.load.configuration import LoaderClientDwhConfiguration


@configspec
class BigQueryClientConfiguration(LoaderClientDwhConfiguration):
    client_type: str = "bigquery"


def configuration(initial_values: StrAny = None) -> Tuple[BigQueryClientConfiguration, GcpClientCredentials]:

    def maybe_partial_credentials() -> GcpClientCredentials:
        try:
            return make_configuration(GcpClientCredentials(), initial_value=initial_values)
        except ConfigEntryMissingException as cfex:
            # if config is missing check if credentials can be obtained from defaults
            try:
                _, project_id = default_credentials()
                # if so then return partial so we can access timeouts
                C_PARTIAL = make_configuration(GcpClientCredentials(), initial_value=initial_values, accept_partial = True)
                # set the project id - it needs to be known by the client
                C_PARTIAL.project_id = C_PARTIAL.project_id or project_id
                return C_PARTIAL
            except DefaultCredentialsError:
                raise cfex

    return (
        make_configuration(BigQueryClientConfiguration(), initial_value=initial_values),
        # allow partial credentials so the client can fallback to default credentials
        maybe_partial_credentials()
    )
