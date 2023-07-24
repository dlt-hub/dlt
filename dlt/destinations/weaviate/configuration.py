from typing import Dict, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import DestinationClientConfiguration


@configspec
class WeaviateCredentials(CredentialsConfiguration):
    url: str
    api_key: str
    additional_headers: Optional[Dict[str, str]] = None


@configspec(init=True)
class WeaviateClientConfiguration(DestinationClientConfiguration):
    destination_name: str = "weaviate"
    weaviate_batch_size: int = 100

    credentials: WeaviateCredentials
