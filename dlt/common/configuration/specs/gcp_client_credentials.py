from typing import Any
from dlt.common import json

from dlt.common.typing import StrAny, TSecretValue
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec


@configspec
class GcpClientCredentials(CredentialsConfiguration):

    __namespace__: str = "gcp"

    project_id: str = None
    type: str = "service_account"  # noqa: A003
    private_key: TSecretValue = None
    location: str = "US"
    token_uri: str = "https://oauth2.googleapis.com/token"
    client_email: str = None

    http_timeout: float = 15.0
    file_upload_timeout: float = 30 * 60.0
    retry_deadline: float = 600  # how long to retry the operation in case of error, the backoff 60s

    def from_native_representation(self, initial_value: Any) -> None:
        if not isinstance(initial_value, str):
            raise ValueError(initial_value)
        try:
            service_dict = json.loads(initial_value)
            self.update(service_dict)
        except Exception:
            raise ValueError(initial_value)

    def check_integrity(self) -> None:
        if self.private_key and self.private_key[-1] != "\n":
            # must end with new line, otherwise won't be parsed by Crypto
            self.private_key = TSecretValue(self.private_key + "\n")

    def to_native_representation(self) -> StrAny:
        return {
                "type": self.type,
                "project_id": self.project_id,
                "private_key": self.private_key,
                "token_uri": self.token_uri,
                "client_email": self.client_email
            }