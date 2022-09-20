from dlt.common.typing import StrAny, TSecretValue
from dlt.common.configuration import CredentialsConfiguration


class GcpClientCredentials(CredentialsConfiguration):

    __namespace__: str = "GCP"

    PROJECT_ID: str = None
    CRED_TYPE: str = "service_account"
    PRIVATE_KEY: TSecretValue = None
    LOCATION: str = "US"
    TOKEN_URI: str = "https://oauth2.googleapis.com/token"
    CLIENT_EMAIL: str = None

    HTTP_TIMEOUT: float = 15.0
    RETRY_DEADLINE: float = 600

    @classmethod
    def check_integrity(cls) -> None:
        if cls.PRIVATE_KEY and cls.PRIVATE_KEY[-1] != "\n":
            # must end with new line, otherwise won't be parsed by Crypto
            cls.PRIVATE_KEY = TSecretValue(cls.PRIVATE_KEY + "\n")

    @classmethod
    def as_credentials(cls) -> StrAny:
        return {
                "type": cls.CRED_TYPE,
                "project_id": cls.PROJECT_ID,
                "private_key": cls.PRIVATE_KEY,
                "token_uri": cls.TOKEN_URI,
                "client_email": cls.CLIENT_EMAIL
            }