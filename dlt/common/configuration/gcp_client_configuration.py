from dlt.common.typing import StrStr
from dlt.common.configuration.utils import TConfigSecret

class GcpClientConfiguration:
    PROJECT_ID: str = None
    DATASET: str = None
    TIMEOUT: float = 30.0
    BQ_CRED_TYPE: str = "service_account"
    BQ_CRED_PRIVATE_KEY: TConfigSecret = None
    BQ_CRED_TOKEN_URI: str = "https://oauth2.googleapis.com/token"
    BQ_CRED_CLIENT_EMAIL: str = None

    @classmethod
    def check_integrity(cls) -> None:
        if cls.BQ_CRED_PRIVATE_KEY and cls.BQ_CRED_PRIVATE_KEY[-1] != "\n":
            # must end with new line, otherwise won't be parsed by Crypto
            cls.BQ_CRED_PRIVATE_KEY = TConfigSecret(cls.BQ_CRED_PRIVATE_KEY + "\n")

    @classmethod
    def to_service_credentials(cls) -> StrStr:
        return {
                "type": cls.BQ_CRED_TYPE,
                "project_id": cls.PROJECT_ID,
                "private_key": cls.BQ_CRED_PRIVATE_KEY,
                "token_uri": cls.BQ_CRED_TOKEN_URI,
                "client_email": cls.BQ_CRED_CLIENT_EMAIL
            }


class GcpClientProductionConfiguration(GcpClientConfiguration):
    PROJECT_ID: str = None
    DATASET: str = None
    BQ_CRED_PRIVATE_KEY: TConfigSecret = None
    BQ_CRED_CLIENT_EMAIL: str = None
