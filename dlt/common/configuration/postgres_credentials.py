from dlt.common.typing import StrAny, TSecretValue
from dlt.common.configuration import CredentialsConfiguration


class PostgresCredentials(CredentialsConfiguration):

    __namespace__: str = "PG"

    DBNAME: str = None
    PASSWORD: TSecretValue = None
    USER: str = None
    HOST: str = None
    PORT: int = 5439
    CONNECT_TIMEOUT: int = 15

    @classmethod
    def check_integrity(cls) -> None:
        cls.DBNAME = cls.DBNAME.lower()
        # cls.DEFAULT_DATASET = cls.DEFAULT_DATASET.lower()
        cls.PASSWORD = TSecretValue(cls.PASSWORD.strip())

    @classmethod
    def as_credentials(cls) -> StrAny:
        return cls.as_dict()
