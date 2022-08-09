from dlt.common.configuration import BaseConfiguration
from dlt.common.configuration.utils import TSecretValue
from dlt.common.typing import StrAny


class PostgresCredentials(BaseConfiguration):
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
