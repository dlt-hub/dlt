from dlt.common.configuration.utils import TConfigSecret


class PostgresConfiguration:
    PG_DATABASE_NAME: str = None
    PG_SCHEMA_PREFIX: str = None
    PG_PASSWORD: TConfigSecret = None
    PG_USER: str = None
    PG_HOST: str = None
    PG_PORT: int = 5439
    PG_CONNECTION_TIMEOUT: int = 15

    @classmethod
    def check_integrity(cls) -> None:
        cls.PG_DATABASE_NAME = cls.PG_DATABASE_NAME.lower()
        cls.PG_SCHEMA_PREFIX = cls.PG_SCHEMA_PREFIX.lower()
        cls.PG_PASSWORD = TConfigSecret(cls.PG_PASSWORD.strip())


class PostgresProductionConfiguration(PostgresConfiguration):
    PG_DATABASE_NAME: str = None
    PG_SCHEMA_PREFIX: str = None
    PG_PASSWORD: TConfigSecret = None
    PG_USER: str = None
    PG_HOST: str = None
