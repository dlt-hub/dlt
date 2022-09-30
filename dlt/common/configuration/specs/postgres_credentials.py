from typing import Any

from dlt.common.typing import StrAny, TSecretValue
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec


@configspec
class PostgresCredentials(CredentialsConfiguration):

    __namespace__: str = "pg"

    dbname: str = None
    password: TSecretValue = None
    user: str = None
    host: str = None
    port: int = 5439
    connect_timeout: int = 15

    def from_native_repesentation(self, initial_value: Any) -> None:
        if not isinstance(initial_value, str):
            raise ValueError(initial_value)
        # TODO: parse postgres connection string
        raise NotImplementedError()

    def check_integrity(self) -> None:
        self.dbname = self.dbname.lower()
        self.password = TSecretValue(self.password.strip())

    def to_native_representation(self) -> StrAny:
        raise NotImplementedError()
