import dataclasses
from pathlib import Path
from typing import Final, Optional, Any, Dict, ClassVar, List

from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.libs.cryptography import decode_private_key
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration import configspec
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.utils import digest128


SNOWFLAKE_APPLICATION_ID = "dltHub_dlt"


@configspec(init=False)
class SnowflakeCredentials(ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(default="snowflake", init=False, repr=False, compare=False)  # type: ignore[misc]
    host: str = None
    database: str = None
    username: str = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    authenticator: Optional[str] = None
    token: Optional[str] = None
    private_key: Optional[TSecretStrValue] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[TSecretStrValue] = None
    application: Optional[str] = SNOWFLAKE_APPLICATION_ID

    __config_gen_annotations__: ClassVar[List[str]] = ["password", "warehouse", "role"]
    __query_params__: ClassVar[List[str]] = [
        "warehouse",
        "role",
        "authenticator",
        "token",
        "private_key",
        "private_key_path",
        "private_key_passphrase",
    ]

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        for param in self.__query_params__:
            if param in self.query:
                setattr(self, param, self.query.get(param))

    def on_resolved(self) -> None:
        if self.private_key_path:
            try:
                self.private_key = Path(self.private_key_path).read_text("ascii")
            except Exception:
                raise ValueError(
                    "Make sure that `private_key` in dlt recognized format is at"
                    f" `{self.private_key_path}`. Note that binary formats are not supported"
                )
        if not self.password and not self.private_key and not self.authenticator:
            raise ConfigurationValueError(
                "`SnowflakeCredentials` requires one of the following to be specified: `password`,"
                " `private_key`, `authenticator` (OAuth2)."
            )

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query() or {})
        for param in self.__query_params__:
            if self.get(param, None) is not None:
                query[param] = self[param]
        return query

    def to_connector_params(self) -> Dict[str, Any]:
        # gather all params in query
        query = self.get_query()
        if self.private_key:
            query["private_key"] = decode_private_key(self.private_key, self.private_key_passphrase)

        # we do not want passphrase to be passed
        query.pop("private_key_passphrase", None)

        conn_params: Dict[str, Any] = dict(
            query,
            user=self.username,
            password=self.password,
            account=self.host,
            database=self.database,
        )

        if self.application != "" and "application" not in conn_params:
            conn_params["application"] = self.application

        return conn_params


@configspec
class SnowflakeClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="snowflake", init=False, repr=False, compare=False)  # type: ignore[misc]
    credentials: SnowflakeCredentials = None

    stage_name: Optional[str] = None
    """Use an existing named stage instead of the default. Default uses the implicit table stage per table"""
    keep_staged_files: bool = True
    """Whether to keep or delete the staged files after COPY INTO succeeds"""

    csv_format: Optional[CsvFormatConfiguration] = None
    """Optional csv format configuration"""

    query_tag: Optional[str] = None
    """A tag with placeholders to tag sessions executing jobs"""

    create_indexes: bool = False
    """Whether UNIQUE or PRIMARY KEY constrains should be created"""

    use_vectorized_scanner: bool = False
    """Whether to use or not use the vectorized scanner in COPY INTO"""

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""
