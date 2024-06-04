import dataclasses
import base64
from typing import Final, Optional, Any, Dict, ClassVar, List, TYPE_CHECKING, Union

from dlt import version
from dlt.common.libs.sql_alchemy import URL
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration
from dlt.common.utils import digest128


def _read_private_key(private_key: str, password: Optional[str] = None) -> bytes:
    """Load an encrypted or unencrypted private key from string."""
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives.asymmetric import dsa
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
    except ModuleNotFoundError as e:
        raise MissingDependencyException(
            "SnowflakeCredentials with private key",
            dependencies=[f"{version.DLT_PKG_NAME}[snowflake]"],
        ) from e

    try:
        # load key from base64-encoded DER key
        pkey = serialization.load_der_private_key(
            base64.b64decode(private_key),
            password=password.encode() if password is not None else None,
            backend=default_backend(),
        )
    except Exception:
        # loading base64-encoded DER key failed -> assume it's a plain-text PEM key
        pkey = serialization.load_pem_private_key(
            private_key.encode(encoding="ascii"),
            password=password.encode() if password is not None else None,
            backend=default_backend(),
        )

    return pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


SNOWFLAKE_APPLICATION_ID = "dltHub_dlt"


@configspec(init=False)
class SnowflakeCredentials(ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(default="snowflake", init=False, repr=False, compare=False)  # type: ignore[misc]
    password: Optional[TSecretStrValue] = None
    host: str = None
    database: str = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    authenticator: Optional[str] = None
    private_key: Optional[TSecretStrValue] = None
    private_key_passphrase: Optional[TSecretStrValue] = None
    application: Optional[str] = SNOWFLAKE_APPLICATION_ID

    __config_gen_annotations__: ClassVar[List[str]] = ["password", "warehouse", "role"]

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        self.warehouse = self.query.get("warehouse")
        self.role = self.query.get("role")
        self.private_key = self.query.get("private_key")  # type: ignore
        self.private_key_passphrase = self.query.get("private_key_passphrase")  # type: ignore
        if not self.is_partial() and (self.password or self.private_key):
            self.resolve()

    def on_resolved(self) -> None:
        if not self.password and not self.private_key:
            raise ConfigurationValueError(
                "Please specify password or private_key. SnowflakeCredentials supports password and"
                " private key authentication and one of those must be specified."
            )

    def to_url(self) -> URL:
        query = dict(self.query or {})
        if self.warehouse and "warehouse" not in query:
            query["warehouse"] = self.warehouse
        if self.role and "role" not in query:
            query["role"] = self.role

        if self.application != "" and "application" not in query:
            query["application"] = self.application

        return URL.create(
            self.drivername,
            self.username,
            self.password,
            self.host,
            self.port,
            self.database,
            query,
        )

    def to_connector_params(self) -> Dict[str, Any]:
        private_key: Optional[bytes] = None
        if self.private_key:
            private_key = _read_private_key(self.private_key, self.private_key_passphrase)

        conn_params = dict(
            self.query or {},
            user=self.username,
            password=self.password,
            account=self.host,
            database=self.database,
            warehouse=self.warehouse,
            role=self.role,
            private_key=private_key,
        )

        if self.authenticator:
            conn_params["authenticator"] = self.authenticator

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

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""
