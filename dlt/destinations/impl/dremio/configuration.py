import base64
from typing import Final, Optional, Any, Dict, ClassVar, List, TYPE_CHECKING

from sqlalchemy.engine import URL

from dlt import version
from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue
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
            "DremioCredentials with private key",
            dependencies=[f"{version.DLT_PKG_NAME}[dremio]"],
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


@configspec
class DremioCredentials(ConnectionStringCredentials):
    drivername: Final[str] = "grpc"  # type: ignore[misc]
    username: str = None
    password: Optional[TSecretStrValue] = None
    host: str = None
    port: Optional[int] = 32010
    database: str = None
    warehouse: Optional[str] = None

    __config_gen_annotations__: ClassVar[List[str]] = ["password", "warehouse", "role"]

    def to_url(self) -> URL:
        return URL.create(drivername=self.drivername, host=self.host, port=self.port)

    def db_kwargs(self) -> Dict[str, Any]:
        return dict(username=self.username, password=self.password)


@configspec
class DremioClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = "dremio"  # type: ignore[misc]
    credentials: DremioCredentials

    stage_name: Optional[str] = None
    """Use an existing named stage instead of the default. Default uses the implicit table stage per table"""
    keep_staged_files: bool = True
    """Whether to keep or delete the staged files after COPY INTO succeeds"""

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            destination_type: str = None,
            credentials: DremioCredentials = None,
            dataset_name: str = None,
            default_schema_name: str = None,
            stage_name: str = None,
            keep_staged_files: bool = True,
            destination_name: str = None,
            environment: str = None,
        ) -> None: ...
