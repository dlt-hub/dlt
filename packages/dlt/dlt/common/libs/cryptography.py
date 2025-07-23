import re
import base64
from typing import Optional

from dlt.common.exceptions import MissingDependencyException


PEM_REGEX = re.compile(
    r"-----BEGIN ([A-Z ]+)-----\s+([A-Za-z0-9+/=\s]+)-----END \1-----", re.MULTILINE
)


def is_pem(data: str) -> bool:
    return PEM_REGEX.search(data) is not None


def decode_private_key(private_key: str, password: Optional[str] = None) -> bytes:
    """Decode encrypted or unencrypted private key from string. Supported formats:
    1. base64 encoded DER
    2. plain-text or base64 encoded PEM
    """
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives.asymmetric import dsa
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
    except ModuleNotFoundError as e:
        raise MissingDependencyException(
            "public / private key authentication",
            dependencies=["cryptography"],
        ) from e

    # check if this is PEM
    if is_pem(private_key):
        private_key_blob = private_key.encode(encoding="ascii")
    else:
        try:
            private_key_blob = base64.b64decode(private_key, validate=True)
        except Exception as der_exc:
            raise ValueError(
                "Could not decode private key for key pair authentication. Following formats were"
                f" attempted:\n1. base64 encoded DER or PEM (error: `{der_exc}`)\n2. plain-text PEM"
                " (error: BEGIN and END markers not found)\nIf you are using connection string and"
                " are passing DER/PEM or password in query string, make sure you url-encode them."
            )

    try:
        # load key as binary DER
        pkey = serialization.load_der_private_key(
            private_key_blob,
            password=password.encode() if password is not None else None,
            backend=default_backend(),
        )
    except Exception as der_exc:
        # loading DER key failed -> assume it's a plain-text PEM key
        try:
            pkey = serialization.load_pem_private_key(
                private_key_blob,
                password=password.encode() if password is not None else None,
                backend=default_backend(),
            )
        except Exception as pem_exc:
            raise ValueError(
                "Could not decode private key for key pair authentication. Following formats were"
                f" attempted:\n1. base64 encoded DER (error: `{der_exc}`)\n2. plain-text or base64"
                f" encoded PEM (error: `{pem_exc}`)\nIf you are using connection string and are"
                " passing DER/PEM or password in query string, make sure you url-encode them."
            )

    return pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
