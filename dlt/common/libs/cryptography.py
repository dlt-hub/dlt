import re
import os
import base64
from typing import Any, Optional, cast

from dlt.common.exceptions import MissingDependencyException


PEM_REGEX = re.compile(
    r"-----BEGIN ([A-Z ]+)-----\s+([A-Za-z0-9+/=\s]+)-----END \1-----", re.MULTILINE
)


def _import_fernet() -> Any:
    try:
        from cryptography.fernet import Fernet

        return Fernet
    except ModuleNotFoundError as e:
        raise MissingDependencyException(
            "symmetric encryption of secrets",
            dependencies=["cryptography"],
        ) from e


def generate_secret() -> str:
    """Generates a random url-safe secret with high entropy."""
    return base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")


def derive_encryption_key(secret: str, purpose: str) -> str:
    """Derives a url-safe Fernet key from `secret` for `purpose` with HKDF-SHA256."""

    try:
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
        from cryptography.hazmat.primitives import hashes
    except ModuleNotFoundError as e:
        raise MissingDependencyException(
            "symmetric encryption of secrets",
            dependencies=["cryptography"],
        ) from e

    key = HKDF(
        algorithm=hashes.SHA256(), length=32, salt=None, info=purpose.encode("utf-8")
    ).derive(secret.encode("utf-8"))
    return base64.urlsafe_b64encode(key).decode("ascii")


def encrypt_text(key: str, text: str) -> str:
    """Encrypts `text` with a Fernet `key`. Returns a url-safe token."""
    token = _import_fernet()(key.encode("ascii")).encrypt(text.encode("utf-8"))
    return cast(str, token.decode("ascii"))


def decrypt_text(key: str, token: str) -> str:
    """Decrypts a Fernet `token` with `key`. Raises `ValueError` when the key does not match."""
    fernet = _import_fernet()
    try:
        return cast(str, fernet(key.encode("ascii")).decrypt(token.encode("ascii")).decode("utf-8"))
    except Exception as e:
        raise ValueError("dlt cannot decrypt the token. The encryption key does not match.") from e


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
