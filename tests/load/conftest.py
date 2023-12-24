import datetime
import os
import tempfile
from typing import Iterator

import pytest
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from tests.load.utils import DEFAULT_BUCKETS, ALL_BUCKETS


@pytest.fixture(scope="function", params=DEFAULT_BUCKETS)
def default_buckets_env(request) -> Iterator[str]:
    """Parametrized fixture to configure filesystem destination bucket in env for each test bucket"""
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = request.param
    yield request.param


@pytest.fixture(scope="function", params=ALL_BUCKETS)
def all_buckets_env(request) -> Iterator[str]:
    if isinstance(request.param, dict):
        bucket_url = request.param["bucket_url"]
        # R2 bucket needs to override all credentials
        for key, value in request.param["credentials"].items():
            os.environ[f"DESTINATION__FILESYSTEM__CREDENTIALS__{key.upper()}"] = value
    else:
        bucket_url = request.param
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = bucket_url
    yield bucket_url


@pytest.fixture(scope="function")
def self_signed_cert() -> Iterator[str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "DE"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Berlin"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "Britz"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "dltHub"),
        x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
    ])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost")]),
            critical=False,
        )
        .sign(key, hashes.SHA256(), default_backend())
    )

    with tempfile.NamedTemporaryFile(suffix=".crt", delete=False) as cert_file:
        cert_file.write(cert.public_bytes(serialization.Encoding.PEM))
        cert_path = cert_file.name

    yield cert_path

    os.remove(cert_path)
