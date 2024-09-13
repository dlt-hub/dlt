import os
import posixpath
import tempfile
from contextlib import contextmanager
from typing import Iterator
from typing import List, Sequence, Tuple

import pytest
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.destination.reference import RunnableLoadJob
from dlt.common.pendulum import timedelta, __utcnow
from dlt.destinations import filesystem
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.destinations.job_impl import FinalizedLoadJobWithFollowupJobs
from dlt.load import Load
from tests.load.utils import prepare_load_package


def setup_loader(dataset_name: str) -> Load:
    destination = filesystem()
    config = destination.spec()._bind_dataset_name(dataset_name=dataset_name)
    # setup loader
    with Container().injectable_context(ConfigSectionContext(sections=("filesystem",))):
        return Load(destination, initial_client_config=config)  # type: ignore[arg-type]


@contextmanager
def perform_load(
    dataset_name: str, cases: Sequence[str], write_disposition: str = "append"
) -> Iterator[Tuple[FilesystemClient, List[RunnableLoadJob], str, str]]:
    load = setup_loader(dataset_name)
    load_id, schema = prepare_load_package(load.load_storage, cases, write_disposition)
    client: FilesystemClient = load.get_destination_client(schema)  # type: ignore[assignment]

    # for the replace disposition in the loader, we truncate the tables, so do this here
    truncate_tables = []
    if write_disposition == "replace":
        for item in cases:
            parts = item.split(".")
            truncate_tables.append(parts[0])

    client.initialize_storage(truncate_tables=truncate_tables)
    client.update_stored_schema()
    root_path = posixpath.join(client.bucket_path, client.config.dataset_name)

    files = load.load_storage.list_new_jobs(load_id)
    try:
        jobs = []
        for f in files:
            job = load.submit_job(f, load_id, schema)
            # job execution failed
            if isinstance(job, FinalizedLoadJobWithFollowupJobs):
                raise RuntimeError(job.exception())
            jobs.append(job)

        yield client, jobs, root_path, load_id  # type: ignore
    finally:
        try:
            client.drop_storage()
        except Exception:
            print(f"Failed to delete FILESYSTEM dataset: {client.dataset_path}")


@pytest.fixture(scope="function", autouse=False)
def self_signed_cert() -> Iterator[str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())

    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "DE"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Berlin"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "Prenzlauer Berg"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ScaleVector GmbH"),
            x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
        ]
    )
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(__utcnow())
        .not_valid_after(__utcnow() + timedelta(days=1))
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
