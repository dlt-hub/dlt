import os

import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.schema import Schema
from dlt.common.utils import digest128
from dlt.destinations import synapse
from dlt.destinations.impl.synapse.configuration import (
    SynapseClientConfiguration,
    SynapseCredentials,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_synapse_configuration() -> None:
    # By default, unique indexes should not be created.
    c = SynapseClientConfiguration()
    assert c.create_indexes is False
    assert c.has_case_sensitive_identifiers is False
    assert c.staging_use_msi is False


@pytest.mark.parametrize(
    "connection_string,expected_fingerprint",
    [
        pytest.param("", "", id="empty"),
        pytest.param(
            "synapse://user1:pass1@host1:1433/db1",
            digest128("host1"),
            id="legacy_host_only_default_port",
        ),
        pytest.param(
            "synapse://user1:pass1@host1:1434/db1",
            digest128("host1"),
            id="legacy_host_only_custom_port",
        ),
    ],
)
def test_synapse_fingerprint(connection_string: str, expected_fingerprint: str) -> None:
    if connection_string:
        credentials = SynapseCredentials(connection_string)
        config = SynapseClientConfiguration(credentials=credentials)
    else:
        config = SynapseClientConfiguration()

    assert config.fingerprint() == expected_fingerprint


def test_synapse_factory() -> None:
    schema = Schema("schema")
    dest = synapse()
    client = dest.client(schema, SynapseClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is False
    assert client.config.staging_use_msi is False
    assert client.config.has_case_sensitive_identifiers is False
    assert client.capabilities.has_case_sensitive_identifiers is False
    assert client.capabilities.casefold_identifier is str

    # set args explicitly
    dest = synapse(has_case_sensitive_identifiers=True, create_indexes=True, staging_use_msi=True)
    client = dest.client(schema, SynapseClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is True
    assert client.config.staging_use_msi is True
    assert client.config.has_case_sensitive_identifiers is True
    assert client.capabilities.has_case_sensitive_identifiers is True
    assert client.capabilities.casefold_identifier is str

    # set args via config
    os.environ["DESTINATION__CREATE_INDEXES"] = "True"
    os.environ["DESTINATION__STAGING_USE_MSI"] = "True"
    os.environ["DESTINATION__HAS_CASE_SENSITIVE_IDENTIFIERS"] = "True"
    dest = synapse()
    client = dest.client(schema, SynapseClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is True
    assert client.config.staging_use_msi is True
    assert client.config.has_case_sensitive_identifiers is True
    assert client.capabilities.has_case_sensitive_identifiers is True
    assert client.capabilities.casefold_identifier is str


def test_driver_query_parameter_is_ignored() -> None:
    # mssql-python bundles its own driver, so a legacy `driver` query parameter is ignored
    # and the DSN carries no DRIVER key.
    creds = resolve_configuration(
        SynapseCredentials(
            "synapse://test_user:test_pwd@test.sql.azuresynapse.net/test_db?DRIVER=ODBC+Driver+17+for+SQL+Server"
        )
    )
    assert "DRIVER=" not in creds.to_odbc_dsn()


def test_to_odbc_dsn_longasmax_absent() -> None:
    # The mssql-python driver handles long/max types natively, so LONGASMAX must never
    # appear in the DSN, regardless of what the user passes in the query.
    creds = resolve_configuration(
        SynapseCredentials(
            "synapse://test_user:test_pwd@test.sql.azuresynapse.net/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert "LONGASMAX" not in result

    # Case: LongAsMax specified in query; it is still dropped from the DSN.
    creds = resolve_configuration(
        SynapseCredentials(
            "synapse://test_user:test_pwd@test.sql.azuresynapse.net/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server&LongAsMax=yes"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert "LONGASMAX" not in result
