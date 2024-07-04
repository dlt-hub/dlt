import os
import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.exceptions import SystemConfigurationException
from dlt.common.schema import Schema

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


def test_parse_native_representation() -> None:
    # Case: unsupported driver specified.
    with pytest.raises(SystemConfigurationException):
        resolve_configuration(
            SynapseCredentials(
                "synapse://test_user:test_pwd@test.sql.azuresynapse.net/test_db?DRIVER=ODBC+Driver+17+for+SQL+Server"
            )
        )


def test_to_odbc_dsn_longasmax() -> None:
    # Case: LONGASMAX not specified in query (this is the expected scenario).
    creds = resolve_configuration(
        SynapseCredentials(
            "synapse://test_user:test_pwd@test.sql.azuresynapse.net/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result["LONGASMAX"] == "yes"

    # Case: LONGASMAX specified in query; specified value should be overridden.
    creds = resolve_configuration(
        SynapseCredentials(
            "synapse://test_user:test_pwd@test.sql.azuresynapse.net/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server&LONGASMAX=no"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result["LONGASMAX"] == "yes"
