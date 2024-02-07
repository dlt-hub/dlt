import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.exceptions import SystemConfigurationException

from dlt.destinations.impl.synapse.configuration import (
    SynapseClientConfiguration,
    SynapseCredentials,
)


def test_synapse_configuration() -> None:
    # By default, unique indexes should not be created.
    assert SynapseClientConfiguration().create_indexes is False


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
