from typing import Any
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import PostgresCredentials

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment


# def test_credentials_native_representation(environ) -> None:
def test_resolved_from_native_representation(environment: Any) -> None:
    # sometimes it is sometimes not try URL without password
    destination_dsn = "postgres://loader@localhost:5432/dlt_data"
    c = PostgresCredentials()
    c.from_native_representation(destination_dsn)
    assert c.is_partial()
    assert not c.is_resolved()

    resolve_configuration(c, accept_partial=True)
    assert c.is_partial()

    environment["CREDENTIALS__PASSWORD"] = "loader"
    resolve_configuration(c, accept_partial=False)

# def test_gcp_credentials_with_default()