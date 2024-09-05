import pytest

import sqlalchemy as sa

from dlt.common.configuration import resolve_configuration
from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyClientConfiguration,
    SqlalchemyCredentials,
)


def test_sqlalchemy_credentials_from_engine() -> None:
    engine = sa.create_engine("sqlite:///:memory:")

    creds = resolve_configuration(SqlalchemyCredentials(engine))

    # Url is taken from engine
    assert creds.to_url() == sa.engine.make_url("sqlite:///:memory:")
    # Engine is stored on the instance
    assert creds.engine is engine

    assert creds.drivername == "sqlite"
    assert creds.database == ":memory:"
