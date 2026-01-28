import os

import pytest

import dlt
import sqlalchemy as sa

from dlt.common.configuration import resolve_configuration
from dlt.common.known_env import DLT_LOCAL_DIR
from dlt.common.utils import uniq_id
from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyClientConfiguration,
    SqlalchemyCredentials,
)

from tests.utils import get_test_storage_root


def test_sqlalchemy_credentials_from_engine() -> None:
    engine = sa.create_engine("sqlite:///:memory:")

    creds = resolve_configuration(SqlalchemyCredentials(engine))

    # Url is taken from engine
    assert creds.to_url() == sa.engine.make_url("sqlite:///:memory:")
    # Engine is stored on the instance
    assert creds.engine is engine

    assert creds.drivername == "sqlite"
    assert creds.database == ":memory:"


def test_sqlalchemy_sqlite_follows_local_dir() -> None:
    local_dir = os.path.join(get_test_storage_root(), uniq_id())
    os.makedirs(local_dir)
    os.environ[DLT_LOCAL_DIR] = local_dir

    # default case: no explicit database, uses destination_type as default name
    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials("sqlite:///")
        )._bind_dataset_name(dataset_name="test_dataset")
    )
    db_path = os.path.join(local_dir, "sqlalchemy.db")
    assert c.credentials.database == os.path.abspath(db_path)

    # named destination: uses destination_name for the filename
    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials("sqlite:///"),
            destination_name="named",
        )._bind_dataset_name(dataset_name="test_dataset")
    )
    db_path = os.path.join(local_dir, "named.db")
    assert c.credentials.database == os.path.abspath(db_path)

    # explicit relative location: relocated to local_dir
    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials("sqlite:///./local.db"),
        )._bind_dataset_name(dataset_name="test_dataset")
    )
    db_path = os.path.join(local_dir, "local.db")
    assert c.credentials.database.endswith(db_path)

    # pipeline context: uses pipeline_name for the filename
    pipeline = dlt.pipeline("test_sqlalchemy_sqlite_follows_local_dir")
    c = resolve_configuration(
        pipeline._bind_local_files(
            SqlalchemyClientConfiguration(
                credentials=SqlalchemyCredentials("sqlite:///"),
            )._bind_dataset_name(dataset_name="test_dataset")
        )
    )
    db_path = os.path.join(local_dir, "test_sqlalchemy_sqlite_follows_local_dir.db")
    assert c.credentials.database.endswith(db_path)

    # absolute path: preserved as-is
    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials("sqlite:////absolute/path/test.db"),
        )._bind_dataset_name(dataset_name="test_dataset")
    )
    assert c.credentials.database == "/absolute/path/test.db"

    # memory database: preserved as-is
    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials("sqlite:///:memory:"),
        )._bind_dataset_name(dataset_name="test_dataset")
    )
    assert c.credentials.database == ":memory:"
