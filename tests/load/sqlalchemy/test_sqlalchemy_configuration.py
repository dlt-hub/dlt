import os
import warnings

import pytest

import dlt
import sqlalchemy as sa

from dlt.common.configuration import resolve_configuration
from dlt.common.known_env import DLT_LOCAL_DIR
from dlt.common.utils import uniq_id
from dlt.destinations import sqlalchemy as dlt_sqlalchemy
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

    # pipeline context: uses <pipeline_name>.db for the filename
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


def test_engine_kwargs_forwarded_to_credentials() -> None:
    """engine_kwargs set on the configuration should be forwarded to credentials."""
    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials("sqlite:///:memory:"),
            engine_kwargs={"echo": True},
        )._bind_dataset_name(dataset_name="test_dataset")
    )
    assert c.credentials.engine_kwargs == {"echo": True}


def test_deprecated_engine_args_still_works() -> None:
    """engine_args should still work but emit a DeprecationWarning."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        c = resolve_configuration(
            SqlalchemyClientConfiguration(
                credentials=SqlalchemyCredentials("sqlite:///:memory:"),
                engine_args={"echo": True},
            )._bind_dataset_name(dataset_name="test_dataset")
        )
    deprecation_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert len(deprecation_warnings) >= 1
    assert "engine_args" in str(deprecation_warnings[0].message)
    # engine_args should have been forwarded to engine_kwargs
    assert c.credentials.engine_kwargs == {"echo": True}


def test_both_engine_kwargs_and_engine_args_raises() -> None:
    """Providing both engine_kwargs and engine_args must raise ValueError."""
    with pytest.raises(ValueError, match="Both engine_kwargs and engine_args"):
        resolve_configuration(
            SqlalchemyClientConfiguration(
                credentials=SqlalchemyCredentials("sqlite:///:memory:"),
                engine_kwargs={"echo": True},
                engine_args={"pool_size": 5},
            )._bind_dataset_name(dataset_name="test_dataset")
        )


def test_factory_accepts_engine_kwargs() -> None:
    """The sqlalchemy destination factory should accept engine_kwargs."""
    dest = dlt_sqlalchemy(
        credentials="sqlite:///:memory:",
        engine_kwargs={"echo": True},
    )
    # Verify the factory stored the kwarg (it will be resolved later)
    assert dest.config_params["engine_kwargs"] == {"echo": True}


def test_owned_engine_ref_counting_disposes_on_last_return(mocker) -> None:
    """Owned engine should be disposed when the last borrowed connection is returned."""
    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials("sqlite:///:memory:")
        )._bind_dataset_name(dataset_name="test_dataset")
    )

    engine = c.credentials.engine
    dispose_spy = mocker.spy(engine, "dispose")

    assert c.credentials._conn_borrows == 0
    assert c.credentials._conn_owner is True

    conn = c.credentials.borrow_conn()
    assert c.credentials._conn_borrows == 1

    c.credentials.return_conn(conn)

    assert c.credentials._conn_borrows == 0
    dispose_spy.assert_called_once()


def test_owned_engine_multiple_borrows(mocker) -> None:
    """Multiple borrows should increment refcount; engine disposed only after all returned."""
    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials("sqlite:///:memory:")
        )._bind_dataset_name(dataset_name="test_dataset")
    )

    engine = c.credentials.engine
    dispose_spy = mocker.spy(engine, "dispose")

    conn1 = c.credentials.borrow_conn()
    assert c.credentials._conn_borrows == 1

    conn2 = c.credentials.borrow_conn()
    assert c.credentials._conn_borrows == 2

    c.credentials.return_conn(conn1)
    assert c.credentials._conn_borrows == 1
    dispose_spy.assert_not_called()

    c.credentials.return_conn(conn2)
    assert c.credentials._conn_borrows == 0
    dispose_spy.assert_called_once()


def test_owned_engine_connect_failure_does_not_leak_refcount(mocker) -> None:
    """If engine.connect() fails, refcount must not be permanently inflated."""
    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials("sqlite:///:memory:")
        )._bind_dataset_name(dataset_name="test_dataset")
    )

    # Force engine creation so we can spy on it
    engine = c.credentials.engine
    assert c.credentials._conn_borrows == 0

    # Make connect() raise
    mocker.patch.object(engine, "connect", side_effect=sa.exc.OperationalError("fail", {}, None))

    with pytest.raises(sa.exc.OperationalError):
        c.credentials.borrow_conn()

    # Refcount must be back to 0, not stuck at 1
    assert c.credentials._conn_borrows == 0


def test_external_engine_ref_counting_does_not_dispose(mocker) -> None:
    """External engine should not be disposed when connections are returned."""
    engine = sa.create_engine("sqlite:///:memory:")

    try:
        c = resolve_configuration(
            SqlalchemyClientConfiguration(
                credentials=SqlalchemyCredentials(engine)
            )._bind_dataset_name(dataset_name="test_dataset")
        )

        dispose_spy = mocker.spy(engine, "dispose")

        assert c.credentials._conn_owner is False
        assert c.credentials._conn_borrows == 0

        conn = c.credentials.borrow_conn()
        assert c.credentials._conn_borrows == 0

        c.credentials.return_conn(conn)
        assert c.credentials._conn_borrows == 0

        dispose_spy.assert_not_called()

        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
    finally:
        engine.dispose()
