import pytest
import time
import sqlite3
import sqlalchemy as sa
import logging
from pathlib import Path
import uuid

from dlt.extract.exceptions import ResourceExtractionError
from dlt.sources.sql_database import sql_database, sql_table

from tests.utils import TEST_STORAGE_ROOT

MYSQL_TEST = "mysql+pymysql://user:pass@10.255.255.1/testdb"


# these two tests below are still valuable since the db driver validates engine kwargs before establishing connection
@pytest.mark.parametrize(
    "credentials",
    [
        "sqlite:///:memory:",
        MYSQL_TEST,
    ],
)
def test_invalid_engine_kwargs_propagate(credentials):
    """Invalid engine kwargs must be forwarded to DBAPI."""
    with pytest.raises(TypeError):
        sql_database(
            credentials=credentials,
            engine_kwargs={"this_is_an_invalid_argument_name": True},
            table_names=["dummy"],
        )


@pytest.mark.parametrize(
    "credentials",
    [
        "sqlite:///:memory:",
        MYSQL_TEST,
    ],
)
def test_invalid_engine_kwargs_fail_during_reflection_for_table(credentials):
    """Invalid engine kwargs must fail during table reflection."""
    with pytest.raises(TypeError):
        sql_table(
            table="dummy",
            credentials=credentials,
            engine_kwargs={"invalid_argument_name": True},
        )


def test_engine_kwargs_timeout_is_honored_sqlite():
    """SQLite timeout in engine_kwargs must be honored."""
    test_dir = Path(TEST_STORAGE_ROOT) / f"sqlite_{uuid.uuid4().hex}"
    test_dir.mkdir(parents=True, exist_ok=True)
    db_path = test_dir / "test.db"

    engine = sa.create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        conn.execute(sa.text("CREATE TABLE test_table (id INTEGER PRIMARY KEY)"))

    dbapi_conn = sqlite3.connect(db_path)
    cur = dbapi_conn.cursor()
    cur.execute("BEGIN EXCLUSIVE")

    try:
        source = sql_database(
            credentials=f"sqlite:///{db_path}",
            engine_kwargs={"connect_args": {"timeout": 1}},
            table_names=["test_table"],
            defer_table_reflect=True,
        )
        resource = source.resources["test_table"]
        with pytest.raises(ResourceExtractionError):
            list(resource)
    finally:
        dbapi_conn.rollback()
        dbapi_conn.close()


@pytest.mark.parametrize("echo, expect_logs", [(True, True), (False, False)])
def test_engine_kwargs_sqlalchemy_echo(caplog, echo, expect_logs):
    """SQLAlchemy echo flag must control logging output."""
    test_dir = Path(TEST_STORAGE_ROOT) / f"sqlite_{uuid.uuid4().hex}"
    test_dir.mkdir(parents=True, exist_ok=True)
    db_path = test_dir / "test.db"

    engine = sa.create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        conn.execute(sa.text("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(sa.text("INSERT INTO test_table (value) VALUES ('x')"))

    source = sql_database(
        credentials=f"sqlite:///{db_path}",
        table_names=["test_table"],
        engine_kwargs={"echo": echo},
    )

    with caplog.at_level(logging.INFO):
        list(source.resources["test_table"])

    logs = " ".join(r.message.lower() for r in caplog.records)

    if expect_logs:
        assert "select" in logs or "pragma" in logs or "raw sql" in logs
    else:
        assert logs == ""
