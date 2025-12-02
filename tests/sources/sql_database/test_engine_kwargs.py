import pytest
import time
import tempfile
import os
import sqlite3
import sqlalchemy as sa
import logging

from dlt.sources.sql_database import sql_database, sql_table
from dlt.extract.exceptions import ResourceExtractionError

MYSQL_TEST = "mysql+pymysql://user:pass@10.255.255.1/testdb"
POSTGRES_TEST = "postgresql://user:pass@127.0.0.1:5432/testdb"


@pytest.mark.parametrize(
    "credentials",
    [
        "sqlite:///:memory:",
        MYSQL_TEST,
        POSTGRES_TEST,
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
        POSTGRES_TEST,
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
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
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
            start = time.time()
            with pytest.raises(ResourceExtractionError):
                list(resource)
            assert time.time() - start < 1.5
        finally:
            dbapi_conn.rollback()
            dbapi_conn.close()
    finally:
        os.remove(db_path)


def test_engine_kwargs_and_backend_kwargs_with_arrow_backend():
    """Engine kwargs and backend kwargs must work together."""
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        engine = sa.create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)"))
            conn.execute(sa.text("INSERT INTO test_table (value) VALUES ('hello'), ('world')"))

        source = sql_database(
            credentials=f"sqlite:///{db_path}",
            table_names=["test_table"],
            engine_kwargs={"echo": True},
            backend_kwargs={"chunk_size": 2},
            backend="pyarrow",
        )

        tables = list(source.resources["test_table"])
        assert len(tables) == 1
        table = tables[0]
        assert table.num_rows == 2
        assert table["value"].to_pylist() == ["hello", "world"]
    finally:
        os.remove(db_path)


@pytest.mark.parametrize("echo, expect_logs", [(True, True), (False, False)])
def test_engine_kwargs_sqlalchemy_echo(caplog, echo, expect_logs):
    """SQLAlchemy echo flag must control logging output."""
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
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
    finally:
        os.remove(db_path)