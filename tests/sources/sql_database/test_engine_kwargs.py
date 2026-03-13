import logging
import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa

from dlt.sources.sql_database import sql_database, sql_table

from tests.utils import get_test_storage_root

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


@pytest.mark.parametrize("echo, expect_logs", [(True, True), (False, False)])
def test_engine_kwargs_sqlalchemy_echo(caplog, echo, expect_logs):
    """SQLAlchemy echo flag must control logging output."""
    test_dir = Path(get_test_storage_root()) / f"sqlite_{uuid.uuid4().hex}"
    test_dir.mkdir(parents=True, exist_ok=True)
    db_path = test_dir / "test.db"

    setup_engine = sa.create_engine(f"sqlite:///{db_path}")
    try:
        with setup_engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)"))
            conn.execute(sa.text("INSERT INTO test_table (value) VALUES ('x')"))
    finally:
        setup_engine.dispose()

    source = sql_database(
        credentials=f"sqlite:///{db_path}",
        table_names=["test_table"],
        engine_kwargs={"echo": echo},
    )

    # Do not force log level here — echo=True sets sqlalchemy.engine to INFO,
    # echo=False leaves it at WARNING, which is exactly what we want to verify.
    with caplog.at_level(logging.DEBUG):
        list(source.resources["test_table"])

    engine_logs = [
        r.message.lower() for r in caplog.records if r.name.startswith("sqlalchemy.engine")
    ]
    logs = " ".join(engine_logs)

    if expect_logs:
        assert "select" in logs or "pragma" in logs or "raw sql" in logs
    else:
        assert len(engine_logs) == 0
