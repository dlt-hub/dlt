import os

import dlt
import sqlalchemy as sa
from sqlalchemy import text
from dlt.destinations import sqlalchemy as dlt_sqlalchemy
from dlt.common.utils import uniq_id

from tests.pipeline.utils import assert_load_info


def assert_engine_not_disposed(engine: sa.engine.Engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except sa.exc.ResourceClosedError:
        return False


def test_inmemory_database_passing_engine() -> None:
    output = [
        {"content": 1},
        {"content": 2},
        {"content": 3},
    ]

    @dlt.resource
    def some_data():
        yield output

    engine = sa.create_engine(
        "sqlite:///file:shared?mode=memory&cache=shared&uri=true",
        connect_args={"check_same_thread": False},
        poolclass=sa.pool.SingletonThreadPool,
    )

    try:
        pipeline = dlt.pipeline(
            pipeline_name="test_pipeline_sqlite_inmemory" + uniq_id(),
            destination=dlt_sqlalchemy(engine),
            dataset_name="main",
        )

        info = pipeline.run(some_data(), table_name="inmemory_table")

        assert_load_info(info)
        assert assert_engine_not_disposed(engine)

        with engine.connect() as conn:
            rows = conn.execute(
                sa.text("SELECT content FROM inmemory_table ORDER BY content")
            ).fetchall()

            actual_values = [row[0] for row in rows]
            expected_values = [row["content"] for row in output]

            assert actual_values == expected_values

            # verify the database is actually in-memory via PRAGMA database_list
            # for in-memory databases the file column is empty
            db_list = conn.execute(text("PRAGMA database_list")).fetchall()
            main_db = [row for row in db_list if row[1] == "main"]
            assert main_db, "main database not found in PRAGMA database_list"
            assert (
                main_db[0][2] == ""
            ), f"expected empty file path for in-memory db, got: {main_db[0][2]!r}"

    finally:
        engine.dispose()


def test_file_based_database_with_engine_kwargs() -> None:
    output = [
        {"content": 1},
        {"content": 2},
        {"content": 3},
    ]

    @dlt.resource
    def some_data():
        yield output

    local_dir = dlt.current.run_context().local_dir
    db_path = os.path.join(local_dir, "test.db")
    credentials = f"sqlite:///{db_path}"

    engine_kwargs = {
        "connect_args": {"check_same_thread": True},
        "poolclass": sa.pool.SingletonThreadPool,
    }

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_sqlite_file_engine_kwargs" + uniq_id(),
        destination=dlt_sqlalchemy(credentials, engine_kwargs=engine_kwargs),
        dataset_name="main",
    )

    info = pipeline.run(some_data(), table_name="file_table")

    assert_load_info(info)

    verify_engine = sa.create_engine(credentials)
    try:
        with verify_engine.connect() as conn:
            rows = conn.execute(
                sa.text("SELECT content FROM file_table ORDER BY content")
            ).fetchall()

        actual_values = [row[0] for row in rows]
        expected_values = [row["content"] for row in output]

        assert actual_values == expected_values
    finally:
        verify_engine.dispose()
