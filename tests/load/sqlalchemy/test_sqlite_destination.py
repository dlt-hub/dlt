import os
import tempfile

import dlt
import sqlalchemy as sa
from sqlalchemy import text
from dlt.destinations import sqlalchemy as dlt_sqlalchemy
from dlt.common.utils import uniq_id

from tests.pipeline.utils import assert_load_info
from tests.load.utils import sequence_generator


def assert_engine_not_disposed(engine):
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
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=sa.pool.StaticPool
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_sqlite_inmemory" + uniq_id(),
        destination=dlt_sqlalchemy(engine),
        dataset_name='main'
    )

    info = pipeline.run(
        some_data(),
        table_name="inmemory_table"
    )

    assert_load_info(info)
    assert assert_engine_not_disposed(engine)

    with engine.connect() as conn:
        rows = conn.execute(sa.text(
            "SELECT content FROM inmemory_table ORDER BY content"
        )).fetchall()

    actual_values = [row[0] for row in rows]
    expected_values = [row["content"] for row in output]

    assert actual_values == expected_values
    engine.dispose()


def test_file_based_database_with_engine_args() -> None:
    generator_instance = sequence_generator()

    @dlt.resource
    def some_data():
        yield from next(generator_instance)

    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    try:
        credentials = f"sqlite:///{db_path}"

        engine_args = {
            "connect_args": {"check_same_thread": False},
            "poolclass": sa.pool.StaticPool
        }

        pipeline = dlt.pipeline(
            pipeline_name="test_pipeline_sqlite_file_engine_args" + uniq_id(),
            destination=dlt_sqlalchemy(credentials, engine_args=engine_args),
            dataset_name="main"
        )

        info = pipeline.run(
            some_data(),
            table_name="file_table"
        )

        assert_load_info(info)
    finally:
        os.remove(db_path)