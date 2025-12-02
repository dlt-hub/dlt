import dlt
import sqlalchemy as sa
from dlt.destinations import sqlalchemy

from tests.pipeline.utils import assert_load_info
from tests.load.utils import sequence_generator

import tempfile
import os


def test_inmemory_database() -> None:
    generator_instance1 = sequence_generator()

    @dlt.resource
    def some_data():
        yield from next(generator_instance1)

    engine = sa.create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=sa.pool.StaticPool
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_sqlite_inmemory",
        destination=sqlalchemy(engine),
        dataset_name='main'
    )

    info = pipeline.run(
        some_data(),
        table_name="inmemory_table"
    )

    assert_load_info(info)


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
            pipeline_name="test_pipeline_sqlite_file_engine_args",
            destination=sqlalchemy(credentials, engine_args=engine_args),
            dataset_name="main"
        )

        info = pipeline.run(
            some_data(),
            table_name="file_table"
        )

        assert_load_info(info)
    finally:
        os.remove(db_path)