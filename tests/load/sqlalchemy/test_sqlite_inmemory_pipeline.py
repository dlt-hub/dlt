import dlt
import sqlalchemy as sa

from tests.pipeline.utils import assert_load_info
from tests.load.utils import sequence_generator


def test_inmemory_database() -> None:
    engine = sa.create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=sa.pool.StaticPool
    )

    generator_instance1 = sequence_generator()

    @dlt.resource
    def some_data():
        yield from next(generator_instance1)

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_sqlite_inmemory",
        destination=dlt.destinations.sqlalchemy(engine),
        dataset_name="main"
    )
    info = pipeline.run(
        some_data(),
        table_name="inmemory_table"
    )
    assert_load_info(info)