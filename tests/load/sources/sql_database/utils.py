import dlt
from dlt.extract.resource import DltResource
from tests.pipeline.utils import assert_load_info


def assert_incremental_chunks(
    pipeline: dlt.Pipeline, table: DltResource, cursor: str, timezone: bool, row_count: int
) -> None:
    # number of user must be multiply of 10
    assert row_count % 10 == 0
    for _ in range(row_count // 10):
        info = pipeline.run(table.add_limit(1))
        assert_load_info(info)
        assert pipeline.last_trace.last_normalize_info.row_counts[table.name] == 10
        tzinfo = table.state["incremental"][cursor]["last_value"].tzinfo
        if timezone:
            assert tzinfo is not None
        else:
            assert tzinfo is None
    # load but that will be empty
    pipeline.run(table)
    r_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert table.name not in r_counts or r_counts[table.name] == 0
