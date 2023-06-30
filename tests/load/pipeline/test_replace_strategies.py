import dlt, os

from tests.pipeline.utils import  assert_load_info
from tests.load.pipeline.utils import  load_table_counts

def test_replace_strategies() -> None:
    # only allow 40 items per file
    os.environ['DATA_WRITER__FILE_MAX_ITEMS'] = "40"
    pipeline = dlt.pipeline(pipeline_name='test_replace_strategies', destination="duckdb", dataset_name='test_replace_strategies', full_refresh=True)

    global offset
    offset = 0

    @dlt.resource(table_name="items", write_disposition="replace")
    def load_items():
        # will produce 3 jobs for the main table with 40 items each
        # 6 jobs for the sub_items 
        # 3 jobs for the sub_sub_items
        for index, y in enumerate(range(offset, offset+120), 1):
            yield {
                "id": index, 
                "name": f"item {index}",
                "sub_items": [{
                    "id": index + 1000,
                    "name": f"sub item {index + 1000}"
                },{
                    "id": index + 2000,
                    "name": f"sub item {index + 2000}",
                    "sub_sub_items": [{
                        "id": index + 3000,
                        "name": f"sub item {index + 3000}",
                    }]
                }]
                }
            

    # first run with offset 0
    info = pipeline.run(load_items)
    # second run with higher offset so we can check the results
    offset = 1000
    info = pipeline.run(load_items)
    assert_load_info(info)

    # as it stands, only one job will be present for each table and it is unclear which of the items have made it into the table
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 40
    assert table_counts["items__sub_items"] == 40
    assert table_counts["items__sub_items__sub_sub_items"] == 40


