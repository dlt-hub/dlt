import dlt, os
from dlt.common.utils import uniq_id
from tests.load.utils import AWS_BUCKET
from tests.load.pipeline.utils import  load_table_counts
from tests.load.utils import TABLE_UPDATE_COLUMNS_SCHEMA, TABLE_ROW_ALL_DATA_TYPES, assert_all_data_types_row
from copy import copy, deepcopy
from tests.pipeline.utils import assert_load_info

def test_athena_destinations() -> None:

    pipeline = dlt.pipeline(
        pipeline_name='aathena_' + uniq_id(),
        destination="athena",
        staging="filesystem",
        dataset_name='aathena_' + uniq_id(),
        full_refresh=True)
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = AWS_BUCKET

    @dlt.resource(name="items", write_disposition="append")
    def items():
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{
                "id": 101,
                "name": "sub item 101"
            },{
                "id": 101,
                "name": "sub item 102"
            }]
        }

    pipeline.run(items)

    # see if we have athena tables with items
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values() ])
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2
    assert table_counts["_dlt_loads"] == 1

    # load again with schema evloution
    @dlt.resource(name="items", write_disposition="append")
    def items2():
        yield {
            "id": 1,
            "name": "item",
            "new_field": "hello",
            "sub_items": [{
                "id": 101,
                "name": "sub item 101",
                "other_new_field": "hello 101",
            },{
                "id": 101,
                "name": "sub item 102",
                "other_new_field": "hello 102",
            }]
        }
    pipeline.run(items2)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()])
    assert table_counts["items"] == 2
    assert table_counts["items__sub_items"] == 4
    assert table_counts["_dlt_loads"] == 2


# TODO: add athena to destinations in tests and this test will run at the right location
def test_athena_datatypes() -> None:

    pipeline = dlt.pipeline(
        pipeline_name='aathena_' + uniq_id(),
        destination="athena",
        staging="filesystem",
        dataset_name='aathena_' + uniq_id(),
        full_refresh=True)
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = AWS_BUCKET
    data_types = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
    column_schemas = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)

    # apply the exact columns definitions so we process complex and wei types correctly!
    @dlt.resource(table_name="data_types", write_disposition="append", columns=column_schemas)
    def my_resource():
        nonlocal data_types
        yield [data_types]*10

    @dlt.source(max_table_nesting=0)
    def my_source():
        return my_resource

    info = pipeline.run(my_source())
    assert_load_info(info)

    with pipeline.sql_client() as sql_client:
        db_rows = sql_client.execute_sql("SELECT * FROM data_types")
        assert len(db_rows) == 10
        db_row = list(db_rows[0])
        # content must equal
        assert_all_data_types_row(db_row[:-2])
