import pytest, dlt, os

from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_simple_scd2_load(destination_config: DestinationTestConfiguration) -> None:
    # use scd2
    os.environ["DESTINATION__MERGE_STRATEGY"] = "scd2"

    @dlt.resource(name="items", write_disposition="merge")
    def load_items():
        yield from [
            {
                "id": 1,
                "name": "one",
            },
            {
                "id": 2,
                "name": "two",
                "children": [
                    {
                        "id_of": 2,
                        "name": "child2",
                    }
                ],
            },
            {
                "id": 3,
                "name": "three",
                "children": [
                    {
                        "id_of": 3,
                        "name": "child3",
                    }
                ],
            },
        ]

    p = destination_config.setup_pipeline("test", full_refresh=True)
    p.run(load_items())

    # new version of item 1
    # item 3 deleted
    @dlt.resource(name="items", write_disposition="merge")
    def load_items_2():
        yield from [
            {
                "id": 1,
                "name": "one_new",
            },
            {
                "id": 2,
                "name": "two",
                "children": [
                    {
                        "id_of": 2,
                        "name": "child2_new",
                    }
                ],
            },
        ]

    p.run(load_items_2())

    with p.sql_client() as c:
        with c.execute_query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME = 'items' ORDER"
            " BY ORDINAL_POSITION"
        ) as cur:
            print(cur.fetchall())
        with c.execute_query("SELECT * FROM items") as cur:
            for row in cur.fetchall():
                print(row)

    with p.sql_client() as c:
        with c.execute_query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME ="
            " 'items__children' ORDER BY ORDINAL_POSITION"
        ) as cur:
            print(cur.fetchall())
        with c.execute_query("SELECT * FROM items__children") as cur:
            for row in cur.fetchall():
                print(row)
    # print(p.default_schema.to_pretty_yaml())
    assert False
