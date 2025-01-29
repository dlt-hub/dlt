import dlt


@dlt.resource
def nested_data():
    yield [
        {
            "id": 1,
            "a": [
                {
                    "a_id": "2",
                    "b": [{"b_id": "3"}],
                }
            ],
            "c": [
                {
                    "c_id": "4",
                    "d": [{"d_id": "5"}],
                }
            ],
        }
    ]


def test_apply_hints_nested_hints_column_types() -> None:
    nested_data_rs = nested_data()

    nested_data_rs.apply_hints(
        path=["a", "b"],
        columns=[
            {
                "name": "b_id",
                "data_type": "bigint",
            },
        ],
    )
    nested_data_rs.apply_hints(
        path=["c"],
        columns=[
            {
                "name": "c_id",
                "data_type": "double",
            },
        ],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_apply_hints_nested_hints", dev_mode=True, destination="duckdb"
    )
    pipeline.run(nested_data_rs)

    schema_tables = pipeline.default_schema.tables

    assert schema_tables["nested_data__a__b"]["columns"]["b_id"]["data_type"] == "bigint"
    assert schema_tables["nested_data__c"]["columns"]["c_id"]["data_type"] == "double"

    assert schema_tables["nested_data__a__b"]["parent"] == "nested_data__a"
    assert schema_tables["nested_data__c"]["parent"] == "nested_data"

    # Try changing the parent name
    nested_data_rs.apply_hints(table_name="override_parent")

    pipeline = dlt.pipeline(
        pipeline_name="test_apply_hints_nested_hints_2", dev_mode=True, destination="duckdb"
    )
    pipeline.run(nested_data_rs)

    schema_tables = pipeline.default_schema.tables

    assert schema_tables["override_parent__a__b"]["parent"] == "override_parent__a"
    assert schema_tables["override_parent__c"]["parent"] == "override_parent"
    assert schema_tables["override_parent__a__b"]["name"] == "override_parent__a__b"
    assert schema_tables["override_parent__a__b"]["columns"]["b_id"]["data_type"] == "bigint"

    for key in schema_tables.keys():
        assert not key.startswith("nested_data")


def test_apply_hints_nested_hints_override_child_name():
    nested_data_rs = nested_data()

    # Override both levels of child tables
    nested_data_rs.apply_hints(path=["a"], table_name="override_child_a")
    nested_data_rs.apply_hints(path=["a", "b"], table_name="override_child_a_b")

    pipeline = dlt.pipeline(
        pipeline_name="test_apply_hints_nested_hints_3", dev_mode=True, destination="duckdb"
    )

    pipeline.run(nested_data_rs)

    schema_tables = pipeline.default_schema.tables

    assert schema_tables["override_child_a_b"]["name"] == "override_child_a_b"
    # Parent should match the overrid parent name
    assert schema_tables["override_child_a_b"]["parent"] == "override_child_a"

    assert schema_tables["override_child_a"]["name"] == "override_child_a"
