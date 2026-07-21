# mypy: disable-error-code="no-untyped-def,arg-type"


def test_delete_append_load_id() -> None:
    import dlt
    from dlt.common.utils import uniq_id

    pipeline = dlt.pipeline(
        "delete_demo_" + uniq_id(), destination="duckdb", dev_mode=True
    )
    # load append data with nested tables twice; delete the first package, keep the second
    load_info = pipeline.run(
        [{"id": 1, "items": [{"x": 1}, {"x": 2}]}], table_name="my_table"
    )
    other_info = pipeline.run([{"id": 2, "items": [{"x": 3}]}], table_name="my_table")
    load_id = load_info.loads_ids[0]
    other_load_id = other_info.loads_ids[0]

    # @@@DLT_SNIPPET_START delete_append_load_id
    from dlt.common.schema.utils import get_nested_tables

    root_table = "my_table"
    schema = pipeline.default_schema

    with pipeline.sql_client() as client:

        def load_id_filter(table_name: str) -> str:
            table = schema.tables[table_name]
            load_id_col = client.escape_column_name("_dlt_load_id")
            row_key = client.escape_column_name("_dlt_id")
            parent_key = client.escape_column_name("_dlt_parent_id")
            if table.get("parent") is None:
                return f"{load_id_col} = {client.capabilities.escape_literal(load_id)}"
            parent = client.make_qualified_table_name(table["parent"])
            return (
                f"{parent_key} IN (SELECT {row_key} FROM {parent}"
                f" WHERE {load_id_filter(table['parent'])})"
            )

        # delete the deepest nested tables first so parent links still resolve
        for table in reversed(get_nested_tables(schema.tables, root_table)):
            table_name = client.make_qualified_table_name(table["name"])
            client.execute_sql(
                f"DELETE FROM {table_name} WHERE {load_id_filter(table['name'])}"
            )
    # @@@DLT_SNIPPET_END delete_append_load_id

    # the deleted package has no rows left, the other package is intact
    dataset = pipeline.dataset()
    assert dict(dataset.row_counts(load_id=load_id).fetchall())["my_table"] == 0
    assert dict(dataset.row_counts(load_id=other_load_id).fetchall())["my_table"] == 1
    # nested tables carry no _dlt_load_id, so only the other load's nested row remains
    assert len(dataset["my_table__items"].fetchall()) == 1
