"""Pipeline that produces null-only columns for testing seen-null-first migration."""
import dlt


@dlt.resource(table_name="items", write_disposition="append")
def items_with_nulls():
    yield [
        {"id": 1, "name": "Alice", "optional_field": None},
        {"id": 2, "name": "Bob", "optional_field": None},
    ]


if __name__ == "__main__":
    p = dlt.pipeline("null_columns_test", destination="duckdb", dataset_name="null_data")
    info = p.run(items_with_nulls())
    print(info)
