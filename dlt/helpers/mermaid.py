"""Build a mermaid graph representation using raw strings without additional dependencies"""


from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableSchema, TTableSchemaColumns


def schema_to_mermaid(schema: Schema, table_names: list[str] = []) -> str:
    if len(table_names) == 0:
        table_names = schema.data_table_names()

    mermaid_str = "erDiagram\n"
    for table_name in table_names:
        table = schema.get_table(table_name)
        table_schema = _table_to_text(table)
        mermaid_str += table_schema
    return mermaid_str


def _table_to_text(table: TTableSchema) -> str:
    items = [table.get("name", ""), "{", _columns_to_text(table.get("columns")), "}\n"]
    return "".join(items)


def _columns_to_text(columns: TTableSchemaColumns) -> str:
    rows = ""
    for column_name, column_schema in columns.items():
        if column_schema.get("primary_key"):
            rows += f"{column_schema['data_type']} {column_name} PK \n"
        elif column_schema.get("unique"):
            rows += f"{column_schema['data_type']} {column_name} UK \n"
        else:
            rows += f"{column_schema['data_type']} {column_name} \n"
    return rows
