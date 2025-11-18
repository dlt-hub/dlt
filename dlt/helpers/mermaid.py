"""Build a mermaid graph representation using raw strings without additional dependencies"""
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableReferenceStandalone, TTableSchema, TTableSchemaColumns


def schema_to_mermaid(schema: Schema, table_names: list[str] = [], include_dlt_talbes: bool = True) -> str:
    if len(table_names) == 0:
        table_names = schema.data_table_names()
    if include_dlt_talbes:
        table_names += schema.dlt_table_names()
    references = filter(
        _relevant_references(table_names),
        schema.references,
    )

    mermaid_str = "erDiagram\n"
    for table_name in table_names:
        table = schema.get_table(table_name)
        table_schema = _table_to_text(table)
        mermaid_str += table_schema

    if references:
        for ref in references:
            ref_txt = _reference_to_text(ref)
            mermaid_str += ref_txt
    return mermaid_str


def _relevant_references(table_names):
    return lambda x: (x["table"] in table_names) & (x["referenced_table"] in table_names)


def _table_to_text(table: TTableSchema) -> str:
    items = [table.get("name", ""), "{", _columns_to_text(table.get("columns", {})), "}\n"]
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


_CARDINALITY_ARROW = {
    "one_to_many": "||--|{",
    "many_to_one": "|{--||",
    "zero_to_many": "||--o{",
    "one_to_more": "||--o{",
}


def _reference_to_text(ref: TTableReferenceStandalone) -> str:
    # <first-entity> [<relationship> <second-entity> : <relationship-label>]
    first = ref.get("table")
    second = ref.get("referenced_table")
    raw_card = ref.get("cardinality", "")
    label = ref.get("label", "contains")

    # Map cardinality to arrow syntax; fall back to one to many
    arrow = _CARDINALITY_ARROW.get(raw_card, "|{--||")

    # Build the line
    #   1. first entity
    #   2. space + arrow
    #   3. space + second entity
    #   4. optional space + colon + label
    parts = [first, arrow, second]
    line = " ".join(parts)
    if label:
        line += f" : {label} \n"
    return line
