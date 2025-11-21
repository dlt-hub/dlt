"""Build a mermaid graph representation using raw strings without additional dependencies"""
from enum import Enum
from typing import Literal
from dlt.common.schema.typing import TReferenceCardinality, TStoredSchema, TTableReferenceStandalone, TTableSchema, TTableSchemaColumns


def schema_to_mermaid(schema: TStoredSchema, references: list[TTableReferenceStandalone], include_dlt_tables: bool = True) -> str:
    mermaid_er_diagram = "erDiagram\n"

    for table_name, table_schema in schema['tables'].items():
        if not include_dlt_tables and table_name.startswith("_dlt"):
            continue
        mermaid_table = _to_mermaid_table(table_schema)
        mermaid_er_diagram += mermaid_table

    if not include_dlt_tables:
        references = list(filter(lambda x: not _is_dlt_table_reference(x), references))
    for ref in references:
        ref_txt = _to_mermaid_reference(ref)
        mermaid_er_diagram += ref_txt
    return mermaid_er_diagram


def _is_dlt_table_reference(ref: TTableReferenceStandalone) -> bool:
    """returns True if reference table or table is a _dlt_table
    """
    if ref["table"].startswith("_dlt") or ref["referenced_table"].startswith("_dlt"):
        return True
    return False


def _to_mermaid_table(table: TTableSchema) -> str:
    items = [table.get("name", ""), "{", _to_mermaid_column(table.get("columns", {})), "}\n"]
    return "".join(items)


def _to_mermaid_column(columns: TTableSchemaColumns) -> str:
    rows = ""
    for column_name, column_schema in columns.items():
        if column_schema.get("primary_key"):
            rows += f"{column_schema['data_type']} {column_name} PK \n"
        elif column_schema.get("unique"):
            rows += f"{column_schema['data_type']} {column_name} UK \n"
        else:
            rows += f"{column_schema['data_type']} {column_name} \n"
    return rows


class TMermaidArrows(str, Enum):
    ONE_TO_MANY = "||--|{"
    MANY_TO_ONE = "}|--||"
    ZERO_TO_MANY = "|o--|{"
    MANY_TO_ZERO = "}|--o|"
    ONE_TO_MORE = "||--o{"
    MORE_TO_ONE = "}o--||"
    ONE_TO_ONE = "||--||"
    MANY_TO_MANY = "}|--|{"
    ZERO_TO_ONE = "|o--o|"


_CARDINALITY_ARROW: dict[Literal["default"] | TReferenceCardinality, TMermaidArrows] = {
    "one_to_many": TMermaidArrows.ONE_TO_MANY,
    "many_to_one": TMermaidArrows.MANY_TO_ONE,
    "zero_to_many": TMermaidArrows.ZERO_TO_MANY,
    "many_to_zero": TMermaidArrows.MANY_TO_ZERO,
    "one_to_one": TMermaidArrows.ONE_TO_ONE,
    "many_to_many": TMermaidArrows.MANY_TO_MANY,
    "zero_to_one": TMermaidArrows.ZERO_TO_ONE,
    "one_to_zero": TMermaidArrows.ZERO_TO_ONE,
    "default": TMermaidArrows.ZERO_TO_ONE
}


def _to_mermaid_reference(ref: TTableReferenceStandalone) -> str:
    """Builds references in the following format using cardinality and label to describe
    the relationship
    <first-entity> [<relationship> <second-entity> : <relationship-label>]
    """
    first = ref.get("table")
    second = ref.get("referenced_table")
    raw_card = ref.get("cardinality", "default")
    label = ref.get("label", "contains")

    # Map cardinality to arrow syntax; fall back to one to many
    arrow = _CARDINALITY_ARROW.get(raw_card)

    parts = [first, arrow, second]
    line = " ".join(parts)
    if label:
        line += f" : {label} \n"
    return line
