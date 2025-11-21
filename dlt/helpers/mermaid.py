"""Build a mermaid graph representation using raw strings without additional dependencies"""
from enum import Enum

from dlt.common.schema.typing import (
    TColumnSchema,
    TReferenceCardinality,
    TStoredSchema,
    TTableReferenceStandalone,
    TTableSchema,
)


INDENT = "    "


def schema_to_mermaid(
    schema: TStoredSchema,
    *,
    references: list[TTableReferenceStandalone],
    hide_columns: bool = False,
    hide_descriptions: bool = False,
    include_dlt_tables: bool = True,
) -> str:
    mermaid_er_diagram = "erDiagram\n"

    for table_name, table_schema in schema["tables"].items():
        if not include_dlt_tables and table_name.startswith("_dlt"):
            continue

        mermaid_er_diagram += INDENT + _to_mermaid_table(
            table_schema,
            hide_columns=hide_columns,
            hide_descriptions=hide_descriptions,
        )

    for ref in references:
        if not include_dlt_tables:
            if ref["table"].startswith("_dlt") or ref["referenced_table"].startswith("_dlt"):
                continue

        mermaid_er_diagram += INDENT + _to_mermaid_reference(ref)

    return mermaid_er_diagram


def _to_mermaid_table(
    table: TTableSchema, hide_columns: bool = False, hide_descriptions: bool = False
) -> str:
    mermaid_table: str = table["name"]
    mermaid_table += "{\n"

    if hide_columns is False:
        for column in table["columns"].values():
            mermaid_table += INDENT + _to_mermaid_column(
                column,
                hide_descriptions=hide_descriptions,
            )

    mermaid_table += "}\n"
    return mermaid_table


# TODO add scale & precision to `data_type`
def _to_mermaid_column(column: TColumnSchema, hide_descriptions: bool = False) -> str:
    mermaid_col = column["data_type"] + " " + column["name"]
    keys = []
    if column.get("primary_key"):
        keys.append("PK")

    if column.get("unique"):
        keys.append("UK")

    if keys:
        mermaid_col += " " + ",".join(keys)

    if hide_descriptions is False:
        if description := column.get("description"):
            mermaid_col += f' "{description}"'

    mermaid_col += "\n"
    return mermaid_col


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


_CARDINALITY_ARROW: dict[TReferenceCardinality, TMermaidArrows] = {
    "one_to_many": TMermaidArrows.ONE_TO_MANY,
    "many_to_one": TMermaidArrows.MANY_TO_ONE,
    "zero_to_many": TMermaidArrows.ZERO_TO_MANY,
    "many_to_zero": TMermaidArrows.MANY_TO_ZERO,
    "one_to_one": TMermaidArrows.ONE_TO_ONE,
    "many_to_many": TMermaidArrows.MANY_TO_MANY,
    "zero_to_one": TMermaidArrows.ZERO_TO_ONE,
    "one_to_zero": TMermaidArrows.ZERO_TO_ONE,
}


def _to_mermaid_reference(ref: TTableReferenceStandalone) -> str:
    """Builds references in the following format using cardinality and label to describe
    the relationship

    <left-entity> [<relationship> <right-entity> : <relationship-label>]
    """
    left_table = ref.get("table")
    right_table = ref.get("referenced_table")
    cardinality = ref.get("cardinality", "one_to_many")
    label = ref.get("label", '""')
    arrow: str = _CARDINALITY_ARROW.get(cardinality).value

    mermaid_reference = f"{left_table} {arrow} {right_table}"
    if label:
        mermaid_reference += f" : {label}"

    mermaid_reference += "\n"
    return mermaid_reference
