import textwrap
from pathlib import Path

from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import (
    TColumnSchema,
    TTableSchema,
    TTableReference,
    TTableReferenceParam,
    TStoredSchema,
)
from dlt.common.schema.utils import get_first_column_name_with_prop, get_root_table
from dlt.common.storages.schema_storage import SchemaStorage

# TODO create a TStylesheet specs: alpha background, hints to include
INDENT = "    "
TABLE_HEADER_PORT = "p0"
TABLE_HEADER_COLOR = "#bbca06"
TABLE_HEADER_FONT_COLOR = "#1c1c34"
TABLE_BORDER_COLOR = "#1c1c34"
TABLE_ROW_COLOR = "#e7e2dd"


def _get_column_html(column: TColumnSchema, column_idx: int) -> str:
    """Generate a segment of the node label using Graphviz HTML-like language
    ref: https://graphviz.org/doc/info/shapes.html#html 
    """

    # TODO port is how to align arrows
    column_name = column.get("name")
    if column.get("primary_key") is True:
        column_name = f"<B>{column_name}🔑</B>"

    column_type = f'{column.get("data_type")}'
    if column.get("nullable") is False:
        column_type += " <B>!</B>"

    return f"""<TR>
            <TD ALIGN="LEFT" PORT="f{column_idx}" BGCOLOR="{TABLE_ROW_COLOR}">
                <TABLE CELLPADDING="0" CELLSPACING="0" BORDER="0">
                    <TR>
                        <TD ALIGN="LEFT">{column_name}</TD>
                        <TD ALIGN="RIGHT"><FONT>{column_type}</FONT></TD>
                    </TR>
                </TABLE>
            </TD>
        </TR>"""


def _get_table_html(table: TTableSchema) -> str:
    """Generate node label using Graphviz HTML-like language
    ref: https://graphviz.org/doc/info/shapes.html#html 
    """
    rendered_columns = ""
    for idx, column in enumerate(table.get("columns", {}).values()):
        rendered_columns += _get_column_html(column, idx + 1)

    return f"""<TABLE BORDER="2" COLOR="{TABLE_BORDER_COLOR}" CELLBORDER="1" CELLSPACING="0" CELLPADDING="10">
        <TR>
            <TD PORT="{TABLE_HEADER_PORT}" BGCOLOR="{TABLE_HEADER_COLOR}">
                <FONT COLOR="{TABLE_HEADER_FONT_COLOR}"><B>{table.get("name")}</B></FONT>
            </TD>
        </TR>
        {rendered_columns}
    </TABLE>
    """


def get_table_node(table: TTableSchema) -> str:
    """Generate graph nodes representing tables and columns"""
    name = table.get("name")
    description = f'tooltip="{table.get("description")}";' if table.get("description") else ' '
    label = _get_table_html(table)
    return f'"{name}" [id="{name}";{description}label=<\n{label}>];\n\n'


def _get_reference_edges(
    from_table_port: str,
    from_column_port: str,
    to_table_port: str,
    to_column_port: str,
    relationship: str,
) -> str:
    """Create edges between table columns representing references.
    Columns are addressed by port in the Graphviz HTML-like language.
    """
    return textwrap.dedent(
        f"""{from_table_port} -> {to_table_port} [style=invis, weight=100, color=red]
        {from_column_port}:e -> {to_column_port}:w [dir={relationship}, penwidth=3, color="{TABLE_BORDER_COLOR}", arrowtail="crow", arrowhead="tee"];
        """
    )


def get_table_references_edges(reference, table: TTableSchema, referenced_table: TTableSchema) -> str:
    """Generate graph edges representing column references"""
    # TODO determine relationship is `both` or `forward`
    from_table = table["name"]
    to_table = referenced_table["name"]
    from_table_port = f"{from_table}:{TABLE_HEADER_PORT}"
    to_table_port = f"{to_table}:{TABLE_HEADER_PORT}"

    rendered = ""
    # TODO this could be optimized by mapping all ports once upfront
    for column, referenced_column in zip(reference["columns"], reference["referenced_columns"]):
        # indexing starts at 1 because 0 is table header
        column_idx = 1 + list(table["columns"].keys()).index(column)
        referenced_column_idx = 1 + list(referenced_table["columns"].keys()).index(
            referenced_column
        )

        from_column_port = f"{from_table}:f{column_idx}"
        to_column_port = f"{to_table}:f{referenced_column_idx}"
        relationship = "both"

        rendered += _get_reference_edges(
            from_table_port, from_column_port, to_table_port, to_column_port, relationship
        )

    return rendered


def determine_references(table: TTableSchema, schema: Schema) -> list[TTableReference]:
    references: list[TTableReference] = table.get("references", [])
    if table.get("parent"):
        parent_key = get_first_column_name_with_prop(table, "parent_key")
        row_key_in_parent_table = get_first_column_name_with_prop(table, "row_key")
        parent_table_name = table.get("parent")

        dlt_id_reference = TTableReference(
            columns=[parent_key],
            referenced_table=parent_table_name,
            referenced_columns=[row_key_in_parent_table],
        )
        references.append(dlt_id_reference)

    # not all parent-child tables have root_key enabled
    root_key = get_first_column_name_with_prop(table, "root_key")
    if root_key:
        root_table = get_root_table(schema.tables, table["name"])
        dlt_root_key_reference = TTableReference(
            columns=[root_key],
            referenced_table=root_table["name"],
            referenced_columns=[row_key_in_parent_table]
        )
        references.append(dlt_root_key_reference)


    columns = []
    for column in table["columns"].values():
        if column.get("merge_key"):
            columns.append(column["name"])

    if columns:
        # TODO use "denormalized" name
        referenced_table_name, _, _ = table["name"].rpartition("__")
        referenced_table = schema.get_table(referenced_table_name)

        # TODO doesn't cover scenario when a table has different keys to different tables
        referenced_columns = [
            column_name
            for column_name, column in referenced_table["columns"].items()
            if column.get("primary_key")
        ]
        reference = TTableReference(
            columns=columns,
            referenced_table=referenced_table_name,
            referenced_columns=referenced_columns,
        )
        references.append(reference)

    return references


def view(schema: Schema) -> str:
    rendered = f"digraph {schema.name}"
    rendered += """ {
    rankdir=LR;
    graph [fontname="helvetica", fontsize=32, fontcolor="{TABLE_BORDER_COLOR}"];
    node [penwidth=0, margin=0, fontname="helvetica", fontsize=32];
    edge [fontname="helvetica", fontsize=32, fontcolor="{TABLE_BORDER_COLOR}", color="{TABLE_BORDER_COLOR}"];

"""
    for table in schema.data_tables():
        rendered += INDENT + get_table_node(table)
        for reference in determine_references(table, schema):
            referenced_table = schema.get_table(reference["referenced_table"])
            rendered += get_table_references_edges(reference, table, referenced_table)

    rendered += "}"
    return rendered


def generate_column_ports(dbml_schema) -> dict[tuple[str, str], str]:
    ports = {}
    for table in dbml_schema["tables"].values():
        table_identifier = table["table_name"]
        for column_idx, column in enumerate(table["columns"]):
            column_name = column["column_name"]
            ports[(table_identifier, column_name)] = column_idx + 1

    return ports


def render(schema: Schema, path: str):
    dot = view(schema)
    graphviz.Source(source=dot).render(path, format="png")


if __name__ == "__main__":
    import pathlib
    import graphviz

    base_path = pathlib.Path("/home/tjean/projects/dlthub/dlt/.vscode/_schema")
    examples = ["ethereum", "employee_data", "warehouse", "dlt_docs"]

    for example in examples:
        schema = SchemaStorage.load_schema_file(
            str(base_path), example, remove_processing_hints=False
        )
        render(schema, path=str(pathlib.Path(base_path, example)))
