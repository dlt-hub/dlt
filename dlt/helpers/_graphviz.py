"""Build a graphviz graph representation using raw strings to avoid dependency on `graphviz`"""

import pathlib
import textwrap
from typing import Any, Union

import dlt
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import (
    C_DLT_LOAD_ID,
    VERSION_TABLE_NAME,
    TColumnSchema,
    TTableSchema,
    TTableReference,
)
from dlt.common.schema.utils import create_load_table_reference, create_parent_child_reference, create_root_child_reference, create_version_and_loads_hash_reference, create_version_and_loads_schema_name_reference, get_first_column_name_with_prop, get_root_table, is_nested_table

# TODO create a TStylesheet specs: alpha background, hints to include
INDENT = "    "
TABLE_HEADER_PORT = "p0"
TABLE_HEADER_COLOR = "#bbca06"
TABLE_HEADER_FONT_COLOR = "#1c1c34"
TABLE_BORDER_COLOR = "#1c1c34"
TABLE_ROW_COLOR = "#e7e2dd"


def _to_column_html(column: TColumnSchema, column_idx: int) -> str:
    """Generate a segment of the node label using Graphviz HTML-like language

    ref: https://graphviz.org/doc/info/shapes.html#html
    """
    column_name = column.get("name")
    if column.get("primary_key") is True:
        column_name = f"<B>{column_name}🔑</B>"

    column_type = f'{column.get("data_type")}'
    if column.get("nullable") is False:
        column_type += " <B>NN</B>"

    return f"""<tr>
            <td align="left" port="f{column_idx}" bgcolor="{TABLE_ROW_COLOR}">
                <table cellpadding="0" cellspacing="0" border="0">
                    <tr>
                        <td align="left">{column_name}</td>
                        <td align="right"><font>{column_type}</font></td>
                    </tr>
                </table>
            </td>
        </tr>"""


def _get_table_header(table_name: str) -> str:
    return f"""\
        <tr>
            <td port="{TABLE_HEADER_PORT}" bgcolor="{TABLE_HEADER_COLOR}">
                <font color="{TABLE_HEADER_FONT_COLOR}"><b>{table_name}</b></font>
            </td>
        </tr>
"""


def _to_table_html(table: TTableSchema) -> str:
    """Generate node label using Graphviz HTML-like language

    ref: https://graphviz.org/doc/info/shapes.html#html
    """
    table_header = _get_table_header(table["name"])
    dot_columns  = ""
    for idx, column in enumerate(table.get("columns", {}).values()):
        dot_columns += _to_column_html(column, idx + 1)

    # table header followed by columns
    return f"""\
    <table border="0" color="{TABLE_BORDER_COLOR}" cellborder="1" cellspacing="0" cellpadding="6">
        {table_header}
        {dot_columns}
    </table>
"""


def _to_dot_table(table: TTableSchema) -> str:
    """Generate graph nodes representing tables and columns"""
    name = table.get("name")
    description = f'tooltip="{table.get("description")}";' if table.get("description") else " "
    label = _to_table_html(table)
    return f'"{name}" [id="{name}";{description}label=<\n{label}>];\n\n'


# TODO handle multi-column references
def _to_dot_reference(
    schema: dlt.Schema,
    *,
    from_table_name: str,
    reference: TTableReference,
    cardinality = "both",
) -> str:
    from_table = schema.get_table(from_table_name)
    from_table_port = f"{from_table_name}:{TABLE_HEADER_PORT}"
    from_column_name = reference["columns"][0]
    # indexing starts at 1 because 0 is table header
    from_column_idx = 1 + list(from_table["columns"].keys()).index(from_column_name)
    from_column_port = f"{from_table_name}:f{from_column_idx}"

    to_table_name = reference["referenced_table"]
    to_table = schema.get_table(to_table_name)
    to_table_port = f"{to_table_name}:{TABLE_HEADER_PORT}"
    to_column_name = reference["referenced_columns"][0]
    # indexing starts at 1 because 0 is table header
    to_column_idx = 1 + list(to_table["columns"].keys()).index(to_column_name)
    to_column_port = f"{to_table_name}:f{to_column_idx}"
    
    # TODO properly handle cardinality via arrowtail and arrowhead
    # NOTE can change layout by specifying port position: n, ne, e, se, s, sw, w, nw, w, c, _
    return textwrap.dedent(
        f"""{INDENT}{from_table_port} -> {to_table_port} [style=invis, weight=100, color=red]
    {from_column_port}:_ -> {to_column_port}:_ [dir={cardinality}, penwidth=1, color="{TABLE_BORDER_COLOR}", arrowtail="crow", arrowhead="tee"];
    """)


# NOTE if changing `rankdir` to `TB`, need to change how edges are built
def _get_graph_header(schema_name: str) -> str:
    # don't forget the double `{{` to escape `{` inside an f-string; your IDE might wrongly complain
    return f"""digraph {schema_name} {{
    rankdir=LR;
    graph [fontname="helvetica", fontcolor="{{TABLE_BORDER_COLOR}}"];
    node [penwidth=0, margin=0, fontname="helvetica"];
    edge [fontname="helvetica", fontcolor="{{TABLE_BORDER_COLOR}}", color="{{TABLE_BORDER_COLOR}}"];

"""


def _add_tables(schema: dlt.Schema, graphviz_dot: str, *, include_dlt_tables: bool) -> str:
    """Append a DOT table for each dlt table and return the updated DOT string.

    The DOT string will be in an invalid state until the graph definition is closed by `}`
    """
    tables: list[TTableSchema]
    if include_dlt_tables is True:
        # the unpacking order ensures dlt tables are at the end
        tables = [*schema.data_tables(), *schema.dlt_tables()]
    else:
        tables = schema.dlt_tables()

    for table in tables:
        if not table.get("columns"):
            continue

        dot_table = _to_dot_table(table)
        graphviz_dot += dot_table

    return graphviz_dot


def _add_references(
    schema: dlt.Schema,
    graphviz_dot: str,
    *,
    include_internal_dlt_ref: bool,
    include_parent_child_ref: bool,
    include_root_child_ref: bool,
) -> str:
    """Append a DOT referebce for each dlt table and return the updated DOT string.

    The DOT string will be in an invalid state until the graph definition is closed by `}`
    """
    for table in schema.tables.values():
        table_name: str = table["name"]
        for reference in table.get("references", []):
            # user-defined references can have arbitrary cardinality and may incorrectly describe the data
            graphviz_dot += _to_dot_reference(
                schema,
                from_table_name=table_name,
                reference=reference,
                cardinality="both",  # because it's the loosest
            )

        # link root -> loads table
        if include_internal_dlt_ref is True and bool(table["columns"].get(C_DLT_LOAD_ID)):
            # root table contains 1 to many rows associated with a single row in loads table
            # possible cardinality: `-` (1-to-1) or `>` (m-to-1)
            graphviz_dot += _to_dot_reference(
                schema,
                from_table_name=table_name,
                reference=create_load_table_reference(table),
                # cardinality=">",  # m-to-1
            )

        # link child -> parent
        if include_parent_child_ref is True and is_nested_table(table):
            # child table contains 1 to many rows associated with has a single row in parent table
            # possible cardinality: `-` (1-to-1) or `>` (m-to-1)
            graphviz_dot += _to_dot_reference(
                schema,
                from_table_name=table_name,
                reference=create_parent_child_reference(schema.tables, table_name),
                # cardinality=">",  # m-to-1
            )

        # link child -> root
        if (
            include_root_child_ref is True
            and is_nested_table(table)
            # the table must have a root key column; can be enabled via `@dlt.source(root_key=True)` or write_disposition
            and get_first_column_name_with_prop(table, "root_key")
        ):
            # child table contains 1 to many rows associated with has a single row in root table
            # possible cardinality: `-` (1-to-1) or `>` (m-to-1)
            graphviz_dot += _to_dot_reference(
                schema,
                from_table_name=table_name,
                reference=create_root_child_reference(schema.tables, table_name),
            )

    # generate links between internal dlt tables
    if include_internal_dlt_ref is True:
        # a schema version hash can have multiple runs in the loads table
        # schema version hash is unique
        # possible: cardinality: `-` (1-to-1) or `<` (1-to-m)
        graphviz_dot += _to_dot_reference(
            schema,
            from_table_name=VERSION_TABLE_NAME,
            reference=create_version_and_loads_hash_reference(schema.tables),
            # cardinality="<",
        )
        # a schema name can have multiple multiple runs in the loads table
        # schema name is not unique; it can have multiple version hash
        # possible: cardinality: `-` (1-to-1), `<` (1-to-m), or `<>` (m-to-n)
        graphviz_dot += _to_dot_reference(
            schema,
            from_table_name=VERSION_TABLE_NAME,
            reference=create_version_and_loads_schema_name_reference(schema.tables),
            # cardinality="<>",
        )

    return graphviz_dot


# most of this module reuses logic from dbml renderer
def schema_to_graphviz(
    schema: dlt.Schema,
    *,
    include_dlt_tables: bool = True,
    include_internal_dlt_ref: bool = True,
    include_parent_child_ref: bool = True,
    include_root_child_ref: bool = True,
    group_by_resource: bool = False,
    **kwargs: Any,
) -> str:
    # TODO use the cluster attribute to group tables by resource: https://www.graphviz.org/docs/clusters/
    graphviz_dot = _get_graph_header(schema.name)
    graphviz_dot = _add_tables(schema, graphviz_dot, include_dlt_tables=include_dlt_tables)
    graphviz_dot = _add_references(
        schema,
        graphviz_dot,
        include_internal_dlt_ref=include_internal_dlt_ref,
        include_parent_child_ref=include_parent_child_ref,
        include_root_child_ref=include_root_child_ref,
    )

    graphviz_dot += "}"
    return graphviz_dot


def render_with_graphviz(
    schema: dlt.Schema,
    *,
    path: Union[pathlib.Path, str],
    **schema_to_graphviz_kwargs: Any,
) -> None:
    try:
        import graphviz
    except ModuleNotFoundError:
        raise MissingDependencyException("graphviz", ["graphviz"])
    
    path = pathlib.Path(path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    file_suffix = str(path.with_suffix(""))

    graphviz_dot = schema_to_graphviz(schema, **schema_to_graphviz_kwargs)
    dot = graphviz.Source(source=graphviz_dot)
    # TODO allow passing kwargs to `render()`
    dot.render(path, format=file_suffix, cleanup=True)
