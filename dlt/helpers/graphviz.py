"""Build a graphviz graph representation using raw strings to avoid dependency on `graphviz`"""

import pathlib
import textwrap
from typing import Union

import dlt
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import (
    C_DLT_LOAD_ID,
    VERSION_TABLE_NAME,
    TColumnSchema,
    TSchemaTables,
    TStoredSchema,
    TTableSchema,
    TTableReference,
)
from dlt.common.schema.utils import create_load_table_reference, create_parent_child_reference, create_root_child_reference, create_version_and_loads_hash_reference, create_version_and_loads_schema_name_reference, get_data_and_dlt_tables, get_first_column_name_with_prop, group_tables_by_resource, is_nested_table

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


def _to_dot_reference(
    *,
    from_table_name: str,
    reference: TTableReference,
    tables: TSchemaTables,
    cardinality = "both",
) -> str:
    from_table = tables.get(from_table_name)
    from_table_port = f"{from_table_name}:{TABLE_HEADER_PORT}"
    from_column_name = reference["columns"][0]
    # indexing starts at 1 because 0 is table header
    from_column_idx = 1 + list(from_table["columns"].keys()).index(from_column_name)
    from_column_port = f"{from_table_name}:f{from_column_idx}"

    to_table_name = reference["referenced_table"]
    to_table = tables.get(to_table_name)
    to_table_port = f"{to_table_name}:{TABLE_HEADER_PORT}"
    to_column_name = reference["referenced_columns"][0]
    # indexing starts at 1 because 0 is table header
    to_column_idx = 1 + list(to_table["columns"].keys()).index(to_column_name)
    to_column_port = f"{to_table_name}:f{to_column_idx}"
    
    # TODO properly handle cardinality via arrowtail and arrowhead
    # NOTE can change layout by specifying port position: n, ne, e, se, s, sw, w, nw, w, c, _
    return textwrap.dedent(
        # we need invisible edges between tables with large weight to guide the layout
        f"""{INDENT}{from_table_port} -> {to_table_port} [style=invis]
    {from_column_port}:_ -> {to_column_port}:_ [dir={cardinality}, penwidth=1, color="{TABLE_BORDER_COLOR}", arrowtail="vee", arrowhead="dot"];
    """)


# NOTE if changing `rankdir` to `TB`, need to change how edges are built
def _get_graph_header(schema_name: str) -> str:
    # don't forget the double `{{` to escape `{` inside an f-string; your IDE might wrongly complain
    """
    ref for graph layout: https://graphviz.org/pdf/dot.1.pdf
    Good options:
    - `layout="twopi", ranksep=5, root="_dlt_loads"`
        `ranksep` should be increased to accomodate large tables
        `root` can be changed to a more central node
    - `layout=circo` is efficient, but will have edges overlapping
    - `layout=fdp` will minimize overlap while reducing sprawl, but odd layouts
    
    Another important property is how stable the layout is when modifying the 
    graph.
    """
    return f"""digraph {schema_name} {{
    rankdir=LR;
    graph [fontname="helvetica", fontcolor="{{TABLE_BORDER_COLOR}}", layout="twopi", ranksep=5, root="_dlt_loads"];
    node [penwidth=0, margin=0, fontname="helvetica"];
    edge [fontname="helvetica", fontcolor="{{TABLE_BORDER_COLOR}}", color="{{TABLE_BORDER_COLOR}}"];

"""


def _add_tables(schema: TStoredSchema, graphviz_dot: str, *, include_dlt_tables: bool) -> str:
    """Append a DOT table for each dlt table and return the updated DOT string.

    The DOT string will be in an invalid state until the graph definition is closed by `}`
    """
    data_tables, dlt_tables = get_data_and_dlt_tables(schema["tables"])
    
    if include_dlt_tables is True:
        # order allows to keep _dlt tables at the end
        tables = data_tables + dlt_tables
    else:
        tables = data_tables

    for table in tables:
        if not table.get("columns"):
            continue

        dot_table = _to_dot_table(table)
        graphviz_dot += dot_table

    return graphviz_dot


def _get_cluster_header(cluster_idx: int, resource_name: str) -> str:
    # don't forget the double `{{` to escape `{` inside an f-string; your IDE might wrongly complain
    # to create a cluster, the subgraph name
    # must have the `cluster_` prefix
    return f"""subgraph cluster_{cluster_idx} {{
    label="{resource_name}"

"""


def _add_table_clusters(schema: TStoredSchema, graphviz_dot: str, *, include_dlt_tables: bool) -> str:
    _, dlt_tables = get_data_and_dlt_tables(schema["tables"])

    cluster_idx = 0
    for cluster_idx, (resource, tables) in enumerate(group_tables_by_resource(schema["tables"]).items()):
        if resource in ["_dlt_loads", "_dlt_version", "_dlt_pipeline_state"]:
            continue
        
        graphviz_dot += _get_cluster_header(cluster_idx, resource_name=resource)
        for table in tables:
            if not table.get("columns"):
                continue
            graphviz_dot += INDENT + _to_dot_table(table)
        
        graphviz_dot += "}"

    if include_dlt_tables:
        graphviz_dot += _get_cluster_header(cluster_idx + 1, resource_name="_dlt")
        for table in dlt_tables:
            if not table.get("columns"):
                continue

            graphviz_dot += INDENT + _to_dot_table(table)
        
        graphviz_dot += "}"            

    return graphviz_dot


def _add_references(
    schema: TStoredSchema,
    graphviz_dot: str,
    *,
    include_internal_dlt_ref: bool,
    include_parent_child_ref: bool,
    include_root_child_ref: bool,
) -> str:
    """Append a DOT reference for each dlt table and return the updated DOT string.

    The DOT string will be in an invalid state until the graph definition is closed by `}`
    """
    tables: TSchemaTables = schema["tables"]
    for table in tables.values():
        table_name: str = table["name"]
        for reference in table.get("references", []):
            # user-defined references can have arbitrary cardinality and may incorrectly describe the data
            graphviz_dot += _to_dot_reference(
                from_table_name=table_name,
                reference=reference,
                tables=tables,
                cardinality="both",  # because it's the loosest
            )

        # link root -> loads table
        if include_internal_dlt_ref is True and bool(table["columns"].get(C_DLT_LOAD_ID)):
            # root table contains 1 to many rows associated with a single row in loads table
            # possible cardinality: `-` (1-to-1) or `>` (m-to-1)
            graphviz_dot += _to_dot_reference(
                from_table_name=table_name,
                reference=create_load_table_reference(table),
                tables=tables,
                # cardinality=">",  # m-to-1
            )

        # link child -> parent
        if include_parent_child_ref is True and is_nested_table(table):
            # child table contains 1 to many rows associated with has a single row in parent table
            # possible cardinality: `-` (1-to-1) or `>` (m-to-1)
            graphviz_dot += _to_dot_reference(
                from_table_name=table_name,
                reference=create_parent_child_reference(tables, table_name),
                tables=tables,
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
                from_table_name=table_name,
                reference=create_root_child_reference(tables, table_name),
                tables=tables,
            )

    # generate links between internal dlt tables
    if include_internal_dlt_ref is True:
        # a schema version hash can have multiple runs in the loads table
        # schema version hash is unique
        # possible: cardinality: `-` (1-to-1) or `<` (1-to-m)
        graphviz_dot += _to_dot_reference(
            from_table_name=VERSION_TABLE_NAME,
            reference=create_version_and_loads_hash_reference(tables),
            tables=tables,
            # cardinality="<",
        )
        # a schema name can have multiple multiple runs in the loads table
        # schema name is not unique; it can have multiple version hash
        # possible: cardinality: `-` (1-to-1), `<` (1-to-m), or `<>` (m-to-n)
        graphviz_dot += _to_dot_reference(
            from_table_name=VERSION_TABLE_NAME,
            reference=create_version_and_loads_schema_name_reference(tables),
            tables=tables,
            # cardinality="<>",
        )

    return graphviz_dot


# most of this module reuses logic from dbml renderer
def schema_to_graphviz(
    schema: TStoredSchema,
    *,
    include_dlt_tables: bool = True,
    include_internal_dlt_ref: bool = True,
    include_parent_child_ref: bool = True,
    include_root_child_ref: bool = True,
    group_by_resource: bool = False,
) -> str:
    # TODO use the cluster attribute to group tables by resource: https://www.graphviz.org/docs/clusters/
    graphviz_dot = _get_graph_header(schema["name"])

    if group_by_resource:
        graphviz_dot = _add_table_clusters(schema, graphviz_dot, include_dlt_tables=include_dlt_tables)
    else:
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


def render_with_html(dot: str) -> str:
    """Render the DOT string using an HTML file with required JS dependencies.

    HTML output automatically gets zoom and pan features
    This allows to render DOT without any Python or system dependencies. However,
    an internet connection is required to load the JS bundles.

    d3-graphviz is used because it's the smallest option (300Kb): https://github.com/magjac/d3-graphviz    
    """

    HTML_TEMPLATE = """
<script src="//d3js.org/d3.v7.min.js"></script>
<script src="https://unpkg.com/@hpcc-js/wasm@2.20.0/dist/graphviz.umd.js"></script>
<script src="https://unpkg.com/d3-graphviz@5.6.0/build/d3-graphviz.js"></script>

<div id="graph" style="width:100%;height:100vh;display:flex;justify-content:center;align-items:center;"></div>
<script>
    d3.select("#graph")
      .graphviz({{fit: true}})
      .renderDot(
        `
        {dot}
        `
      );
</script>
"""
    return HTML_TEMPLATE.format(dot=dot)


def render_with_graphviz(
    obj: Union[dlt.Schema, TStoredSchema, dlt.Pipeline, dlt.Dataset],
    *,
    path: Union[pathlib.Path, str],
    include_dlt_tables: bool = True,
    include_internal_dlt_ref: bool = True,
    include_parent_child_ref: bool = True,
    include_root_child_ref: bool = True,
    group_by_resource: bool = False,
) -> None:
    try:
        import graphviz
    except ModuleNotFoundError:
        raise MissingDependencyException("graphviz", ["graphviz"])
    
    if isinstance(obj, dlt.Schema):
        stored_schema = obj.to_dict()
    # assuming this is a `TStoredSchema`; can't check isinstance against TypedDict
    elif isinstance(obj, dict):
        stored_schema = obj
    elif isinstance(obj, dlt.Pipeline):
        stored_schema = obj.default_schema.to_dict()
    elif isinstance(obj, dlt.Dataset):  # TODO fix this check
        stored_schema = obj.schema.to_dict()
    else:
        raise TypeError
    
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    file_suffix = path.suffix

    graphviz_dot = schema_to_graphviz(
        stored_schema,
        include_dlt_tables=include_dlt_tables,
        include_internal_dlt_ref=include_internal_dlt_ref,
        include_parent_child_ref=include_parent_child_ref,
        include_root_child_ref=include_root_child_ref,
        group_by_resource=group_by_resource,
    )
    dot = graphviz.Source(source=graphviz_dot)
    # TODO allow passing kwargs to `render()`
    dot.render(path.with_suffix(""), format=file_suffix, cleanup=True)
