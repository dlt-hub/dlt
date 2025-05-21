

import marimo

__generated_with = "0.13.3"
app = marimo.App(
    width="full",
    app_title="dlt studio",
    layout_file="layouts/app2.grid.json",
)

with app.setup:
    # Initialization code that runs before all other cells
    from itertools import chain
    from pathlib import Path
    from typing import Any, cast

    import marimo as mo
    import pyarrow

    import dlt
    from dlt.common.pipeline import get_dlt_pipelines_dir
    from dlt.common.storages import FileStorage
    from dlt.common.utils import without_none

    PICKLE_TRACE_FILE = "trace.pickle"


@app.cell(hide_code=True)
def utilities():
    # TODO can split functions into individual cells to create reusable_functions
    # though, this complexifies the graph

    def _align_dict_keys(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Makes sure all dicts have the same keys, sets "-" as default. Makes for nicer rendering in marimo table
        """
        items = cast(list[dict[str, Any]], [without_none(i) for i in items])
        all_keys = set(chain.from_iterable(i.keys() for i in items))
        for i in items:
            i.update({key: "-" for key in all_keys if key not in i})
        return items


    def style_cell(row_id: str, name: str, __: Any) -> dict[str, str]:
        """
        Style a cell in a table.

        Args:
            row_id (str): The id of the row.
            name (str): The name of the column.
            __ (Any): The value of the cell.

        Returns:
            Dict[str, str]: The css style of the cell.
        """
        style = {}  #"background-color": "white" if (int(row_id) % 2 == 0) else "#f4f4f9"}
        if name.lower() == "name":
            style["font-weight"] = "bold"
        return style


    @mo.cache
    def create_table_list(
        pipeline: dlt.Pipeline,
        show_internals: bool = False,
        show_child_tables: bool = True,
    ) -> list[dict[str, str]]:
        """Create a list of tables for the pipeline.

        Args:
            pipeline_name (str): The name of the pipeline to create the table list for.
        """
        FILL_VALUE = "-"

        # get tables and filter as needed
        tables = list(pipeline.default_schema.tables.values())
        if not show_child_tables:
            tables = [t for t in tables if t.get("parent") is None]

        table_list = []
        keys = ("name", "parent", "resource", "write_disposition", "description")
        for table in tables:
            info = {key.capitalize().replace("_", " "): table.get(key, FILL_VALUE) for key in keys}
            table_list.append(info)

        table_list.sort(key=lambda x: str(x["Name"]))
        if not show_internals:
            table_list = [t for t in table_list if not str(t["Name"]).lower().startswith("_dlt")]

        return _align_dict_keys(table_list)


    def get_local_pipelines(
        pipelines_dir: str = None,
        sort_by_trace: bool = True
    ) -> tuple[str, list[dict[str, Any]]]:
        """Get the local pipelines directory and the list of pipeline names in it.

        Args:
            pipelines_dir (str, optional): The local pipelines directory. Defaults to get_dlt_pipelines_dir().
            sort_by_trace (bool, optional): Whether to sort the pipelines by the latet timestamp of trace. Defaults to True.
        Returns:
            Tuple[str, List[str]]: The local pipelines directory and the list of pipeline names in it.
        """
        pipelines_dir = pipelines_dir or get_dlt_pipelines_dir()
        storage = FileStorage(pipelines_dir)

        try:
            pipelines = storage.list_folder_dirs(".", to_root=False)
        except Exception:
            pipelines = []

        # check last trace timestamp and create dict
        pipelines_with_timestamps = []
        for pipeline in pipelines:
            trace_file = Path(pipelines_dir) / pipeline / PICKLE_TRACE_FILE
            if trace_file.exists():
                pipelines_with_timestamps.append(
                    {"name": pipeline, "timestamp": trace_file.stat().st_mtime}
                )
            else:
                pipelines_with_timestamps.append({"name": pipeline, "timestamp": 0})

        pipelines_with_timestamps.sort(key=lambda x: cast(float, x["timestamp"]), reverse=True)

        return pipelines_dir, pipelines_with_timestamps


    @mo.cache
    def create_column_list(
        pipeline: dlt.Pipeline,
        table_name: str,
        show_internals: bool = False,
        show_type_hints: bool = True,
        show_other_hints: bool = False,
        show_custom_hints: bool = False,
    ) -> list[dict[str, Any]]:
        """Create a list of columns for a table.

        Args:
            pipeline_name (str): The name of the pipeline to create the column list for.
            table_name (str): The name of the table to create the column list for.
        """
        table = pipeline.default_schema.tables[table_name]

        column_list: list[dict[str, Any]] = []
        for column in table["columns"].values():
            column_dict: dict[str, Any] = {
                "Name": column["name"],
            }

            # show type hints if requested
            if show_type_hints:
                column_dict["Data Type"] = column.get("data_type", None)
                column_dict["Nullable"] = column.get("nullable", None)
                column_dict["Precision"] = column.get("precision", None)
                column_dict["Scale"] = column.get("scale", None)
                column_dict["Timezone"] = column.get("timezone", None)

            # show "other" hints if requested, TODO: define what are these?
            if show_other_hints:
                column_dict["Primary Key"] = column.get("primary_key", None)
                column_dict["Merge Key"] = column.get("merge_key", None)
                column_dict["Unique"] = column.get("unique", None)

            # show custom hints (x-) if requesed
            if show_custom_hints:
                for key in column:
                    if key.startswith("x-"):
                        column_dict[key] = column[key]  # type: ignore

            column_list.append(column_dict)

        column_list.sort(key=lambda x: x["Name"])
        if not show_internals:
            column_list = [c for c in column_list if not c["Name"].lower().startswith("_dlt")]

        return _align_dict_keys(column_list)

    def pipeline_details(pipeline: dlt.Pipeline) -> list[dict[str, Any]]:
        """
        Get the details of a pipeline.
        """
        try:
            credentials = str(pipeline.dataset().destination_client.config.credentials)
        except Exception:
            credentials = "Could not resolve credentials"

        details_dict = {
            "Pipeline name": pipeline.pipeline_name,
            "Destination": (
                pipeline.destination.destination_description
                if pipeline.destination
                else "No destination set"
            ),
            "Credentials": credentials,
            "Dataset name": pipeline.dataset_name,
            "Schema name": (
                pipeline.default_schema_name
                if pipeline.default_schema_name
                else "No default schema set"
            ),
            "Pipeline working dir": pipeline.working_dir,
        }

        return [{"Key": k, "Value": v} for k, v in details_dict.items()]


    return (
        create_column_list,
        create_table_list,
        get_local_pipelines,
        pipeline_details,
        style_cell,
    )


@app.cell
def pipeline_selection(get_local_pipelines):
    dlt_pipelines_dir, pipelines = get_local_pipelines(None)
    dlt_pipeline_select = mo.ui.dropdown(
        options=[p["name"] for p in pipelines],
        label="Pipeline: ",
        value=pipelines[0]["name"]
    )
    return dlt_pipeline_select, dlt_pipelines_dir


@app.cell
def get_pipeline(dlt_pipeline_select, dlt_pipelines_dir):
    pipeline = dlt.attach(
        pipeline_name=dlt_pipeline_select.value,
        pipelines_dir=dlt_pipelines_dir
    )
    mo.stop(not pipeline.default_schema_name)
    return (pipeline,)


@app.cell
def home(dlt_pipeline_select):
    app_header =  mo.hstack(
        [
            mo.image(
                "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
            ).style(padding_bottom="1em"),
            dlt_pipeline_select,
        ],
        gap=2,
        justify="start",
    )
    app_header
    return


@app.cell
def sync_panel(pipeline, pipeline_details, style_cell):
    # sync pipeline
    with mo.status.spinner(title="Syncing pipeline state from destination..."):
        try:
            pipeline.sync_destination()
            sync_status = mo.md("# Sync status ✅")

        except Exception:
            # sync_result = ui_elements.build_error_callout(strings.pipeline_sync_error_text)
            sync_status = mo.md("# Sync status ❌")

    sync_table = mo.ui.table(
        pipeline_details(pipeline),
        selection=None,
        style_cell=style_cell,
        show_download=False,
    )

    sync_panel = mo.md(
        f"""
        {sync_status}

        {sync_table}
        """
    )

    sync_panel
    return


@app.cell
def _(pipeline):
    """
    Show state of the currently selected pipeline
    """
    from dlt.common.json import json as _json

    state_dict = mo.json(pipeline.state, label=pipeline.pipeline_name)

    state_json = mo.ui.code_editor(
        _json.dumps(pipeline.state, pretty=True),
        language="json",
        label=f"`{pipeline.pipeline_name}` state",
    )

    state_format_tabs = mo.ui.tabs(
        {"dict": state_dict, "JSON": state_json},
    )

    state_panel = mo.accordion({f"Pipeline state: `{pipeline.pipeline_name}`": state_format_tabs})

    state_panel
    return


@app.cell
def main_schema_controls():
    dlt_main_schema_show_dlt_tables = mo.ui.checkbox(label="<small>`_dlt` tables</small>")
    dlt_main_schema_show_child_tables = mo.ui.checkbox(
        label="<small>Child tables</small>", value=True
    )
    return dlt_main_schema_show_child_tables, dlt_main_schema_show_dlt_tables


@app.cell
def schema_panel(
    create_table_list,
    dlt_main_schema_show_child_tables,
    dlt_main_schema_show_dlt_tables,
    pipeline,
    style_cell,
):
    """
    Show schema of the currently selected pipeline
    """
    # TODO handle when pipeline.default_schema_name is None

    schema_view_controls = mo.hstack([dlt_main_schema_show_dlt_tables, dlt_main_schema_show_child_tables], justify="start")

    table_list = create_table_list(
        pipeline,
        show_internals=dlt_main_schema_show_dlt_tables.value,
        show_child_tables=dlt_main_schema_show_child_tables.value,
    )

    dlt_schema_table_list = mo.ui.table(
        table_list,  # type: ignore[arg-type]
        style_cell=style_cell,
        initial_selection=list(range(0, len(table_list))),
    )

    main_schema_panel = mo.md(
        f"""
        # Schema: `{pipeline.default_schema_name}`

        {schema_view_controls}

        {dlt_schema_table_list}
        """
    )
    return dlt_schema_table_list, main_schema_panel


@app.cell
def schema_details_controls():
    """
    Control elements for various parts of the app
    """
    # TODO switch to a form using a dataclass

    dlt_details_show_dlt_columns = mo.ui.checkbox(label="<small>`_dlt` columns</small>")
    dlt_details_show_type_hints = mo.ui.checkbox(label="<small>Type hints</small>", value=True)
    dlt_details_show_other_hints = mo.ui.checkbox(
        label="<small>Other hints</small>", value=False
    )
    dlt_details_show_custom_hints = mo.ui.checkbox(
        label="<small>Custom hints (x-)</small>", value=False
    )

    table_details_controls = mo.hstack(
        [dlt_details_show_dlt_columns, dlt_details_show_type_hints, dlt_details_show_other_hints, dlt_details_show_custom_hints],
        justify="start"
    )

    return (
        dlt_details_show_custom_hints,
        dlt_details_show_dlt_columns,
        dlt_details_show_other_hints,
        dlt_details_show_type_hints,
        table_details_controls,
    )


@app.cell
def _(
    create_column_list,
    dlt_details_show_custom_hints,
    dlt_details_show_dlt_columns,
    dlt_details_show_other_hints,
    dlt_details_show_type_hints,
    dlt_schema_table_list,
    pipeline,
    style_cell,
    table_details_controls,
):
    """
    Show schema of the currently selected table
    """

    _tables_with_details = {
        table["Name"]: mo.ui.table(
            create_column_list(
                pipeline,
                table["Name"],
                show_internals=dlt_details_show_dlt_columns.value,
                show_type_hints=dlt_details_show_type_hints.value,
                show_other_hints=dlt_details_show_other_hints.value,
                show_custom_hints=dlt_details_show_custom_hints.value,
            ),
            selection=None,
            style_cell=style_cell,
            show_download=False,
        )
        for table in dlt_schema_table_list.value
    }


    table_details = mo.vstack([
        mo.md(f"## {name} {details}") for name, details in _tables_with_details.items()
    ], gap=1)

    table_details_panel = mo.md(
        f"""
        {table_details_controls}

        {table_details}
        """
    )
    return (table_details_panel,)


@app.cell
def raw_schema_panel(pipeline):
    raw_schema = mo.lazy(mo.json(pipeline.default_schema.to_dict(), label=pipeline.default_schema_name))

    raw_schema_yaml = mo.lazy(mo.ui.code_editor(
        pipeline.default_schema.to_pretty_yaml(),
        language="yaml",
        label=f"`{pipeline.default_schema_name}` schema",
    ))

    raw_schema_json = mo.lazy(mo.ui.code_editor(
        pipeline.default_schema.to_pretty_json(),
        language="json",
        label=f"`{pipeline.default_schema_name}` schema",
    ))

    raw_schema_format_tabs = mo.ui.tabs(
        {"dict": raw_schema, "JSON": raw_schema_json, "YAML": raw_schema_yaml},
    )

    raw_schema_panel = mo.accordion({f"Raw schema: `{pipeline.default_schema_name}`": raw_schema_format_tabs})
    return (raw_schema_panel,)


@app.cell
def _(
    dlt_schema_table_list,
    main_schema_panel,
    raw_schema_panel,
    table_details_panel,
):
    schema_panel = mo.md(
        f"""
        {main_schema_panel}

        {table_details_panel if dlt_schema_table_list.value else ""}

        {raw_schema_panel}
        """
    )
    schema_panel
    return


@app.cell
def sql_controls():
    dlt_cache_query_results = mo.ui.checkbox(label="<small>Cache query results</small>", value=True)
    return


@app.cell
def sql_editor():
    """
    Show data of the currently selected pipeline
    """
    # TODO autopopulate editor with query
    # TODO we should simply allow the native SQL over IbisEngine; less maintenance

    sql_query_editor = mo.ui.code_editor(
        language="sql",
        placeholder="SELECT \n  * \nFROM dataset.table \nLIMIT 1000",
        debounce=True,
    ).form()
    return (sql_query_editor,)


@app.cell
def _(pipeline, sql_query_editor):
    # pipeline_state is part of the signature for cache key
    @mo.cache
    def execute_query(pipeline, sql_query, pipeline_state):
        try:
            results = pipeline.dataset()(sql_query).arrow()
        except:
            results = pyarrow.table([])
        return results

    _sql_results = execute_query(pipeline, sql_query_editor.value, pipeline.state)

    sql_results_table = mo.lazy(mo.ui.table(_sql_results))
    sql_results_plotter = mo.lazy(mo.ui.data_explorer(_sql_results))

    sql_results_tabs = mo.ui.tabs(
        {
            "results": sql_results_table,
            "plot": sql_results_plotter
        }
    )
    return (sql_results_tabs,)


@app.cell
def _(sql_query_editor, sql_results_tabs):
    sql_query_panel = mo.md(
        f"""
        # SQL query

        {sql_query_editor}

        {sql_results_tabs}
        """
    )
    sql_query_panel
    return


if __name__ == "__main__":
    app.run()
