# flake8: noqa: F841


import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium", app_title="dlt studio", css_file="style.css")

# global imports
with app.setup:
    from typing import Any, Dict, List, Tuple, cast

    import marimo as mo

    import pandas as pd
    import sqlglot

    import dlt
    from dlt.common.json import json
    from dlt.helpers.studio import strings, utils, ui_elements as ui


@app.cell(hide_code=True)
def home(
    dlt_pipelines_dir: str,
    dlt_pipeline_select: marimo.ui.multiselect,
    dlt_all_pipelines: List[Dict[str, Any]],
) -> Any:
    """
    Displays the welcome page with the pipeline select widget, will only display pipeline title if a pipeline is selected
    """

    # provide pipeline object to the following cells
    dlt_pipeline_name: str = (
        str(dlt_pipeline_select.value[0]) if dlt_pipeline_select.value else None
    )
    dlt_pipeline: dlt.Pipeline = None
    if dlt_pipeline_name:
        dlt_pipeline = utils.get_pipeline(dlt_pipeline_name, dlt_pipelines_dir)

    if not dlt_pipeline:
        _stack = [
            mo.hstack(
                [
                    mo.image(
                        "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
                    ),
                    dlt_pipeline_select,
                ],
            ),
            mo.md(strings.app_title).center(),
            mo.md(strings.app_intro).center(),
            mo.callout(
                mo.vstack(
                    [
                        mo.md(
                            strings.app_quick_start_title.format(
                                ui.build_pipeline_link_list(dlt_all_pipelines)
                            )
                        ),
                        dlt_pipeline_select,
                    ]
                ),
                kind="info",
            ),
            mo.md(strings.app_basics_text.format(len(dlt_all_pipelines), dlt_pipelines_dir)),
        ]
    else:
        _stack = [
            mo.hstack(
                [
                    mo.image(
                        "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
                    ).style(padding_bottom="1em"),
                    mo.center(
                        mo.md(f"## {strings.app_title_pipeline.format(dlt_pipeline.pipeline_name)}")
                    ),
                    dlt_pipeline_select,
                ],
            ),
        ]
    mo.vstack(_stack)


@app.cell(hide_code=True)
def section_sync_status(dlt_pipeline: dlt.Pipeline) -> Any:
    """
    Returns the status of the pipeline
    """
    _result = []

    if dlt_pipeline:
        # sync pipeline
        with mo.status.spinner(title="Syncing pipeline state from destination..."):
            try:
                dlt_pipeline.sync_destination()
                _credentials = str(dlt_pipeline.dataset().destination_client.config.credentials)
                _result.append(
                    mo.callout(
                        mo.vstack([mo.md(strings.pipeline_sync_success_text.format(_credentials))]),
                        kind="success",
                    )
                )
            except Exception:
                _result.append(ui.build_error_callout(strings.pipeline_sync_error_text))
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_overview(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_overview: marimo.ui.switch,
) -> Any:
    """
    Overview page of currently selected pipeline
    """

    _result = [
        ui.build_page_header(strings.overview_title, strings.overview_subtitle, dlt_page_overview)
    ]

    if dlt_pipeline and dlt_page_overview.value:
        _result += [
            mo.ui.table(
                utils.pipeline_details(dlt_pipeline),
                selection=None,
                style_cell=utils.style_cell,
            ),
        ]
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_schema_table_list(
    dlt_pipeline: dlt.Pipeline,
    dlt_schema_show_child_tables: marimo.ui.switch,
    dlt_schema_show_dlt_tables: marimo.ui.switch,
    dlt_page_schema: marimo.ui.switch,
) -> Any:
    """
    Show schema of the currently selected pipeline
    """

    _result = [ui.build_page_header(strings.schema_title, strings.schema_subtitle, dlt_page_schema)]

    if dlt_pipeline and dlt_page_schema.value:
        dlt_schem_table_list = None

        if not dlt_pipeline.default_schema_name:
            _result.append(
                mo.callout(
                    mo.md("No Default Schema available. Does your pipeline have a completed load?"),
                    kind="warn",
                )
            )
            _schema_version = "-"

        else:
            _result.append(
                mo.hstack(
                    [dlt_schema_show_dlt_tables, dlt_schema_show_child_tables], justify="start"
                )
            )
            _table_list = utils.create_table_list(
                dlt_pipeline,
                show_internals=dlt_schema_show_dlt_tables.value,
                show_child_tables=dlt_schema_show_child_tables.value,
            )
            dlt_schem_table_list = mo.ui.table(
                _table_list,  # type: ignore[arg-type]
                style_cell=utils.style_cell,
                initial_selection=list(range(0, len(_table_list))),
            )
            _result.append(dlt_schem_table_list)
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_schema_table_details(
    dlt_pipeline: dlt.Pipeline,
    dlt_schem_table_list: marimo.ui.table,
    dlt_schema_show_other_hints: marimo.ui.switch,
    dlt_schema_show_custom_hints: marimo.ui.switch,
    dlt_schema_show_dlt_columns: marimo.ui.switch,
    dlt_schema_show_type_hints: marimo.ui.switch,
    dlt_page_schema: marimo.ui.switch,
) -> Any:
    """
    Show schema of the currently selected table
    """

    _result: List[Any] = []

    if dlt_pipeline and dlt_page_schema.value and dlt_schem_table_list:
        # build table details
        _result.insert(0, mo.md("### Table details for selected tables"))
        _result.insert(
            0,
            mo.hstack(
                [
                    dlt_schema_show_dlt_columns,
                    dlt_schema_show_type_hints,
                    dlt_schema_show_other_hints,
                    dlt_schema_show_custom_hints,
                ],
                justify="start",
            ),
        )

        # build table list
        for table in dlt_schem_table_list.value:  # type: ignore[union-attr]
            _table_name = table["Name"]  # type: ignore[index]
            _result.append(mo.md(f"### `{_table_name}`"))
            _result.append(
                mo.ui.table(
                    utils.create_column_list(
                        dlt_pipeline,
                        _table_name,
                        show_internals=dlt_schema_show_dlt_columns.value,
                        show_type_hints=dlt_schema_show_type_hints.value,
                        show_other_hints=dlt_schema_show_other_hints.value,
                        show_custom_hints=dlt_schema_show_custom_hints.value,
                    ),
                    selection=None,
                    style_cell=utils.style_cell,
                )
            )

        # build raw schema
        _result.append(
            mo.accordion(
                {
                    "Show raw schema as yaml": mo.ui.code_editor(
                        dlt_pipeline.default_schema.to_pretty_yaml(),
                        language="yaml",
                    )
                }
            )
        )
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def section_browse_data_table_list(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_browse_data: marimo.ui.switch,
    dlt_schema_show_child_tables: marimo.ui.switch,
    dlt_schema_show_dlt_tables: marimo.ui.switch,
    dlt_schema_show_row_counts: marimo.ui.switch,
) -> Any:
    """
    Show data of the currently selected pipeline
    """

    _result = [
        ui.build_page_header(
            strings.browse_data_title, strings.browse_data_subtitle, dlt_page_browse_data
        )
    ]

    if dlt_pipeline and dlt_page_browse_data.value:
        _result.append(
            mo.hstack(
                [
                    dlt_schema_show_dlt_tables,
                    dlt_schema_show_child_tables,
                    dlt_schema_show_row_counts,
                ],
                justify="start",
            ),
        )

        # try to connect to the dataset
        dlt_data_table_list = None
        try:
            dlt_pipeline.dataset().destination_client.config.credentials
            with mo.status.spinner(title="Getting table list..."):
                dlt_data_table_list = mo.ui.table(
                    utils.create_table_list(  # type: ignore[arg-type]
                        dlt_pipeline,
                        show_internals=dlt_schema_show_dlt_tables.value,
                        show_child_tables=dlt_schema_show_child_tables.value,
                        show_row_counts=dlt_schema_show_row_counts.value,
                    ),
                    style_cell=utils.style_cell,
                    selection="single",
                )
                _result.append(dlt_data_table_list)
        except Exception:
            _result.append(ui.build_error_callout(strings.browse_data_error))
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_browse_data_query_editor(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_browse_data: marimo.ui.switch,
    dlt_data_table_list: marimo.ui.table,
    dlt_cache_query_results: marimo.ui.switch,
    dlt_execute_query_on_change: marimo.ui.switch,
    dlt_restrict_to_last_1000: marimo.ui.switch,
) -> Any:
    """
    Show data of the currently selected pipeline
    """

    _result = []

    if dlt_pipeline and dlt_page_browse_data.value and dlt_data_table_list:
        _sql_query = ""
        if dlt_data_table_list.value:
            _table_name = dlt_data_table_list.value[0]["Name"]  # type: ignore[index]
            _sql_query = (
                dlt_pipeline.dataset()
                .table(_table_name)
                .limit(1000 if dlt_restrict_to_last_1000.value else None)
                .query()
            )

        dlt_query_editor = mo.ui.code_editor(
            language="sql",
            placeholder="SELECT \n  * \nFROM dataset.table \nLIMIT 1000",
            value=_sql_query,
            debounce=True,
        )
        dlt_run_query_button = mo.ui.run_button(
            label="Run query", tooltip="Run the query in the editor"
        )

        _result += [
            mo.md(strings.browse_data_explorer_title),
            mo.hstack(
                [dlt_cache_query_results, dlt_execute_query_on_change, dlt_restrict_to_last_1000],
                justify="start",
            ),
            dlt_query_editor,
        ]

        if not dlt_execute_query_on_change.value:
            _result.append(dlt_run_query_button)
        else:
            _result.append(
                mo.md(
                    "<small>Query will be executed automatically when you select a table or change"
                    " the query</small>"
                )
            )
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_browse_data_execute_query(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_browse_data: marimo.ui.switch,
    dlt_run_query_button: marimo.ui.button,
    dlt_query_editor: marimo.ui.code_editor,
    dlt_execute_query_on_change: marimo.ui.switch,
    dlt_data_table_list: marimo.ui.table,
) -> Any:
    """
    Execute the query in the editor
    """

    _result = []

    if dlt_pipeline and dlt_page_browse_data.value and dlt_data_table_list:
        dlt_query_result = None
        with mo.status.spinner(title="Loading data from destination"):
            if dlt_query_editor.value and (
                dlt_run_query_button.value or dlt_execute_query_on_change.value
            ):
                try:
                    sqlglot.parse_one(
                        dlt_query_editor.value,
                        dialect=dlt_pipeline.destination.capabilities().sqlglot_dialect,
                    )
                    dlt_query = dlt_query_editor.value

                    dlt_query_result = utils.get_query_result(dlt_pipeline, dlt_query)
                except Exception as e:
                    _result.append(
                        ui.build_error_callout(strings.browse_data_query_error, code=str(e))
                    )

        # always show last query if nothing was found
        if dlt_query_result is None:
            dlt_query_result = utils.get_last_query_result(dlt_pipeline)
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_browse_data_data_explorer(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_browse_data: marimo.ui.switch,
    dlt_data_table_list: marimo.ui.table,
    dlt_query_result: pd.DataFrame,
) -> Any:
    """
    Show data of the currently selected pipeline
    """

    _result: List[Any] = []
    if dlt_pipeline and dlt_page_browse_data.value and dlt_data_table_list:
        dlt_query_history_table = None
        _query_history = utils.get_query_history(dlt_pipeline)
        dlt_query_history_table = mo.ui.table(_query_history)

        _result += [
            mo.md(
                strings.browse_data_query_result_title.format(utils.get_last_query(dlt_pipeline))
            ),
            mo.ui.table(dlt_query_result, selection=None),
            mo.md(strings.browse_data_query_history_title),
            dlt_query_history_table,
        ]
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_browse_data_cached_query(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_browse_data: marimo.ui.switch,
    dlt_query_history_table: marimo.ui.table,
    dlt_data_table_list: marimo.ui.table,
) -> Any:
    _result: List[Any] = []
    if dlt_pipeline and dlt_page_browse_data.value and dlt_data_table_list:
        for _r in dlt_query_history_table.value:  # type: ignore
            _query = _r["Query"]  # type: ignore
            _q_result = utils.get_query_result(dlt_pipeline, _query)
            _result.append(mo.md(f"<small>`{_query}`</small>"))
            _result.append(mo.ui.table(_q_result, selection=None))
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_state(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_state: marimo.ui.switch,
) -> Any:
    """
    Show state of the currently selected pipeline
    """
    _result = [ui.build_page_header(strings.state_title, strings.state_subtitle, dlt_page_state)]

    if dlt_pipeline and dlt_page_state.value:
        _result.append(
            mo.ui.code_editor(
                json.dumps(dlt_pipeline.state, pretty=True),
                language="json",
            ),
        )
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_trace(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_trace: marimo.ui.switch,
) -> Any:
    """
    Show last trace of the currently selected pipeline
    """

    _result = [ui.build_page_header(strings.trace_title, strings.trace_subtitle, dlt_page_trace)]

    if dlt_pipeline and dlt_page_trace.value:
        dlt_trace = dlt_pipeline.last_trace
        if not dlt_trace:
            _result.append(
                mo.callout(
                    mo.md(strings.last_trace_no_trace),
                    kind="warn",
                )
            )
        else:
            _result.append(mo.md("### Execution context"))
            _result.append(
                mo.ui.table(utils.trace_execution_context(dlt_trace.asdict()), selection=None)
            )
            _result.append(mo.md("### Steps overview"))
            _result.append(
                mo.ui.table(utils.trace_steps_overview(dlt_trace.asdict()), selection="single")
            )
            _result.append(mo.md("### Raw trace"))
            _result.append(
                mo.accordion(
                    {
                        strings.trace_raw_title: mo.ui.code_editor(
                            json.dumps(dlt_trace, pretty=True),
                            language="json",
                        )
                    }
                )
            )
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_loads(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_loads: marimo.ui.switch,
    dlt_cache_query_results: marimo.ui.switch,
    dlt_restrict_to_last_1000: marimo.ui.switch,
) -> Any:
    """
    Show loads of the currently selected pipeline
    """

    _result = [ui.build_page_header(strings.loads_title, strings.loads_subtitle, dlt_page_loads)]

    if dlt_pipeline and dlt_page_loads.value:
        _result.append(
            mo.hstack([dlt_cache_query_results, dlt_restrict_to_last_1000], justify="start")
        )

        with mo.status.spinner(title="Loading loads from destination..."):
            try:
                _loads_data = utils.get_loads(
                    dlt_pipeline, limit=1000 if dlt_restrict_to_last_1000.value else None
                )
                dlt_loads_table = mo.ui.table(_loads_data, selection="single")
                _result.append(dlt_loads_table)
            except Exception:
                _result.append(ui.build_error_callout(strings.loading_load_failes))
                dlt_loads_table = None
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_loads_details(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_loads: marimo.ui.switch,
    dlt_loads_table: marimo.ui.table,
) -> Any:
    """
    Show details of the currently selected load
    """
    _result = []

    if dlt_pipeline and dlt_page_loads.value and dlt_loads_table and dlt_loads_table.value:
        _load_id = dlt_loads_table.value[0]["load_id"]  # type: ignore

        try:
            with mo.status.spinner(title="Loading row counts and schema..."):
                _schema = utils.get_schema_by_version(
                    dlt_pipeline, dlt_loads_table.value[0]["schema_version_hash"]  # type: ignore
                )

                # prepare and sort row counts
                _row_counts_dict = utils.get_row_counts(dlt_pipeline, _load_id)
                _row_counts = [
                    {"Table Name": k, "Row Count": v} for k, v in _row_counts_dict.items()
                ]
                _row_counts.sort(key=lambda x: str(x["Table Name"]))

            # add row counts
            _result.append(mo.md(strings.loads_details_row_counts.format(_load_id)))
            _result.append(mo.ui.table(_row_counts, selection=None))

            # add schema info
            if _schema:
                _result.append(
                    mo.md(
                        strings.loads_details_schema_version.format(
                            _schema.name,
                            _schema.version,
                            _load_id,
                            (
                                "is not"
                                if _schema.version_hash != dlt_pipeline.default_schema.version_hash
                                else "is"
                            ),
                        )
                    )
                )
                _result.append(
                    mo.accordion(
                        {
                            "Show raw schema as yaml": mo.ui.code_editor(
                                _schema.to_pretty_yaml(),
                                language="yaml",
                            )
                        }
                    )
                )

        except Exception:
            _result.append(ui.build_error_callout(strings.loads_details_error))
    mo.vstack(_result) if _result else None


@app.cell(hide_code=True)
def section_ibis_backend(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_ibis_browser: marimo.ui.switch,
) -> Any:
    """
    Connects to ibis backend and makes it available in the datasources panel
    """
    _result = [
        ui.build_page_header(
            strings.ibis_backend_title, strings.ibis_backend_subtitle, dlt_page_ibis_browser
        )
    ]

    if dlt_pipeline and dlt_page_ibis_browser.value:
        try:
            with mo.status.spinner(title="Connecting Ibis Backend..."):
                con = dlt_pipeline.dataset().ibis()
            _result.append(
                mo.callout(mo.vstack([mo.md(strings.ibis_backend_connected)]), kind="success")
            )
        except Exception:
            _result.append(ui.build_error_callout(strings.ibis_connect_error))
    mo.vstack(_result) if _result else None


#
# Utility Cells
#


@app.cell(hide_code=True)
def utils_discover_pipelines(cli_arg_pipelines_dir: str, query_var_pipeline_name: str) -> Any:
    """
    Discovers local pipelines and returns a multiselect widget to select one of the pipelines
    """

    # discover pipelines and build selector
    dlt_pipelines_dir: str = ""
    dlt_all_pipelines: List[Dict[str, Any]] = []
    dlt_pipelines_dir, dlt_all_pipelines = utils.get_local_pipelines(cli_arg_pipelines_dir)
    dlt_pipeline_select: mo.ui.multiselect = mo.ui.multiselect(
        options=[p["name"] for p in dlt_all_pipelines],
        value=[query_var_pipeline_name] if query_var_pipeline_name else None,
        max_selections=1,
        label=strings.pipeline_select_label,
        on_change=lambda value: mo.query_params().set("pipeline", str(value[0]) if value else None),
    )

    return (dlt_pipelines_dir, dlt_pipeline_select, dlt_all_pipelines, dlt_pipeline_select)


@app.cell(hide_code=True)
def utils_purge_caches(
    dlt_pipeline: dlt.Pipeline, dlt_cache_query_results: marimo.ui.switch
) -> Any:
    """
    Purge caches of the currently selected pipeline
    """

    if not dlt_cache_query_results.value:
        utils.clear_query_cache(dlt_pipeline)


@app.cell(hide_code=True)
def utils_controls() -> Any:
    """
    Control elements for various parts of the app
    """

    # page switches
    dlt_page_overview = mo.ui.switch(value=True)
    dlt_page_schema = mo.ui.switch(value=False)
    dlt_page_browse_data = mo.ui.switch(value=False)
    dlt_page_state = mo.ui.switch(value=False)
    dlt_page_trace = mo.ui.switch(value=False)
    dlt_page_loads = mo.ui.switch(value=False)
    dlt_page_ibis_browser = mo.ui.switch(value=False)

    # other switches
    dlt_schema_show_dlt_tables = mo.ui.switch(label="<small>Show `_dlt` tables</small>")
    dlt_schema_show_child_tables = mo.ui.switch(
        label="<small>Show child tables</small>", value=True
    )
    dlt_schema_show_row_counts = mo.ui.switch(label="<small>Show row counts</small>", value=False)
    dlt_schema_show_dlt_columns = mo.ui.switch(label="<small>Show `_dlt` columns</small>")
    dlt_schema_show_type_hints = mo.ui.switch(label="<small>Show type hints</small>", value=True)
    dlt_schema_show_other_hints = mo.ui.switch(label="<small>Show other hints</small>", value=False)
    dlt_schema_show_custom_hints = mo.ui.switch(
        label="<small>Show custom hints (x-)</small>", value=False
    )
    dlt_cache_query_results = mo.ui.switch(label="<small>Cache query results</small>", value=True)
    dlt_restrict_to_last_1000 = mo.ui.switch(label="<small>Limit to 1000 rows</small>", value=True)
    dlt_execute_query_on_change = mo.ui.switch(
        label="<small>Execute query automatically on change (loose focus)</small>", value=False
    )
    return


@app.cell(hide_code=True)
def utils_cli_args_and_query_vars() -> Any:
    """
    Prepare cli args  as globals for the following cells
    """
    query_var_pipeline_name: str = cast(str, mo.query_params().get("pipeline")) or None
    cli_arg_pipelines_dir: str = cast(str, mo.cli_args().get("pipelines_dir")) or None
    return (cli_arg_pipelines_dir,)


if __name__ == "__main__":
    app.run()
