# flake8: noqa: F841
# mypy: disable-error-code=no-untyped-def

import marimo

__generated_with = "0.13.9"
app = marimo.App(width="medium", app_title="dlt pipeline dashboard", css_file="dlt_app_styles.css")

with app.setup:
    from typing import Any, Dict, List, Callable, cast

    import marimo as mo

    import pandas as pd
    import sqlglot

    import dlt
    from dlt.common.json import json
    from dlt.helpers.dashboard import strings, utils, ui_elements as ui
    from dlt.helpers.dashboard.config import DashboardConfiguration
    from dlt.destinations.dataset.dataset import ReadableDBAPIDataset, ReadableDBAPIRelation


@app.cell(hide_code=True)
def home(
    dlt_all_pipelines: List[Dict[str, Any]],
    dlt_pipeline_select: mo.ui.multiselect,
    dlt_pipelines_dir: str,
):
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
    dlt_config = utils.resolve_dashboard_config(dlt_pipeline)

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
                            strings.home_quick_start_title.format(
                                ui.build_pipeline_link_list(dlt_config, dlt_all_pipelines)
                            )
                        ),
                        dlt_pipeline_select,
                    ]
                ),
                kind="info",
            ),
            mo.md(strings.home_basics_text.format(len(dlt_all_pipelines), dlt_pipelines_dir)),
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
    return (dlt_pipeline,)


@app.cell(hide_code=True)
def section_sync_status(
    dlt_pipeline: dlt.Pipeline,
    dlt_section_sync_switch: mo.ui.switch,
):
    """
    Returns the status of the pipeline
    """
    _result = ui.build_page_header(
        dlt_pipeline,
        strings.sync_status_title,
        strings.sync_status_subtitle,
        strings.sync_status_subtitle_long,
        dlt_section_sync_switch,
    )

    if dlt_pipeline and dlt_section_sync_switch.value:
        # sync pipeline
        with mo.status.spinner(title=strings.sync_status_spinner_text):
            try:
                dlt_pipeline.sync_destination()
                _credentials = str(utils.get_destination_config(dlt_pipeline).credentials)
                _result.append(
                    mo.callout(
                        mo.vstack([mo.md(strings.sync_status_success_text.format(_credentials))]),
                        kind="success",
                    )
                )
            except Exception:
                _result.append(ui.build_error_callout(strings.sync_status_error_text))
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def section_overview(
    dlt_pipeline: dlt.Pipeline,
    dlt_section_overview_switch: mo.ui.switch,
):
    """
    Overview page of currently selected pipeline
    """

    _result = ui.build_page_header(
        dlt_pipeline,
        strings.overview_title,
        strings.overview_subtitle,
        strings.overview_subtitle,
        dlt_section_overview_switch,
    )

    if dlt_pipeline and dlt_section_overview_switch.value:
        _result += [
            mo.ui.table(
                utils.pipeline_details(dlt_pipeline),
                selection=None,
                style_cell=utils.style_cell,
            ),
        ]
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def section_schema(
    dlt_config: DashboardConfiguration,
    dlt_pipeline: dlt.Pipeline,
    dlt_schema_show_child_tables: mo.ui.switch,
    dlt_schema_show_custom_hints: mo.ui.switch,
    dlt_schema_show_dlt_columns: mo.ui.switch,
    dlt_schema_show_dlt_tables: mo.ui.switch,
    dlt_schema_show_other_hints: mo.ui.switch,
    dlt_schema_show_type_hints: mo.ui.switch,
    dlt_schema_table_list: mo.ui.table,
    dlt_section_schema_switch: mo.ui.switch,
):
    """
    Show schema of the currently selected pipeline
    """

    _result = ui.build_page_header(
        dlt_pipeline,
        strings.schema_title,
        strings.schema_subtitle,
        strings.schema_subtitle_long,
        dlt_section_schema_switch,
    )

    if dlt_pipeline and dlt_section_schema_switch.value and dlt_schema_table_list is None:
        _result.append(
            mo.callout(
                mo.md(strings.schema_no_default_available_text),
                kind="warn",
            )
        )
    elif dlt_pipeline and dlt_section_schema_switch.value:
        # build table overview
        _result.append(
            mo.hstack([dlt_schema_show_dlt_tables, dlt_schema_show_child_tables], justify="start")
        )
        _result.append(dlt_schema_table_list)

        # add table details
        _result.append(ui.build_title_and_subtitle(strings.schema_table_details_title))
        _result.append(
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

        for table in dlt_schema_table_list.value:  # type: ignore[union-attr,unused-ignore]
            _table_name = table["name"]  # type: ignore[index,unused-ignore]
            _result.append(mo.md(strings.schema_table_columns_title.format(_table_name)))
            columns_list = utils.create_column_list(
                dlt_config,
                dlt_pipeline,
                _table_name,
                show_internals=dlt_schema_show_dlt_columns.value,
                show_type_hints=dlt_schema_show_type_hints.value,
                show_other_hints=dlt_schema_show_other_hints.value,
                show_custom_hints=dlt_schema_show_custom_hints.value,
            )
            _result.append(
                mo.ui.table(
                    columns_list,
                    selection=None,
                    style_cell=utils.style_cell,
                    freeze_columns_left=["name"] if len(columns_list) > 0 else None,
                )
            )

        # add raw schema
        _result.append(ui.build_title_and_subtitle(strings.schema_raw_yaml_title))
        _result.append(
            mo.accordion(
                {
                    strings.schema_show_raw_yaml_text: mo.ui.code_editor(
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
    dlt_clear_query_cache: mo.ui.run_button,
    dlt_data_table_list: mo.ui.table,
    dlt_pipeline: dlt.Pipeline,
    dlt_restrict_to_last_1000: mo.ui.switch,
    dlt_schema_show_child_tables: mo.ui.switch,
    dlt_schema_show_dlt_tables: mo.ui.switch,
    dlt_schema_show_row_counts: mo.ui.run_button,
    dlt_section_browse_data_switch: mo.ui.switch,
):
    """
    Show data of the currently selected pipeline
    """

    _result = ui.build_page_header(
        dlt_pipeline,
        strings.browse_data_title,
        strings.browse_data_subtitle,
        strings.browse_data_subtitle_long,
        dlt_section_browse_data_switch,
    )

    dlt_query_editor: mo.ui.code_editor = None
    if dlt_pipeline and dlt_section_browse_data_switch.value and dlt_data_table_list is not None:
        try:
            # try to connect to the dataset
            utils.get_destination_config(dlt_pipeline)
            _result.append(
                mo.hstack(
                    [
                        dlt_schema_show_dlt_tables,
                        dlt_schema_show_child_tables,
                    ],
                    justify="start",
                ),
            )
            _result.append(dlt_data_table_list)
            _result.append(dlt_schema_show_row_counts)

            _sql_query = ""
            if dlt_data_table_list.value:
                _table_name = dlt_data_table_list.value[0]["name"]  # type: ignore[index,unused-ignore]
                _dataset = cast(ReadableDBAPIDataset, dlt_pipeline.dataset())
                _sql_query = (
                    cast(ReadableDBAPIRelation, _dataset.table(_table_name))
                    .limit(1000 if dlt_restrict_to_last_1000.value else None)
                    .to_sql(pretty=True)
                )

            dlt_query_editor = mo.ui.code_editor(
                language="sql",
                placeholder=strings.browse_data_query_hint,
                value=_sql_query,
                debounce=True,
            )
            dlt_run_query_button: mo.ui.run_button = mo.ui.run_button(
                label=strings.browse_data_run_query_button,
                tooltip=strings.browse_data_run_query_tooltip,
            )

            _result += [
                mo.md(strings.browse_data_explorer_title),
                mo.hstack(
                    [dlt_restrict_to_last_1000],
                    justify="start",
                ),
                dlt_query_editor,
            ]

            _result.append(
                mo.hstack([dlt_run_query_button, dlt_clear_query_cache], justify="start")
            )
        except Exception:
            _result.append(ui.build_error_callout(strings.browse_data_error_text))
    elif dlt_pipeline and dlt_section_browse_data_switch.value:
        _result.append(ui.build_error_callout(strings.browse_data_error_text))
    mo.vstack(_result) if _result else None
    return dlt_query_editor, dlt_run_query_button


@app.cell(hide_code=True)
def section_browse_data_query_result(
    dlt_data_table_list: mo.ui.table,
    dlt_pipeline: dlt.Pipeline,
    dlt_query_editor: mo.ui.code_editor,
    dlt_run_query_button: mo.ui.run_button,
    dlt_section_browse_data_switch: mo.ui.switch,
    dlt_clear_query_cache: mo.ui.run_button,
    dlt_get_last_query_result,
    dlt_set_last_query_result,
    dlt_set_query_cache,
    dlt_get_query_cache,
):
    """
    Execute the query in the editor
    """

    _result = []

    dlt_query_history_table: mo.ui.table = None
    dlt_query_error_encountered: bool = False
    dlt_query: str = None

    if (
        dlt_pipeline
        and dlt_section_browse_data_switch.value
        and dlt_data_table_list is not None
        and dlt_query_editor is not None
    ):
        _result.append(ui.build_title_and_subtitle(strings.browse_data_query_result_title))
        with mo.status.spinner(title=strings.browse_data_loading_spinner_text):
            if dlt_query_editor.value and (dlt_run_query_button.value):
                try:
                    sqlglot.parse_one(
                        dlt_query_editor.value,
                        dialect=dlt_pipeline.destination.capabilities().sqlglot_dialect,
                    )
                    if dlt_clear_query_cache.value:
                        utils.clear_query_cache(dlt_pipeline)
                    dlt_query = dlt_query_editor.value
                    dlt_set_last_query_result(utils.get_query_result(dlt_pipeline, dlt_query))
                except Exception as e:
                    dlt_query_error_encountered = True
                    _result.append(
                        ui.build_error_callout(strings.browse_data_query_error, code=str(e))
                    )

        # add result
        _last_result = dlt_get_last_query_result()
        if _last_result is not None and not dlt_query_error_encountered:
            _result += [
                mo.ui.table(_last_result, selection=None),
            ]
            # update query cache
            cache = dlt_get_query_cache()
            if dlt_query:
                # insert into dict with re-ordering most recent first:
                cache.pop(dlt_query, None)
                cache = {dlt_query: _last_result.shape[0], **cache}
            dlt_set_query_cache(cache)

    # provide query history table
    _query_history = dlt_get_query_cache()
    if _query_history:
        dlt_query_history_table = mo.ui.table(
            [{"query": q, "row_count": _query_history[q]} for q in _query_history]
        )
    mo.vstack(_result) if _result else None
    return dlt_query_history_table


@app.cell(hide_code=True)
def section_browse_data_query_history(
    dlt_pipeline: dlt.Pipeline,
    dlt_query_history_table: mo.ui.table,
    dlt_section_browse_data_switch: mo.ui.switch,
):
    """
    Show the query history
    """

    _result: List[Any] = []
    if (
        dlt_pipeline
        and dlt_section_browse_data_switch.value
        and dlt_query_history_table is not None
    ):
        _result.append(
            ui.build_title_and_subtitle(
                strings.browse_data_query_history_title, strings.browse_data_query_history_subtitle
            )
        )
        _result.append(dlt_query_history_table)

        for _r in dlt_query_history_table.value:  # type: ignore[unused-ignore,union-attr]
            _query = _r["query"]  # type: ignore[unused-ignore,index]
            _q_result = utils.get_query_result(dlt_pipeline, _query)
            _result.append(mo.md(f"<small>```{_query}```</small>"))
            _result.append(mo.ui.table(_q_result, selection=None))
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def section_state(
    dlt_pipeline: dlt.Pipeline,
    dlt_section_state_switch: mo.ui.switch,
):
    """
    Show state of the currently selected pipeline
    """
    _result = ui.build_page_header(
        dlt_pipeline,
        strings.state_title,
        strings.state_subtitle,
        strings.state_subtitle,
        dlt_section_state_switch,
    )

    if dlt_pipeline and dlt_section_state_switch.value:
        _result.append(
            mo.ui.code_editor(
                json.dumps(dlt_pipeline.state, pretty=True),
                language="json",
            ),
        )
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def section_trace(
    dlt_pipeline: dlt.Pipeline,
    dlt_section_trace_switch: mo.ui.switch,
    dlt_trace_steps_table: mo.ui.table,
    dlt_config: DashboardConfiguration,
):
    """
    Show last trace of the currently selected pipeline
    """

    _result = ui.build_page_header(
        dlt_pipeline,
        strings.trace_title,
        strings.trace_subtitle,
        strings.trace_subtitle,
        dlt_section_trace_switch,
    )

    if dlt_pipeline and dlt_section_trace_switch.value:
        dlt_trace = dlt_pipeline.last_trace
        if not dlt_trace:
            _result.append(
                mo.callout(
                    mo.md(strings.trace_no_trace_text),
                    kind="warn",
                )
            )
        else:
            trace_dict = dlt_trace.asdict()
            _result.append(
                ui.build_title_and_subtitle(
                    strings.trace_overview_title,
                    title_level=3,
                )
            )
            _result.append(
                mo.ui.table(utils.trace_overview(dlt_config, trace_dict), selection=None)
            )
            _result.append(
                ui.build_title_and_subtitle(
                    strings.trace_execution_context_title,
                    strings.trace_execution_context_subtitle,
                    title_level=3,
                )
            )
            _result.append(
                mo.ui.table(utils.trace_execution_context(dlt_config, trace_dict), selection=None)
            )
            _result.append(
                ui.build_title_and_subtitle(
                    strings.trace_steps_overview_title,
                    strings.trace_steps_overview_subtitle,
                    title_level=3,
                )
            )
            _result.append(dlt_trace_steps_table)
            for item in dlt_trace_steps_table.value:  # type: ignore[unused-ignore,union-attr]
                step_id = item["step"]  # type: ignore[unused-ignore,index]
                _result.append(
                    ui.build_title_and_subtitle(
                        strings.trace_step_details_title.format(step_id.capitalize()),
                        title_level=3,
                    )
                )
                _result += utils.trace_step_details(dlt_config, trace_dict, step_id)

            # config values
            _result.append(
                ui.build_title_and_subtitle(
                    strings.trace_resolved_config_title,
                    strings.trace_resolved_config_subtitle,
                    title_level=3,
                )
            )
            _result.append(
                mo.ui.table(
                    utils.trace_resolved_config_values(dlt_config, trace_dict), selection=None
                )
            )
            _result.append(
                ui.build_title_and_subtitle(
                    strings.trace_raw_trace_title,
                    title_level=3,
                )
            )
            _result.append(
                mo.accordion(
                    {
                        strings.trace_show_raw_trace_text: mo.ui.code_editor(
                            json.dumps(dlt_trace, pretty=True),
                            language="json",
                        )
                    }
                )
            )
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def section_loads(
    dlt_config: DashboardConfiguration,
    dlt_clear_query_cache: mo.ui.run_button,
    dlt_pipeline: dlt.Pipeline,
    dlt_restrict_to_last_1000: mo.ui.switch,
    dlt_section_loads_switch: mo.ui.switch,
):
    """
    Show loads of the currently selected pipeline
    """

    _result = ui.build_page_header(
        dlt_pipeline,
        strings.loads_title,
        strings.loads_subtitle,
        strings.loads_subtitle_long,
        dlt_section_loads_switch,
    )

    if dlt_pipeline and dlt_section_loads_switch.value:
        _result.append(mo.hstack([dlt_restrict_to_last_1000], justify="start"))

        with mo.status.spinner(title=strings.loads_loading_spinner_text):
            dlt_loads_table: mo.ui.table = None
            try:
                _loads_data = utils.get_loads(
                    dlt_config,
                    dlt_pipeline,
                    limit=1000 if dlt_restrict_to_last_1000.value else None,
                )
                dlt_loads_table = mo.ui.table(_loads_data, selection="single")
                _result.append(dlt_loads_table)
                _result.append(dlt_clear_query_cache)
            except Exception:
                _result.append(ui.build_error_callout(strings.loads_loading_failed_text))
    mo.vstack(_result) if _result else None
    return (dlt_loads_table,)


@app.cell(hide_code=True)
def section_loads_results(
    dlt_loads_table: mo.ui.table,
    dlt_pipeline: dlt.Pipeline,
    dlt_section_loads_switch: mo.ui.switch,
):
    """
    Show details of the currently selected load
    """
    _result = []

    if (
        dlt_pipeline
        and dlt_section_loads_switch.value
        and dlt_loads_table is not None
        and dlt_loads_table.value
    ):
        _load_id = dlt_loads_table.value[0]["load_id"]  # type: ignore[unused-ignore,index]
        _result.append(mo.md(strings.loads_details_title.format(_load_id)))

        try:
            with mo.status.spinner(title=strings.loads_details_loading_spinner_text):
                _schema = utils.get_schema_by_version(
                    dlt_pipeline, dlt_loads_table.value[0]["schema_version_hash"]  # type: ignore[unused-ignore,index]
                )

                # prepare and sort row counts
                _row_counts_dict = utils.get_row_counts(dlt_pipeline, _load_id)
                _row_counts = [{"name": k, "row_count": v} for k, v in _row_counts_dict.items()]
                _row_counts.sort(key=lambda x: str(x["name"]))

            # add row counts
            _result.append(
                ui.build_title_and_subtitle(
                    strings.loads_details_row_counts_title,
                    strings.loads_details_row_counts_subtitle,
                    3,
                ),
            )
            _result.append(mo.ui.table(_row_counts, selection=None))

            # add schema info
            if _schema:
                _result.append(
                    ui.build_title_and_subtitle(
                        strings.loads_details_schema_version_title,
                        strings.loads_details_schema_version_subtitle.format(
                            (
                                "is not"
                                if _schema.version_hash != dlt_pipeline.default_schema.version_hash
                                else "is"
                            ),
                        ),
                        3,
                    )
                )
                _result.append(
                    mo.accordion(
                        {
                            strings.schema_show_raw_yaml_text: mo.ui.code_editor(
                                _schema.to_pretty_yaml(),
                                language="yaml",
                            )
                        }
                    )
                )

        except Exception:
            _result.append(ui.build_error_callout(strings.loads_details_error_text))
    mo.vstack(_result) if len(_result) else None
    return


@app.cell(hide_code=True)
def section_ibis_backend(
    dlt_pipeline: dlt.Pipeline,
    dlt_section_ibis_browser_switch: mo.ui.switch,
):
    """
    Connects to ibis backend and makes it available in the datasources panel
    """
    _result = ui.build_page_header(
        dlt_pipeline,
        strings.ibis_backend_title,
        strings.ibis_backend_subtitle,
        strings.ibis_backend_subtitle,
        dlt_section_ibis_browser_switch,
    )

    if dlt_pipeline and dlt_section_ibis_browser_switch.value:
        try:
            with mo.status.spinner(title=strings.ibis_backend_connecting_spinner_text):
                con = dlt_pipeline.dataset().ibis()
            _result.append(
                mo.callout(mo.vstack([mo.md(strings.ibis_backend_connected_text)]), kind="success")
            )
        except Exception:
            _result.append(ui.build_error_callout(strings.ibis_backend_error_text))
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def utils_discover_pipelines(
    mo_cli_arg_pipelines_dir: str,
    mo_cli_arg_pipeline: str,
    mo_query_var_pipeline_name: str,
):
    """
    Discovers local pipelines and returns a multiselect widget to select one of the pipelines
    """

    # discover pipelines and build selector
    dlt_pipelines_dir: str = ""
    dlt_all_pipelines: List[Dict[str, Any]] = []
    dlt_pipelines_dir, dlt_all_pipelines = utils.get_local_pipelines(mo_cli_arg_pipelines_dir)
    dlt_pipeline_select: mo.ui.multiselect = mo.ui.multiselect(
        options=[p["name"] for p in dlt_all_pipelines],
        value=(
            [mo_query_var_pipeline_name]
            if mo_query_var_pipeline_name
            else ([mo_cli_arg_pipeline] if mo_cli_arg_pipeline else None)
        ),
        max_selections=1,
        label=strings.app_pipeline_select_label,
        on_change=lambda value: mo.query_params().set("pipeline", str(value[0]) if value else None),
    )

    return dlt_all_pipelines, dlt_pipeline_select, dlt_pipelines_dir


@app.cell(hide_code=True)
def utils_caches_and_state(
    dlt_clear_query_cache: mo.ui.run_button,
    dlt_pipeline: dlt.Pipeline,
):
    """
    Purge caches of the currently selected pipeline
    """

    # some state variables
    dlt_get_last_query_result, dlt_set_last_query_result = mo.state(pd.DataFrame())
    # a cache of query results in the form of {query: row_count}
    dlt_get_query_cache, dlt_set_query_cache = mo.state(cast(Dict[str, int], {}))

    if dlt_clear_query_cache.value:
        utils.clear_query_cache(dlt_pipeline)

    return


@app.cell(hide_code=True)
def ui_controls(mo_cli_arg_with_test_identifiers: bool):
    """
    Control elements for various parts of the app
    """

    # page switches
    dlt_section_sync_switch: mo.ui.switch = mo.ui.switch(
        value=True, label="sync" if mo_cli_arg_with_test_identifiers else ""
    )
    dlt_section_overview_switch: mo.ui.switch = mo.ui.switch(
        value=True, label="overview" if mo_cli_arg_with_test_identifiers else ""
    )
    dlt_section_schema_switch: mo.ui.switch = mo.ui.switch(
        value=False, label="schema" if mo_cli_arg_with_test_identifiers else ""
    )
    dlt_section_browse_data_switch: mo.ui.switch = mo.ui.switch(
        value=False, label="data" if mo_cli_arg_with_test_identifiers else ""
    )
    dlt_section_state_switch: mo.ui.switch = mo.ui.switch(
        value=False, label="state" if mo_cli_arg_with_test_identifiers else ""
    )
    dlt_section_trace_switch: mo.ui.switch = mo.ui.switch(
        value=False, label="trace" if mo_cli_arg_with_test_identifiers else ""
    )
    dlt_section_loads_switch: mo.ui.switch = mo.ui.switch(
        value=False, label="loads" if mo_cli_arg_with_test_identifiers else ""
    )
    dlt_section_ibis_browser_switch: mo.ui.switch = mo.ui.switch(
        value=False, label="ibis" if mo_cli_arg_with_test_identifiers else ""
    )

    # other switches
    dlt_schema_show_dlt_tables: mo.ui.switch = mo.ui.switch(
        label=f"<small>{strings.ui_show_dlt_tables}</small>"
    )
    dlt_schema_show_child_tables: mo.ui.switch = mo.ui.switch(
        label=f"<small>{strings.ui_show_child_tables}</small>", value=False
    )
    dlt_schema_show_row_counts: mo.ui.run_button = mo.ui.run_button(
        label=f"<small>{strings.ui_load_row_counts}</small>"
    )
    dlt_schema_show_dlt_columns: mo.ui.switch = mo.ui.switch(
        label=f"<small>{strings.ui_show_dlt_columns}</small>"
    )
    dlt_schema_show_type_hints: mo.ui.switch = mo.ui.switch(
        label=f"<small>{strings.ui_show_type_hints}</small>", value=True
    )
    dlt_schema_show_other_hints: mo.ui.switch = mo.ui.switch(
        label=f"<small>{strings.ui_show_other_hints}</small>", value=False
    )
    dlt_schema_show_custom_hints: mo.ui.switch = mo.ui.switch(
        label=f"<small>{strings.ui_show_custom_hints}</small>", value=False
    )
    dlt_clear_query_cache: mo.ui.run_button = mo.ui.run_button(
        label=f"<small>{strings.ui_clear_cache}</small>"
    )
    dlt_restrict_to_last_1000: mo.ui.switch = mo.ui.switch(
        label=f"<small>{strings.ui_limit_to_1000_rows}</small>", value=True
    )
    return (
        dlt_clear_query_cache,
        dlt_restrict_to_last_1000,
        dlt_schema_show_child_tables,
        dlt_schema_show_custom_hints,
        dlt_schema_show_dlt_columns,
        dlt_schema_show_dlt_tables,
        dlt_schema_show_other_hints,
        dlt_schema_show_row_counts,
        dlt_schema_show_type_hints,
        dlt_section_browse_data_switch,
        dlt_section_ibis_browser_switch,
        dlt_section_loads_switch,
        dlt_section_overview_switch,
        dlt_section_schema_switch,
        dlt_section_state_switch,
        dlt_section_sync_switch,
        dlt_section_trace_switch,
    )


@app.cell(hide_code=True)
def ui_primary_controls(
    dlt_pipeline: dlt.Pipeline,
    dlt_schema_show_child_tables: mo.ui.switch,
    dlt_schema_show_dlt_tables: mo.ui.switch,
    dlt_schema_show_row_counts: mo.ui.switch,
    dlt_section_browse_data_switch: mo.ui.switch,
    dlt_section_schema_switch: mo.ui.switch,
    dlt_section_trace_switch: mo.ui.switch,
    dlt_config: DashboardConfiguration,
):
    """
    Helper cell for creating certain controls based on selected sections
    """

    #
    # Schema controls
    #
    dlt_schema_table_list: mo.ui.table = None
    if dlt_section_schema_switch.value and dlt_pipeline and dlt_pipeline.default_schema_name:
        _table_list = utils.create_table_list(
            dlt_config,
            dlt_pipeline,
            show_internals=dlt_schema_show_dlt_tables.value,
            show_child_tables=dlt_schema_show_child_tables.value,
        )
        dlt_schema_table_list = mo.ui.table(
            _table_list,  # type: ignore[arg-type,unused-ignore]
            style_cell=utils.style_cell,
            initial_selection=[0] if len(_table_list) > 0 else None,
            freeze_columns_left=["name"] if len(_table_list) > 0 else None,
        )

    #
    # Browse data controls
    #
    dlt_data_table_list: mo.ui.table = None
    if dlt_section_browse_data_switch.value and dlt_pipeline and dlt_pipeline.default_schema_name:
        table_list = utils.create_table_list(
            dlt_config,
            dlt_pipeline,
            show_internals=dlt_schema_show_dlt_tables.value,
            show_child_tables=dlt_schema_show_child_tables.value,
            show_row_counts=dlt_schema_show_row_counts.value,
        )
        dlt_data_table_list = mo.ui.table(
            table_list,  # type: ignore[arg-type,unused-ignore]
            style_cell=utils.style_cell,
            selection="single",
            freeze_columns_left=["name"] if len(table_list) > 0 else None,
        )

    #
    # Trace steps table
    #
    dlt_trace_steps_table: mo.ui.table = None
    if dlt_section_trace_switch.value and dlt_pipeline and dlt_pipeline.last_trace:
        dlt_trace_steps_table = mo.ui.table(
            utils.trace_steps_overview(dlt_config, dlt_pipeline.last_trace.asdict())
        )

    return dlt_data_table_list, dlt_schema_table_list, dlt_trace_steps_table


@app.cell(hide_code=True)
def utils_cli_args_and_query_vars_config():
    """
    Prepare cli args  as globals for the following cells
    """

    try:
        mo_query_var_pipeline_name: str = cast(str, mo.query_params().get("pipeline")) or None
        mo_cli_arg_pipeline: str = cast(str, mo.cli_args().get("pipeline")) or None
        mo_cli_arg_pipelines_dir: str = cast(str, mo.cli_args().get("pipelines_dir")) or None
        mo_cli_arg_with_test_identifiers: bool = (
            cast(bool, mo.cli_args().get("with_test_identifiers")) or False
        )
    except Exception:
        mo_query_var_pipeline_name = None
        mo_cli_arg_pipelines_dir = None
        mo_cli_arg_with_test_identifiers = False
        mo_cli_arg_pipeline = None

    return (
        mo_cli_arg_pipelines_dir,
        mo_cli_arg_with_test_identifiers,
        mo_query_var_pipeline_name,
        mo_cli_arg_pipeline,
    )


if __name__ == "__main__":
    app.run()
