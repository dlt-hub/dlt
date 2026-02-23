# /// script
# [tool.marimo.display]
# theme = "light"
# ///
# flake8: noqa: F841
# mypy: disable-error-code=no-untyped-def

import marimo

__generated_with = "0.13.9"
app = marimo.App(
    width="medium", app_title="dlt workspace dashboard", css_file="dlt_dashboard_styles.css"
)

with app.setup:
    from typing import Any, Dict, List, cast

    from dlt._workspace.helpers.dashboard.typing import (
        TLoadItem,
        TPipelineListItem,
        TQueryHistoryItem,
        TTableListItem,
    )

    import marimo as mo

    import dlt
    import pyarrow
    from dlt.common import logger
    from dlt._workspace.helpers.dashboard import strings
    from dlt._workspace.helpers.dashboard import utils

    from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
    from dlt.common.configuration.specs.pluggable_run_context import ProfilesRunContext
    from dlt._workspace.run_context import switch_profile


@app.cell(hide_code=True)
def home(
    dlt_profile_select: mo.ui.dropdown,
    dlt_all_pipelines: List[TPipelineListItem],
    dlt_pipeline_select: mo.ui.multiselect,
    dlt_pipelines_dir: str,
    dlt_refresh_button: mo.ui.run_button,
    dlt_pipeline_name: str,
    dlt_file_watcher: Any,  # marimo internal watcher type
):
    """Displays the welcome page or pipeline dashboard depending on selection."""

    # NOTE: keep these two lines for refreshing
    dlt_refresh_button
    dlt_file_watcher

    # returned by cell
    dlt_pipeline: dlt.Pipeline = None
    dlt_config: DashboardConfiguration = None
    _result: List[mo.Html] = []

    if dlt_pipeline_name:
        try:
            dlt_pipeline = utils.pipeline.get_pipeline(dlt_pipeline_name, dlt_pipelines_dir)
            dlt_config = utils.pipeline.resolve_dashboard_config(dlt_pipeline)
        except Exception:
            _result = utils.home.render_pipeline_header_row(
                dlt_pipeline_name, dlt_profile_select, dlt_pipeline_select, [dlt_refresh_button]
            )
            _result.append(
                utils.ui.error_callout(
                    strings.home_error_attach_pipeline.format(dlt_pipeline_name),
                )
            )

        if dlt_pipeline:
            try:
                _result = utils.home.render_pipeline_home(
                    dlt_profile_select,
                    dlt_pipeline,
                    dlt_pipeline_select,
                    dlt_pipelines_dir,
                    dlt_refresh_button,
                    dlt_pipeline_name,
                )
            except Exception:
                _result = [
                    utils.ui.error_callout(
                        strings.home_error_rendering_pipeline,
                        strings.home_error_rendering_pipeline_detail,
                    )
                ]
    else:
        try:
            dlt_config = utils.pipeline.resolve_dashboard_config(None)
            _result = utils.home.render_workspace_home(
                dlt_profile_select,
                dlt_all_pipelines,
                dlt_pipeline_select,
                dlt_pipelines_dir,
                dlt_config,
            )
        except Exception:
            _result = [
                utils.ui.error_callout(
                    strings.home_error_rendering_home,
                )
            ]
    mo.vstack(_result) if _result else None
    return (dlt_pipeline, dlt_config)


@app.cell(hide_code=True)
def section_info(
    dlt_pipeline: dlt.Pipeline,
    dlt_section_info_switch: mo.ui.switch,
    dlt_all_pipelines: List[TPipelineListItem],
    dlt_config: DashboardConfiguration,
    dlt_pipelines_dir: str,
):
    """
    Overview page of currently selected pipeline
    """

    _result, _show = utils.ui.section(strings.overview, dlt_pipeline, dlt_section_info_switch)

    if _show:
        _result += [
            utils.ui.dlt_table(
                utils.pipeline.pipeline_details(dlt_config, dlt_pipeline, dlt_pipelines_dir)
            ),
        ]
        _result.append(
            utils.ui.title_and_subtitle(
                strings.overview_remote_state_title, strings.overview_remote_state_subtitle
            )
        )
        _result.append(
            mo.accordion(
                {
                    strings.overview_remote_state_button: utils.ui.dlt_table(
                        utils.pipeline.remote_state_details(dlt_pipeline),
                    )
                },
                lazy=True,
            )
        )
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
    dlt_schema_select: mo.ui.multiselect,
    dlt_selected_schema_name: str,
):
    """
    Show schema of the currently selected pipeline
    """

    _result, _show = utils.ui.section(strings.schema, dlt_pipeline, dlt_section_schema_switch)

    if _show and dlt_schema_table_list is None:
        _result.append(
            mo.callout(
                mo.md(strings.schema_no_default_available_text),
                kind="warn",
            )
        )
    elif _show:
        # build table overview
        _result.append(
            mo.hstack(
                [dlt_schema_select, dlt_schema_show_dlt_tables, dlt_schema_show_child_tables],
                justify="start",
            )
        )
        _result.append(dlt_schema_table_list)

        # add table details
        _result.append(utils.ui.title_and_subtitle(strings.schema_table_details_title))
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

        for _table in cast(List[TTableListItem], dlt_schema_table_list.value):
            _table_name = _table["name"]
            _result.append(mo.md(strings.schema_table_columns_title.format(_table_name)))
            _columns_list = utils.schema.create_column_list(
                dlt_config,
                dlt_pipeline,
                _table_name,
                dlt_selected_schema_name,
                show_internals=dlt_schema_show_dlt_columns.value,
                show_type_hints=dlt_schema_show_type_hints.value,
                show_other_hints=dlt_schema_show_other_hints.value,
                show_custom_hints=dlt_schema_show_custom_hints.value,
            )
            _result.append(utils.ui.dlt_table(_columns_list))

        # add raw schema
        _result.append(utils.ui.title_and_subtitle(strings.schema_raw_yaml_title))
        _result.append(
            mo.accordion(
                {
                    strings.schema_show_raw_yaml_text: mo.ui.code_editor(
                        dlt_pipeline.schemas[dlt_selected_schema_name].to_pretty_yaml(),
                        language="yaml",
                    )
                }
            )
        )
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def ui_data_quality_controls(
    dlt_pipeline: dlt.Pipeline,
    dlt_section_data_quality_switch: mo.ui.switch,
):
    """Create data quality filter controls (separate cell for marimo reactivity)."""
    dlt_data_quality_show_failed_filter: mo.ui.checkbox = None
    dlt_data_quality_table_filter: mo.ui.dropdown = None
    dlt_data_quality_rate_filter: mo.ui.slider = None
    dlt_data_quality_checks_arrow = None

    if utils.home.detect_dlt_hub() and dlt_pipeline:
        (
            dlt_data_quality_show_failed_filter,
            dlt_data_quality_table_filter,
            dlt_data_quality_rate_filter,
            dlt_data_quality_checks_arrow,
        ) = utils.data_quality.create_dq_controls(dlt_pipeline)

    return (
        dlt_data_quality_show_failed_filter,
        dlt_data_quality_table_filter,
        dlt_data_quality_rate_filter,
        dlt_data_quality_checks_arrow,
    )


@app.cell(hide_code=True)
def section_data_quality(
    dlt_pipeline: dlt.Pipeline,
    dlt_section_data_quality_switch: mo.ui.switch,
    dlt_data_quality_show_failed_filter: mo.ui.checkbox,
    dlt_data_quality_table_filter: mo.ui.dropdown,
    dlt_data_quality_rate_filter: mo.ui.slider,
    dlt_data_quality_checks_arrow,
):
    """Show data quality of the currently selected pipeline (only if dlt.hub is installed)."""
    dlt_data_quality_show_raw_table_switch = None
    if not utils.home.detect_dlt_hub():
        _result: List[mo.Html] = []
    else:
        _result, _show = utils.ui.section(
            strings.data_quality, dlt_pipeline, dlt_section_data_quality_switch
        )
        if _show:
            _dq_widgets, dlt_data_quality_show_raw_table_switch = (
                utils.data_quality.build_dq_section(
                    dlt_pipeline,
                    dlt_data_quality_show_failed_filter,
                    dlt_data_quality_table_filter,
                    dlt_data_quality_rate_filter,
                    dlt_data_quality_checks_arrow,
                )
            )
            _result.extend(_dq_widgets)
        else:
            dlt_data_quality_show_raw_table_switch = None
    mo.vstack(_result) if _result else None
    return dlt_data_quality_show_raw_table_switch


@app.cell(hide_code=True)
def section_data_quality_raw_table(
    dlt_pipeline: dlt.Pipeline,
    dlt_section_data_quality_switch: mo.ui.switch,
    dlt_data_quality_show_raw_table_switch: mo.ui.switch,
    dlt_get_last_query_result,
    dlt_set_last_query_result,
):
    """Display the raw data quality checks table with _dlt_load_id column."""
    _result = []

    if (
        dlt_pipeline
        and dlt_section_data_quality_switch.value
        and dlt_data_quality_show_raw_table_switch is not None
        and dlt_data_quality_show_raw_table_switch.value
    ):
        _result = utils.data_quality.build_dq_raw_table(
            dlt_pipeline, dlt_get_last_query_result, dlt_set_last_query_result
        )
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def section_browse_data_table_list(
    dlt_clear_result_cache: mo.ui.run_button,
    dlt_data_table_list: mo.ui.table,
    dlt_pipeline: dlt.Pipeline,
    dlt_restrict_to_last_1000: mo.ui.switch,
    dlt_schema_show_child_tables: mo.ui.switch,
    dlt_schema_show_dlt_tables: mo.ui.switch,
    dlt_schema_show_row_counts: mo.ui.run_button,
    dlt_section_browse_data_switch: mo.ui.switch,
    dlt_schema_select: mo.ui.multiselect,
    dlt_selected_schema_name: str,
):
    """
    Show data of the currently selected pipeline
    """

    _result, _show = utils.ui.section(
        strings.browse_data, dlt_pipeline, dlt_section_browse_data_switch
    )

    dlt_query_editor: mo.ui.code_editor = None
    dlt_run_query_button: mo.ui.run_button = None
    if _show and dlt_data_table_list is not None:
        _result.append(
            mo.hstack(
                [
                    dlt_schema_select,
                    dlt_schema_show_row_counts,
                    dlt_schema_show_dlt_tables,
                    dlt_schema_show_child_tables,
                ],
                justify="start",
            ),
        )
        _result.append(dlt_data_table_list)

        _sql_query = ""
        if dlt_data_table_list.value:
            _selected_table = cast(List[TTableListItem], dlt_data_table_list.value)[0]
            _table_name = _selected_table["name"]

            if _state_widget := utils.schema.build_resource_state_widget(
                dlt_pipeline, dlt_selected_schema_name, _table_name
            ):
                _result.append(_state_widget)

            _sql_query, _error_message, _traceback_string = (
                utils.queries.get_default_query_for_table(
                    dlt_pipeline,
                    dlt_selected_schema_name,
                    _table_name,
                    dlt_restrict_to_last_1000.value,
                )
            )
        _placeholder, _error_message, _traceback_string = (
            utils.queries.get_example_query_for_dataset(
                dlt_pipeline,
                dlt_selected_schema_name,
            )
        )

        if _error_message:
            _result.append(
                utils.ui.error_callout(
                    strings.browse_data_error_text + _error_message,
                    traceback_string=_traceback_string,
                )
            )
        else:
            dlt_query_editor = mo.ui.code_editor(
                language="sql",
                placeholder=_placeholder,
                value=_sql_query,
                debounce=True,
            )

            dlt_run_query_button = mo.ui.run_button(
                label=utils.ui.small(strings.browse_data_run_query_button),
                tooltip=strings.browse_data_run_query_tooltip,
            )

            _result += [
                mo.md(utils.ui.small(strings.browse_data_explorer_title)),
                mo.hstack(
                    [dlt_restrict_to_last_1000],
                    justify="start",
                ),
                dlt_query_editor,
            ]

            _result.append(
                mo.hstack([dlt_run_query_button, dlt_clear_result_cache], justify="start")
            )
    elif _show:
        # here we also use the no schemas text, as it is appropriate for the case where we have no table information.
        _result.append(utils.ui.error_callout(strings.schema_no_default_available_text))
    mo.vstack(_result) if _result else None
    return dlt_query_editor, dlt_run_query_button


@app.cell(hide_code=True)
def section_browse_data_query_result(
    dlt_data_table_list: mo.ui.table,
    dlt_pipeline: dlt.Pipeline,
    dlt_query_editor: mo.ui.code_editor,
    dlt_run_query_button: mo.ui.run_button,
    dlt_section_browse_data_switch: mo.ui.switch,
    dlt_clear_result_cache: mo.ui.run_button,
    dlt_get_last_query_result,
    dlt_set_last_query_result,
    dlt_set_query_history,
    dlt_get_query_history,
):
    """
    Execute the query in the editor
    """

    _result = []

    dlt_query_history_table: mo.ui.table = None
    dlt_query: str = None

    if (
        dlt_pipeline
        and dlt_section_browse_data_switch.value
        and dlt_data_table_list is not None
        and dlt_query_editor is not None
    ):
        _result.append(utils.ui.title_and_subtitle(strings.browse_data_query_result_title))
        _error_message: str = None
        with mo.status.spinner(title=strings.browse_data_loading_spinner_text):
            if dlt_query_editor.value and (dlt_run_query_button.value):
                if dlt_clear_result_cache.value:
                    utils.queries.clear_query_cache()
                dlt_query = dlt_query_editor.value
                _query_result, _error_message, _traceback_string = utils.queries.get_query_result(
                    dlt_pipeline, dlt_query
                )
                dlt_set_last_query_result(_query_result)

            # display error message if encountered
            if _error_message:
                _result.append(
                    utils.ui.error_callout(
                        strings.browse_data_query_error + _error_message,
                        traceback_string=_traceback_string,
                    )
                )

        # always display result table
        _last_result = dlt_get_last_query_result()
        if _last_result is not None:
            _result.append(utils.ui.dlt_table(_last_result, freeze_column=None))

        # update query history if there was no error
        if _last_result is not None and not _error_message:
            if dlt_query:
                dlt_set_query_history(
                    utils.queries.update_query_history(
                        dlt_get_query_history(), dlt_query, _last_result.shape[0]
                    )
                )

    # provide query history table
    if _query_history := dlt_get_query_history():
        dlt_query_history_table = utils.ui.dlt_table(
            [{"query": q, "row_count": _query_history[q]} for q in _query_history],
            selection="multi",
            freeze_column=None,
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

    _result: List[mo.Html] = []
    if (
        dlt_pipeline
        and dlt_section_browse_data_switch.value
        and dlt_query_history_table is not None
    ):
        _result.append(
            utils.ui.title_and_subtitle(
                strings.browse_data_query_history_title, strings.browse_data_query_history_subtitle
            )
        )
        _result.append(dlt_query_history_table)

        for _r in cast(List[TQueryHistoryItem], dlt_query_history_table.value):
            _query = _r["query"]
            _q_result, _q_error, _q_traceback = utils.queries.get_query_result(dlt_pipeline, _query)
            _result.append(mo.md(utils.ui.small(f"```{_query}```")))
            if _q_error:
                _result.append(utils.ui.error_callout(_q_error, traceback_string=_q_traceback))
            else:
                _result.append(utils.ui.dlt_table(_q_result, freeze_column=None))
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
    _result, _show = utils.ui.section(strings.state, dlt_pipeline, dlt_section_state_switch)

    if _show:
        _result.append(
            mo.json(
                dlt_pipeline.state,  # type: ignore[arg-type]
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

    _result, _show = utils.ui.section(strings.trace, dlt_pipeline, dlt_section_trace_switch)

    if _show:
        try:
            _result.extend(
                utils.trace.build_trace_section(dlt_config, dlt_pipeline, dlt_trace_steps_table)
            )
        except Exception as exc:
            _result.append(
                utils.ui.error_callout(
                    strings.trace_error_building_section.format(exc),
                )
            )
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def section_loads(
    dlt_config: DashboardConfiguration,
    dlt_clear_result_cache: mo.ui.run_button,
    dlt_pipeline: dlt.Pipeline,
    dlt_restrict_to_last_1000: mo.ui.switch,
    dlt_section_loads_switch: mo.ui.switch,
):
    """
    Show loads of the currently selected pipeline
    """

    _result, _show = utils.ui.section(strings.loads, dlt_pipeline, dlt_section_loads_switch)

    if _show:
        _result.append(mo.hstack([dlt_restrict_to_last_1000], justify="start"))
        with mo.status.spinner(title=strings.loads_loading_spinner_text):
            _loads_data, _error_message, _traceback_string = utils.queries.get_loads(
                dlt_config,
                dlt_pipeline,
                limit=1000 if dlt_restrict_to_last_1000.value else None,
            )
            dlt_loads_table = utils.ui.dlt_table(_loads_data, selection="single")
            if _error_message:
                _result.append(
                    utils.ui.error_callout(
                        strings.loads_loading_failed_text + _error_message,
                        traceback_string=_traceback_string,
                    )
                )
            _result.append(dlt_loads_table)
            _result.append(dlt_clear_result_cache)
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
        _selected_load = cast(List[TLoadItem], dlt_loads_table.value)[0]
        try:
            _result.extend(utils.queries.build_load_details(dlt_pipeline, _selected_load))
        except Exception:
            _result.append(utils.ui.error_callout(strings.loads_details_error_text))
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def section_ibis_backend(
    dlt_pipeline: dlt.Pipeline,
    dlt_section_ibis_browser_switch: mo.ui.switch,
):
    """
    Connects to ibis backend and makes it available in the datasources panel
    """
    _result, _show = utils.ui.section(
        strings.ibis_backend, dlt_pipeline, dlt_section_ibis_browser_switch
    )

    if _show:
        try:
            with mo.status.spinner(title=strings.ibis_backend_connecting_spinner_text):
                con = dlt_pipeline.dataset().ibis(read_only=True)
            _result.append(
                mo.callout(mo.vstack([mo.md(strings.ibis_backend_connected_text)]), kind="success")
            )
        except Exception as exc:
            _result.append(utils.ui.error_callout(strings.ibis_backend_error_text + str(exc)))
    mo.vstack(_result) if _result else None
    return


@app.cell(hide_code=True)
def utils_discover_pipelines(
    dlt_profile_select: mo.ui.dropdown,
    mo_cli_arg_pipelines_dir: str,
    mo_cli_arg_pipeline: str,
    mo_query_var_pipeline_name: str,
):
    """
    Discovers local pipelines and returns a multiselect widget to select one of the pipelines
    """
    from dlt._workspace.cli.utils import list_local_pipelines

    # sync from runtime if enabled
    _tmp_config = utils.pipeline.resolve_dashboard_config(None)
    if _tmp_config.sync_from_runtime:
        from dlt._workspace.helpers.runtime.runtime_artifacts import sync_from_runtime

        with mo.status.spinner(title=strings.home_syncing_spinner_text):
            sync_from_runtime()

    _run_context = dlt.current.run_context()
    if (
        isinstance(_run_context, ProfilesRunContext)
        and not _run_context.profile == dlt_profile_select.value
    ):
        switch_profile(dlt_profile_select.value)

    # discover pipelines and build selector
    dlt_pipelines_dir: str = ""
    dlt_all_pipelines: List[TPipelineListItem] = []
    dlt_pipelines_dir, dlt_all_pipelines = list_local_pipelines(
        mo_cli_arg_pipelines_dir,
        additional_pipelines=[mo_cli_arg_pipeline, mo_query_var_pipeline_name],
    )

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
def utils_discover_profiles(mo_query_var_profile: str, mo_cli_arg_profile: str):
    """Discover profiles and return a single-select multiselect, similar to pipelines."""
    run_context = dlt.current.run_context()

    # Default (non-profile-aware) output
    dlt_profile_select = mo.ui.dropdown(
        options=[], value=None, label=strings.app_profile_select_label
    )
    selected_profile = None

    if isinstance(run_context, ProfilesRunContext):
        options = run_context.configured_profiles() or []
        current = run_context.profile if options and run_context.profile in options else None

        selected_profile = current
        if mo_query_var_profile and mo_query_var_profile in options:
            selected_profile = mo_query_var_profile
        elif mo_cli_arg_profile and mo_cli_arg_profile in options:
            selected_profile = mo_cli_arg_profile

        def _on_profile_change(v: str) -> None:
            mo.query_params().set("profile", v)

        dlt_profile_select = mo.ui.dropdown(
            options=options,
            value=selected_profile,
            label=strings.app_profile_select_label,
            on_change=_on_profile_change,
            searchable=True,
        )

    return dlt_profile_select, selected_profile


@app.cell(hide_code=True)
def utils_discover_schemas(dlt_pipeline: dlt.Pipeline):
    """
    Create schema multiselect widget
    """
    schemas = dlt_pipeline.schemas.values() if (dlt_pipeline and dlt_pipeline.schemas) else []
    dlt_schema_select: mo.ui.dropdown = mo.ui.dropdown(
        options=[s.name for s in schemas],
        value=(
            dlt_pipeline.default_schema_name
            if (dlt_pipeline and dlt_pipeline.default_schema_name)
            else None
        ),
        label=strings.app_schema_select_label,
    )
    return dlt_schema_select


@app.cell(hide_code=True)
def state_and_cache_reset(
    dlt_clear_result_cache: mo.ui.run_button,
    dlt_pipeline: dlt.Pipeline,
):
    """State variables for query results and history, plus cache clearing."""

    dlt_get_last_query_result, dlt_set_last_query_result = mo.state(pyarrow.table({}))
    # query history: maps query text to row count for the history sidebar
    dlt_get_query_history, dlt_set_query_history = mo.state(cast(Dict[str, int], {}))

    if dlt_clear_result_cache.value:
        utils.queries.clear_query_cache()

    return


@app.cell(hide_code=True)
def ui_controls(mo_cli_arg_with_test_identifiers: bool):
    """
    Control elements for various parts of the app
    """

    dlt_refresh_button: mo.ui.run_button = mo.ui.run_button(
        label=utils.ui.small(strings.app_refresh_button),
    )

    # page switches
    dlt_section_sync_switch: mo.ui.switch = mo.ui.switch(
        value=True, label="sync" if mo_cli_arg_with_test_identifiers else ""
    )
    dlt_section_info_switch: mo.ui.switch = mo.ui.switch(
        value=False, label="overview" if mo_cli_arg_with_test_identifiers else ""
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
    dlt_section_data_quality_switch: mo.ui.switch = mo.ui.switch(
        value=False, label="data_quality" if mo_cli_arg_with_test_identifiers else ""
    )

    # other switches
    dlt_schema_show_dlt_tables: mo.ui.switch = mo.ui.switch(
        label=utils.ui.small(strings.ui_show_dlt_tables)
    )
    dlt_schema_show_child_tables: mo.ui.switch = mo.ui.switch(
        label=utils.ui.small(strings.ui_show_child_tables), value=True
    )
    dlt_schema_show_row_counts: mo.ui.run_button = mo.ui.run_button(
        label=utils.ui.small(strings.ui_load_row_counts)
    )
    dlt_schema_show_dlt_columns: mo.ui.switch = mo.ui.switch(
        label=utils.ui.small(strings.ui_show_dlt_columns)
    )
    dlt_schema_show_type_hints: mo.ui.switch = mo.ui.switch(
        label=utils.ui.small(strings.ui_show_type_hints), value=True
    )
    dlt_schema_show_other_hints: mo.ui.switch = mo.ui.switch(
        label=utils.ui.small(strings.ui_show_other_hints), value=False
    )
    dlt_schema_show_custom_hints: mo.ui.switch = mo.ui.switch(
        label=utils.ui.small(strings.ui_show_custom_hints), value=False
    )
    dlt_clear_result_cache: mo.ui.run_button = mo.ui.run_button(
        label=utils.ui.small(strings.ui_clear_cache)
    )
    dlt_restrict_to_last_1000: mo.ui.switch = mo.ui.switch(
        label=utils.ui.small(strings.ui_limit_to_1000_rows), value=True
    )
    return (
        dlt_clear_result_cache,
        dlt_restrict_to_last_1000,
        dlt_schema_show_child_tables,
        dlt_schema_show_custom_hints,
        dlt_schema_show_dlt_columns,
        dlt_schema_show_dlt_tables,
        dlt_schema_show_other_hints,
        dlt_schema_show_row_counts,
        dlt_schema_show_type_hints,
        dlt_section_browse_data_switch,
        dlt_section_data_quality_switch,
        dlt_section_ibis_browser_switch,
        dlt_section_loads_switch,
        dlt_section_info_switch,
        dlt_section_schema_switch,
        dlt_section_state_switch,
        dlt_section_sync_switch,
        dlt_section_trace_switch,
    )


@app.cell(hide_code=True)
def watch_changes(
    dlt_pipeline_select: mo.ui.multiselect,
    dlt_pipelines_dir: str,
):
    """
    Watch changes in the trace file and trigger reload in the home cell and all following cells on change
    """
    from dlt.pipeline.trace import get_trace_file_path

    # provide pipeline object to the following cells
    dlt_pipeline_name: str = (
        str(dlt_pipeline_select.value[0]) if dlt_pipeline_select.value else None
    )
    dlt_file_watcher = None
    if dlt_pipeline_name:
        dlt_file_watcher = mo.watch.file(get_trace_file_path(dlt_pipelines_dir, dlt_pipeline_name))
    return dlt_pipeline_name, dlt_file_watcher


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
    dlt_schema_select: mo.ui.multiselect,
):
    """
    Helper cell for creating certain controls based on selected sections
    """

    dlt_selected_schema_name = (
        cast(str, dlt_schema_select.value) if dlt_schema_select.value else None
    )

    #
    # Schema controls
    #
    dlt_schema_table_list: mo.ui.table = None
    if dlt_section_schema_switch.value and dlt_pipeline and dlt_selected_schema_name:
        _table_list = utils.schema.create_table_list(
            dlt_config,
            dlt_pipeline,
            dlt_selected_schema_name,
            show_internals=dlt_schema_show_dlt_tables.value,
            show_child_tables=dlt_schema_show_child_tables.value,
        )
        dlt_schema_table_list = utils.ui.dlt_table(
            _table_list,
            selection="multi",
            initial_selection=[0],
        )

    #
    # Browse data controls
    #
    dlt_data_table_list: mo.ui.table = None
    if dlt_section_browse_data_switch.value and dlt_pipeline and dlt_selected_schema_name:
        table_list = utils.schema.create_table_list(
            dlt_config,
            dlt_pipeline,
            dlt_selected_schema_name,
            show_internals=dlt_schema_show_dlt_tables.value,
            show_child_tables=dlt_schema_show_child_tables.value,
            show_row_counts=dlt_schema_show_row_counts.value,
        )
        dlt_data_table_list = utils.ui.dlt_table(
            table_list,
            selection="single",
            initial_selection=[0],
        )

    #
    # Trace steps table
    #
    dlt_trace_steps_table: mo.ui.table = None
    try:
        if dlt_section_trace_switch.value and dlt_pipeline and dlt_pipeline.last_trace:
            dlt_trace_steps_table = utils.ui.dlt_table(
                utils.trace.trace_steps_overview(dlt_config, dlt_pipeline.last_trace),
                selection="multi",
                freeze_column="step",
            )
    except Exception as exc:
        logger.error(f"Error while building trace steps table: {exc}")
    return (
        dlt_data_table_list,
        dlt_schema_table_list,
        dlt_trace_steps_table,
        dlt_selected_schema_name,
    )


@app.cell(hide_code=True)
def utils_cli_args_and_query_vars_config():
    """
    Prepare cli args  as globals for the following cells
    """
    _run_context = dlt.current.run_context()
    mo_query_var_pipeline_name: str = None
    mo_cli_arg_pipelines_dir: str = None
    mo_cli_arg_with_test_identifiers: bool = False
    mo_cli_arg_pipeline: str = None
    mo_query_var_profile: str = None
    mo_cli_arg_profile: str = None
    try:
        mo_query_var_pipeline_name = cast(str, mo.query_params().get("pipeline")) or None
        mo_cli_arg_pipeline = cast(str, mo.cli_args().get("pipeline")) or None
        mo_cli_arg_pipelines_dir = cast(str, mo.cli_args().get("pipelines-dir")) or None
        mo_cli_arg_with_test_identifiers = (
            cast(bool, mo.cli_args().get("with_test_identifiers")) or False
        )
        mo_query_var_profile = (
            cast(str, mo.query_params().get("profile")) or None
            if isinstance(_run_context, ProfilesRunContext)
            else None
        )
        mo_cli_arg_profile = (
            cast(str, mo.cli_args().get("profile")) or None
            if isinstance(_run_context, ProfilesRunContext)
            else None
        )
    except Exception:
        pass

    return (
        mo_cli_arg_pipelines_dir,
        mo_cli_arg_with_test_identifiers,
        mo_query_var_pipeline_name,
        mo_cli_arg_pipeline,
        mo_query_var_profile,
        mo_cli_arg_profile,
    )


if __name__ == "__main__":
    app.run()
