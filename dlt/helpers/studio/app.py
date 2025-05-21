# flake8: noqa: F841

from typing import Any

import dlt
import marimo
import pandas as pd

__generated_with = "0.13.6"
app = marimo.App(width="medium", app_title="dlt studio", css_file="style.css")


@app.cell(hide_code=True)
def page_welcome(
    dlt_pipelines_dir: str,
    dlt_pipeline_select: marimo.ui.multiselect,
    dlt_pipeline_count: int,
    dlt_pipeline_link_list: str,
) -> Any:
    """
    Displays the welcome page with the pipeline select widget, will only display pipeline title if a pipeline is selected
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u

    _selected_pipeline_name = dlt_pipeline_select.value[0] if dlt_pipeline_select.value else None

    if not _selected_pipeline_name:
        _stack = [
            _mo.hstack(
                [
                    _mo.image(
                        "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
                    ),
                    dlt_pipeline_select,
                ],
            ),
            _mo.md(_s.app_title).center(),
            _mo.md(_s.app_intro).center(),
            _mo.callout(
                _mo.vstack(
                    [
                        _mo.md(_s.app_quick_start_title.format(dlt_pipeline_link_list)),
                        dlt_pipeline_select,
                    ]
                ),
                kind="info",
            ),
            _mo.md(_s.app_basics_text.format(dlt_pipeline_count, dlt_pipelines_dir)),
        ]
    else:
        _stack = [
            _mo.hstack(
                [
                    _mo.image(
                        "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
                    ).style(padding_bottom="1em"),
                    _mo.center(
                        _mo.md(f"## {_s.app_title_pipeline.format(_selected_pipeline_name)}")
                    ),
                    dlt_pipeline_select,
                ],
            ),
        ]
    _mo.vstack(_stack)


@app.cell(hide_code=True)
def app_tabs(dlt_pipeline_name: str, dlt_pipelines_dir: str) -> Any:
    """
    Syncs the pipeline and renders the result of the sync
    """

    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u

    _mo.stop(not dlt_pipeline_name)

    # we also provide the pipeline object to the cells that need it
    dlt_pipeline = _u.get_pipeline(dlt_pipeline_name, dlt_pipelines_dir)

    # build dlt_page_tabs
    dlt_page_tabs = _mo.ui.tabs(
        {v: "" for k, v in _s.app_tab_mapping.items()},
        value=list(_s.app_tab_mapping.values())[0],
    )

    _mo.center(dlt_page_tabs)
    return (dlt_page_tabs, dlt_pipeline)


@app.cell(hide_code=True)
def page_overview(
    dlt_page_tabs: marimo.ui.tabs,
    dlt_pipeline: dlt.Pipeline,
) -> Any:
    """
    Overview page of currently selected pipeline
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u, ui_elements as _ui

    _mo.stop(not dlt_pipeline or _s.app_tab_overview not in dlt_page_tabs.value)

    # sync pipeline
    with _mo.status.spinner(title="Syncing pipeline state from destination..."):
        try:
            dlt_pipeline.sync_destination()
            _credentials = str(dlt_pipeline.dataset().destination_client.config.credentials)
            _sync_result = _mo.callout(
                _mo.vstack([_mo.md(_s.pipeline_sync_success_text.format(_credentials))]),
                kind="success",
            )

        except Exception:
            _sync_result = _ui.build_error_callout(_s.pipeline_sync_error_text)

    _mo.vstack(
        [
            _mo.md(_s.pipeline_sync_status),
            _sync_result,
            _mo.md(_s.pipeline_details),
            _mo.ui.table(
                _u.pipeline_details(dlt_pipeline),
                selection=None,
                style_cell=_u.style_cell,
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def app_controls() -> Any:
    """
    Control elements for various parts of the app
    """
    import marimo as _mo

    dlt_schema_show_dlt_tables = _mo.ui.switch(label="<small>Show `_dlt` tables</small>")
    dlt_schema_show_child_tables = _mo.ui.switch(
        label="<small>Show child tables</small>", value=True
    )
    dlt_schema_show_row_counts = _mo.ui.switch(label="<small>Show row counts</small>", value=False)
    dlt_schema_show_dlt_columns = _mo.ui.switch(label="<small>Show `_dlt` columns</small>")
    dlt_schema_show_type_hints = _mo.ui.switch(label="<small>Show type hints</small>", value=True)
    dlt_schema_show_other_hints = _mo.ui.switch(
        label="<small>Show other hints</small>", value=False
    )
    dlt_schema_show_custom_hints = _mo.ui.switch(
        label="<small>Show custom hints (x-)</small>", value=False
    )
    dlt_cache_query_results = _mo.ui.switch(label="<small>Cache query results</small>", value=True)
    dlt_execute_query_on_change = _mo.ui.switch(
        label="<small>Execute query automatically on change (loose focus)</small>", value=False
    )
    return


@app.cell(hide_code=True)
def page_schema_section_table_list(
    dlt_pipeline: dlt.Pipeline,
    dlt_schema_show_child_tables: marimo.ui.switch,
    dlt_schema_show_dlt_tables: marimo.ui.switch,
    dlt_page_tabs: marimo.ui.tabs,
) -> Any:
    """
    Show schema of the currently selected pipeline
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u

    _mo.stop(not dlt_pipeline or _s.app_tab_schema not in dlt_page_tabs.value)

    if not dlt_pipeline.default_schema_name:
        dlt_schem_table_list = _mo.callout(
            _mo.md("No Schema available. Does your pipeline have a completed load?"),
            kind="warn",
        )
    else:
        _table_list = _u.create_table_list(
            dlt_pipeline,
            show_internals=dlt_schema_show_dlt_tables.value,
            show_child_tables=dlt_schema_show_child_tables.value,
        )
        dlt_schem_table_list = _mo.ui.table(
            _table_list,  # type: ignore[arg-type]
            style_cell=_u.style_cell,
            initial_selection=list(range(0, len(_table_list))),
        )

    _mo.vstack(
        [
            _mo.md(
                _s.schema_table_overview.format(
                    dlt_pipeline.default_schema_name or "<no schema found>"
                )
            ),
            _mo.hstack([dlt_schema_show_dlt_tables, dlt_schema_show_child_tables], justify="start"),
            dlt_schem_table_list,
        ]
    )
    return (dlt_schem_table_list,)


@app.cell(hide_code=True)
def page_schema_section_table_schemas(
    dlt_pipeline: dlt.Pipeline,
    dlt_schem_table_list: marimo.ui.table,
    dlt_schema_show_other_hints: marimo.ui.switch,
    dlt_schema_show_custom_hints: marimo.ui.switch,
    dlt_schema_show_dlt_columns: marimo.ui.switch,
    dlt_schema_show_type_hints: marimo.ui.switch,
    dlt_page_tabs: marimo.ui.tabs,
) -> Any:
    """
    Show schema of the currently selected table
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u

    _mo.stop(not dlt_pipeline or _s.app_tab_schema not in dlt_page_tabs.value)

    _stack = []

    for table in dlt_schem_table_list.value:  # type: ignore[union-attr]
        _table_name = table["Name"]  # type: ignore[index]
        _stack.append(_mo.md(f"### `{_table_name}`"))
        _stack.append(
            _mo.ui.table(
                _u.create_column_list(
                    dlt_pipeline,
                    _table_name,
                    show_internals=dlt_schema_show_dlt_columns.value,
                    show_type_hints=dlt_schema_show_type_hints.value,
                    show_other_hints=dlt_schema_show_other_hints.value,
                    show_custom_hints=dlt_schema_show_custom_hints.value,
                ),
                selection=None,
                style_cell=_u.style_cell,
            )
        )

    if _stack:
        _stack.insert(0, _mo.md("## Table details"))
        _stack.insert(
            1,
            _mo.hstack(
                [
                    dlt_schema_show_dlt_columns,
                    dlt_schema_show_type_hints,
                    dlt_schema_show_other_hints,
                    dlt_schema_show_custom_hints,
                ],
                justify="start",
            ),
        )

    _mo.vstack(_stack)
    return


@app.cell(hide_code=True)
def page_schema_section_raw_schema(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_tabs: marimo.ui.tabs,
) -> Any:
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u

    _mo.stop(
        not dlt_pipeline
        or _s.app_tab_schema not in dlt_page_tabs.value
        or not dlt_pipeline.default_schema_name
    )

    _mo.vstack(
        [
            _mo.md(_s.schema_raw_title),
            _mo.accordion(
                {
                    "Show raw schema as yaml": _mo.ui.code_editor(
                        dlt_pipeline.default_schema.to_pretty_yaml(),
                        language="yaml",
                    )
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def page_browse_data_section_table_list(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_tabs: marimo.ui.tabs,
    dlt_schema_show_child_tables: marimo.ui.switch,
    dlt_schema_show_dlt_tables: marimo.ui.switch,
    dlt_schema_show_row_counts: marimo.ui.switch,
) -> Any:
    """
    Show data of the currently selected pipeline
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u, ui_elements as _ui

    _mo.stop(not dlt_pipeline or _s.app_tab_browse_data not in dlt_page_tabs.value)

    # try to connect to the dataset
    try:
        dlt_pipeline.dataset().destination_client.config.credentials
        with _mo.status.spinner(title="Getting table list..."):
            dlt_data_table_list = _mo.ui.table(
                _u.create_table_list(  # type: ignore[arg-type]
                    dlt_pipeline,
                    show_internals=dlt_schema_show_dlt_tables.value,
                    show_child_tables=dlt_schema_show_child_tables.value,
                    show_row_counts=dlt_schema_show_row_counts.value,
                ),
                style_cell=_u.style_cell,
                selection="single",
            )
            _connect_result = dlt_data_table_list
    except Exception:
        dlt_data_table_list = None
        _connect_result = _ui.build_error_callout(_s.browse_data_error)

    _mo.vstack(
        [
            _mo.md(
                _s.browse_data_title.format(
                    dlt_pipeline.default_schema_name or "<no schema found>",
                    dlt_pipeline.dataset_name,
                )
            ),
            _mo.hstack(
                [
                    dlt_schema_show_dlt_tables,
                    dlt_schema_show_child_tables,
                    dlt_schema_show_row_counts,
                ],
                justify="start",
            ),
            _connect_result,
        ]
    )

    return


@app.cell(hide_code=True)
def page_browse_data_section_query_editor(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_tabs: marimo.ui.tabs,
    dlt_data_table_list: marimo.ui.table,
    dlt_cache_query_results: marimo.ui.switch,
    dlt_execute_query_on_change: marimo.ui.switch,
) -> Any:
    """
    Show data of the currently selected pipeline
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u

    _mo.stop(
        not dlt_pipeline
        or _s.app_tab_browse_data not in dlt_page_tabs.value
        or not dlt_data_table_list
    )

    _sql_query = ""
    if dlt_data_table_list.value:
        _table_name = dlt_data_table_list.value[0]["Name"]  # type: ignore[index]
        _sql_query = dlt_pipeline.dataset().table(_table_name).limit(1000).query()

    dlt_query_editor = _mo.ui.code_editor(
        language="sql",
        placeholder="SELECT \n  * \nFROM dataset.table \nLIMIT 1000",
        value=_sql_query,
        debounce=True,
    )
    dlt_run_query_button = _mo.ui.run_button(
        label="Run query", tooltip="Run the query in the editor"
    )

    _ui_items = [
        _mo.md(_s.browse_data_explorer_title),
        _mo.hstack([dlt_cache_query_results, dlt_execute_query_on_change], justify="start"),
        dlt_query_editor,
    ]

    if not dlt_execute_query_on_change.value:
        _ui_items.append(dlt_run_query_button)
    else:
        _ui_items.append(
            _mo.md(
                "<small>Query will be executed automatically when you select a table or change the"
                " query</small>"
            )
        )

    _mo.vstack(_ui_items)

    return dlt_run_query_button


@app.cell(hide_code=True)
def page_browse_data_section_execute_query(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_tabs: marimo.ui.tabs,
    dlt_run_query_button: marimo.ui.button,
    dlt_query_editor: marimo.ui.code_editor,
    dlt_cache_query_results: marimo.ui.switch,
    dlt_execute_query_on_change: marimo.ui.switch,
) -> Any:
    """
    Execute the query in the editor
    """
    import marimo as _mo
    import sqlglot as _slqglot
    from dlt.helpers.studio import strings as _s, utils as _u, ui_elements as _ui

    _mo.stop(not dlt_pipeline or _s.app_tab_browse_data not in dlt_page_tabs.value)

    _query_error = None
    dlt_query_result = None
    with _mo.status.spinner(title="Loading data from destination"):
        if dlt_query_editor.value and (
            dlt_run_query_button.value or dlt_execute_query_on_change.value
        ):
            try:
                _slqglot.parse_one(dlt_query_editor.value)
                dlt_query = dlt_query_editor.value
                if not dlt_cache_query_results.value:
                    _u.get_query_result.cache_clear()
                dlt_query_result = _u.get_query_result(dlt_pipeline, dlt_query)
            except Exception as e:
                _query_error = _ui.build_error_callout(_s.browse_data_query_error, code=str(e))

    # always show last query if nothing was found
    if dlt_query_result is None:
        dlt_query_result = _u.get_last_query_result(dlt_pipeline)
        dlt_query = _u.get_last_query(dlt_pipeline)

    _query_error


@app.cell(hide_code=True)
def page_browse_data_section_data_explorer(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_tabs: marimo.ui.tabs,
    dlt_data_table_list: marimo.ui.table,
    dlt_query_result: pd.DataFrame,
    dlt_query: str,
) -> Any:
    """
    Show data of the currently selected pipeline
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u

    _mo.stop(
        not dlt_pipeline
        or _s.app_tab_browse_data not in dlt_page_tabs.value
        or not dlt_data_table_list
    )

    _mo.vstack(
        [
            _mo.md(_s.browse_data_query_result_title),
            _mo.ui.table(dlt_query_result, selection=None),
            _mo.md(f"`{dlt_query}`"),
        ]
    )

    return


@app.cell(hide_code=True)
def page_state(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_tabs: marimo.ui.tabs,
) -> Any:
    """
    Show state of the currently selected pipeline
    """
    from dlt.common.json import json as _json
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u

    _mo.stop(not dlt_pipeline or _s.app_tab_state not in dlt_page_tabs.value)

    _mo.vstack(
        [
            _mo.md(_s.state_raw_title),
            _mo.ui.code_editor(
                _json.dumps(dlt_pipeline.state, pretty=True),
                language="json",
            ),
        ]
    )


@app.cell(hide_code=True)
def ibis_browser_page(
    dlt_pipeline: dlt.Pipeline,
    dlt_page_tabs: marimo.ui.tabs,
) -> Any:
    """
    Connects to ibis backend and makes it available in the datasources panel
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, utils as _u, ui_elements as _ui

    _mo.stop(not dlt_pipeline or _s.app_tab_ibis_browser not in dlt_page_tabs.value)

    try:
        with _mo.status.spinner(title="Connecting Ibis Backend..."):
            con = dlt_pipeline.dataset().ibis()
        _connect_result = _mo.callout(
            _mo.vstack([_mo.md(_s.ibis_backend_connected)]), kind="success"
        )
    except Exception:
        _connect_result = _ui.build_error_callout(_s.ibis_connect_error)

    _mo.vstack([_mo.md(_s.ibis_backend_title), _connect_result])
    return


#
# Utility Cells
#


@app.cell(hide_code=True)
def app_discover_pipelines(cli_arg_pipelines_dir: str) -> Any:
    """
    Discovers local pipelines and returns a multiselect widget to select one of the pipelines
    """

    import marimo as _mo
    from datetime import datetime
    from dlt.helpers.studio import strings as _s, utils as _u

    # note: this exception handling is only needed for testing
    try:
        dlt_query_params = _mo.query_params()
    except Exception:
        dlt_query_params = {}  # type: ignore[assignment]

    dlt_pipelines_dir, _pipelines = _u.get_local_pipelines(cli_arg_pipelines_dir)
    dlt_pipeline_count = len(_pipelines)
    dlt_pipeline_select = _mo.ui.multiselect(
        options=[p["name"] for p in _pipelines],
        value=[dlt_query_params.get("pipeline")] if dlt_query_params.get("pipeline") else None,
        max_selections=1,
        label=_s.pipeline_select_label,
        on_change=lambda value: dlt_query_params.set("pipeline", str(value[0]) if value else None),
    )

    _count = 0
    dlt_pipeline_link_list = ""
    for _p in _pipelines:
        link = f"* [{_p['name']}](?pipeline={_p['name']})"
        if _p["timestamp"] == 0:
            link = link + " - never used"
        else:
            link = (
                link
                + " - last executed"
                f" {datetime.fromtimestamp(_p['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}"
            )

        dlt_pipeline_link_list += f"{link}\n"
        _count += 1
        if _count == 5:
            break

    if not dlt_pipeline_link_list:
        dlt_pipeline_link_list = "No local pipelines found."

    return (dlt_pipelines_dir, dlt_pipeline_select, dlt_pipeline_count, dlt_query_params)


@app.cell(hide_code=True)
def prepare_query_vars(dlt_query_params: Any) -> Any:
    """
    Prepare query params as globals for the following cells
    """
    import marimo as _mo
    from dlt.helpers.studio import utils as _u

    dlt_pipeline_name = dlt_query_params.get("pipeline") or None
    dlt_current_page = dlt_query_params.get("page") or None
    return (dlt_pipeline_name, dlt_current_page)


@app.cell(hide_code=True)
def prepare_cli_args() -> Any:
    """
    Prepare cli args  as globals for the following cells
    """
    import marimo as _mo

    dlt_cli_args = _mo.cli_args()

    cli_arg_pipelines_dir = dlt_cli_args.get("pipelines_dir") or None


if __name__ == "__main__":
    app.run()
