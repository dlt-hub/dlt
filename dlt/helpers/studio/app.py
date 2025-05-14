# mypy: disable-error-code="no-untyped-def"
# flake8: noqa: F841

import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium", app_title="dlt studio", css_file="style.css")


@app.cell(hide_code=True)
def page_welcome(dlt_pipelines_dir, dlt_pipeline_select, dlt_pipeline_count):
    """
    Displays the welcome page with the pipeline select widget, will only display pipeline title if a pipeline is selected
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, helpers as _h

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
                _mo.vstack([_mo.md(_s.app_quick_start_title), dlt_pipeline_select]), kind="info"
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
    return


@app.cell(hide_code=True)
def app_tabs(dlt_pipeline_name):
    """
    Syncs the pipeline and renders the result of the sync
    """

    import marimo as _mo
    from dlt.helpers.studio import strings as _s

    _mo.stop(not dlt_pipeline_name)

    # build dlt_page_tabs
    dlt_page_tabs = _mo.ui.tabs(
        {v: "" for k, v in _s.app_tab_mapping.items()},
        value=list(_s.app_tab_mapping.values())[0],
    )

    _mo.center(dlt_page_tabs)
    return (dlt_page_tabs,)


@app.cell(hide_code=True)
def page_overview(
    dlt_pipeline_name,
    dlt_pipelines_dir,
    dlt_page_tabs,
):
    """
    Overview page of currently selected pipeline
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, helpers as _h, ui_elements as _ui

    dlt_destination_credentials = "Could not resolve credentials"
    _mo.stop(not dlt_pipeline_name or _s.app_tab_overview not in dlt_page_tabs.value)

    # sync pipeline
    with _mo.status.spinner(title="Syncing pipeline state from destination..."):
        try:
            _h.get_pipeline(dlt_pipeline_name).sync_destination()
            dlt_destination_credentials = str(
                _h.get_pipeline(dlt_pipeline_name).dataset().destination_client.config.credentials
            )
            _sync_result = _mo.callout(
                _mo.vstack(
                    [_mo.md(_s.pipeline_sync_success_text.format(dlt_destination_credentials))]
                ),
                kind="success",
            )

        except Exception:
            _sync_result = _ui.build_error_callout(_s.pipeline_sync_error_text)

    dlt_pipeline = _h.get_pipeline(dlt_pipeline_name)
    _mo.vstack(
        [
            _mo.md(_s.pipeline_sync_status),
            _sync_result,
            _mo.md(_s.pipeline_details),
            _mo.ui.table(
                [  # type: ignore[arg-type]
                    {"Key": "Pipeline name", "Value": dlt_pipeline.pipeline_name},
                    {
                        "Key": "Destination",
                        "Value": (
                            dlt_pipeline.destination.destination_description
                            if dlt_pipeline.destination
                            else "No destination set"
                        ),
                    },
                    {
                        "Key": "Credentials",
                        "Value": dlt_destination_credentials,
                    },
                    {"Key": "Dataset name", "Value": dlt_pipeline.dataset_name},
                    {"Key": "Schema name", "Value": dlt_pipeline.default_schema_name},
                    {
                        "Key": "Local pipeline directory",
                        "Value": dlt_pipelines_dir + "/" + dlt_pipeline_name,
                    },
                ],
                selection=None,
                style_cell=_h.style_cell,
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def page_schema_controls():
    """
    Control panel for the schema page
    """
    import marimo as _mo

    dlt_schema_show_dlt_tables = _mo.ui.switch(label="<small>Show `_dlt` tables</small>")
    dlt_schema_show_child_tables = _mo.ui.switch(
        label="<small>Show child tables</small>", value=True
    )
    dtl_schema_show_dlt_columns = _mo.ui.switch(label="<small>Show `_dlt` columns</small>")
    dtl_schema_show_type_hints = _mo.ui.switch(label="<small>Show type hints</small>", value=True)
    dlt_schema_show_other_hints = _mo.ui.switch(
        label="<small>Show other hints</small>", value=False
    )
    dtl_schema_show_custom_hints = _mo.ui.switch(
        label="<small>Show custom hints (x-)</small>", value=False
    )
    return (
        dlt_schema_show_child_tables,
        dlt_schema_show_dlt_tables,
        dlt_schema_show_other_hints,
        dtl_schema_show_custom_hints,
        dtl_schema_show_dlt_columns,
        dtl_schema_show_type_hints,
    )


@app.cell(hide_code=True)
def page_schema_section_table_list(
    dlt_pipeline_name,
    dlt_schema_show_child_tables,
    dlt_schema_show_dlt_tables,
    dlt_page_tabs,
):
    """
    Show schema of the currently selected pipeline
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, helpers as _h

    _mo.stop(not dlt_pipeline_name or _s.app_tab_schema not in dlt_page_tabs.value)

    if not _h.get_pipeline(dlt_pipeline_name).default_schema_name:
        dlt_schem_table_list = _mo.callout(
            _mo.md("No Schema available. Does your pipeline have a completed load?"),
            kind="warn",
        )
    else:
        dlt_schem_table_list = _mo.ui.table(
            _h.create_table_list(  # type: ignore[arg-type]
                dlt_pipeline_name,
                show_internals=dlt_schema_show_dlt_tables.value,
                show_child_tables=dlt_schema_show_child_tables.value,
            ),
            style_cell=_h.style_cell,
        )

    _mo.vstack(
        [
            _mo.md(
                _s.schema_table_overview.format(
                    _h.get_pipeline(dlt_pipeline_name).default_schema_name or "<no schema found>"
                )
            ),
            _mo.hstack([dlt_schema_show_dlt_tables, dlt_schema_show_child_tables], justify="start"),
            dlt_schem_table_list,
        ]
    )
    return (dlt_schem_table_list,)


@app.cell(hide_code=True)
def page_schema_section_table_schemas(
    dlt_pipeline_name,
    dlt_schem_table_list,
    dlt_schema_show_other_hints,
    dtl_schema_show_custom_hints,
    dtl_schema_show_dlt_columns,
    dtl_schema_show_type_hints,
    dlt_page_tabs,
):
    """
    Show schema of the currently selected table
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, helpers as _h

    _mo.stop(
        not dlt_pipeline_name
        or _s.app_tab_schema not in dlt_page_tabs.value
        or not _h.get_pipeline(dlt_pipeline_name).default_schema_name
    )

    _stack = []

    for table in dlt_schem_table_list.value:
        _stack.append(_mo.md(f"### `{table['Name']}`"))
        _stack.append(
            _mo.ui.table(
                _h.create_column_list(  # type: ignore[arg-type]
                    dlt_pipeline_name,
                    table["Name"],
                    show_internals=dtl_schema_show_dlt_columns.value,
                    show_type_hints=dtl_schema_show_type_hints.value,
                    show_other_hints=dlt_schema_show_other_hints.value,
                    show_custom_hints=dtl_schema_show_custom_hints.value,
                ),
                selection=None,
                style_cell=_h.style_cell,
            )
        )

    if _stack:
        _stack.insert(0, _mo.md("## Table details"))
        _stack.insert(
            1,
            _mo.hstack(
                [
                    dtl_schema_show_dlt_columns,
                    dtl_schema_show_type_hints,
                    dlt_schema_show_other_hints,
                    dtl_schema_show_custom_hints,
                ],
                justify="start",
            ),
        )

    _mo.vstack(_stack)
    return


@app.cell(hide_code=True)
def page_schema_section_raw_schema(dlt_pipeline_name, dlt_page_tabs):
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, helpers as _h

    _mo.stop(
        not dlt_pipeline_name
        or _s.app_tab_schema not in dlt_page_tabs.value
        or not _h.get_pipeline(dlt_pipeline_name).default_schema_name
    )

    _mo.vstack(
        [
            _mo.md(_s.schema_raw_title),
            _mo.accordion(
                {
                    "Show raw schema as yaml": _mo.ui.code_editor(
                        _h.get_pipeline(dlt_pipeline_name).default_schema.to_pretty_yaml(),
                        language="yaml",
                    )
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def page_brows_data(dlt_pipeline_name, dlt_page_tabs):
    """
    Show data of the currently selected pipeline
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, helpers as _h

    _mo.stop(not dlt_pipeline_name or _s.app_tab_browse_data not in dlt_page_tabs.value)

    _mo.md(_s.browse_data_title)

    return


@app.cell(hide_code=True)
def page_state(dlt_pipeline_name, dlt_page_tabs):
    """
    Show state of the currently selected pipeline
    """
    from dlt.common.json import json as _json
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, helpers as _h

    _mo.stop(not dlt_pipeline_name or _s.app_tab_state not in dlt_page_tabs.value)

    _mo.vstack(
        [
            _mo.md(_s.state_raw_title),
            _mo.ui.code_editor(
                _json.dumps(_h.get_pipeline(dlt_pipeline_name).state, pretty=True),
                language="json",
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def ibis_browser_page(dlt_pipeline_name, dlt_page_tabs):
    """
    Connects to ibis backend and makes it available in the datasources panel
    """
    import marimo as _mo
    from dlt.helpers.studio import strings as _s, helpers as _h, ui_elements as _ui

    _mo.stop(not dlt_pipeline_name or _s.app_tab_ibis_browser not in dlt_page_tabs.value)

    try:
        with _mo.status.spinner(title="Connecting Ibis Backend..."):
            con = _h.get_pipeline(dlt_pipeline_name).dataset().ibis()
        connect_result = _mo.callout(
            _mo.vstack([_mo.md(_s.ibis_backend_connected)]), kind="success"
        )
    except Exception:
        connect_result = _ui.build_error_callout(_s.ibis_connect_error)

    _mo.vstack([_mo.md(_s.ibis_backend_title), connect_result])
    return


#
# Utility Cells
#


@app.cell(hide_code=True)
def app_discover_pipelines():
    """
    Discovers local pipelines and returns a multiselect widget to select one of the pipelines
    """

    import marimo as _mo
    from dlt.helpers.studio import strings as _s, helpers as _h

    query_params = _mo.query_params()
    dlt_pipelines_dir, pipelines = _h.get_local_pipelines()
    dlt_pipeline_count = len(pipelines)
    dlt_pipeline_select = _mo.ui.multiselect(
        options=pipelines,
        value=[query_params.get("pipeline")] if query_params.get("pipeline") else None,
        max_selections=1,
        label=_s.pipeline_select_label,
        on_change=lambda value: query_params.set("pipeline", str(value[0]) if value else None),
    )
    return dlt_pipelines_dir, dlt_pipeline_select, dlt_pipeline_count, query_params


@app.cell(hide_code=True)
def prepare_query_vars(query_params):
    """
    Prepare query params as globals for the following cells
    """
    import marimo as _mo

    dlt_pipeline_name = _mo.query_params().get("pipeline") or None
    dlt_current_page = query_params.get("page") or None
    return (dlt_pipeline_name, dlt_current_page)


if __name__ == "__main__":
    app.run()
