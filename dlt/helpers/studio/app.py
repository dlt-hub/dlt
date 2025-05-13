# mypy: disable-error-code="no-untyped-def"
# flake8: noqa: F841

import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium", css_file="style.css")


@app.cell(hide_code=True)
def welcome_page(
    dlt_pipelines_dir: str,
    pipeline_select: marimo.ui.multiselect,
    pipelines_count: int,
    strings,
):
    """
    Displays the welcome page with the pipeline select widget, will only display pipeline title if a pipeline is selected
    """
    import marimo as _mo

    _selected_pipeline_name = pipeline_select.value[0] if pipeline_select.value else None

    if not _selected_pipeline_name:
        _stack = [
            _mo.hstack(
                [
                    _mo.image(
                        "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
                    ),
                    pipeline_select,
                ],
            ),
            _mo.md(strings.app_title).center(),
            _mo.md(strings.app_intro).center(),
            _mo.callout(
                _mo.vstack([_mo.md(strings.app_quick_start_title), pipeline_select]), kind="info"
            ),
            _mo.md(strings.app_basics_text.format(pipelines_count, dlt_pipelines_dir)),
        ]
    else:
        _stack = [
            _mo.hstack(
                [
                    _mo.image(
                        "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
                    ).style(padding_bottom="1em"),
                    _mo.center(
                        _mo.md(f"## {strings.app_title_pipeline.format(_selected_pipeline_name)}")
                    ),
                    pipeline_select,
                ],
            ),
        ]
    _mo.vstack(_stack)
    return


@app.cell(hide_code=True)
def pipeline_sync_and_tabs(dlt_pipeline_name):
    """
    Syncs the pipeline and renders the result of the sync
    """

    import marimo as _mo

    _mo.stop(not dlt_pipeline_name)

    # build tabs
    tabs = _mo.ui.tabs(
        {
            f"{_mo.icon('lucide:home')} Overview": "",
            f"{_mo.icon('lucide:table-properties')} Schema": "",
            f"{_mo.icon('lucide:database')} Browse Data": "",
            f"{_mo.icon('lucide:file-chart-column')} State": "",
            f"{_mo.icon('lucide:view')} Ibis-Browser": "",
        }
    )

    _mo.center(tabs)
    return tabs


#
# Overview page
#


@app.cell(hide_code=True)
def overview_page(
    dlt_destination_credentials,
    dlt_pipeline_name,
    dlt_pipelines_dir,
    helpers,
    strings,
    tabs,
    ui_elements,
):
    """
    Overview page of currently selected pipeline
    """
    import marimo as _mo

    dlt_destination_credentials = "Could not resolve credentials"
    _mo.stop(not dlt_pipeline_name)
    _mo.stop("Overview" not in tabs.value)

    # sync pipeline
    with _mo.status.spinner(title="Syncing pipeline state from destination..."):
        try:
            helpers.get_pipeline(dlt_pipeline_name).sync_destination()
            dlt_destination_credentials = (
                helpers.get_pipeline(dlt_pipeline_name)
                .dataset()
                .destination_client.config.credentials
            )
            sync_result = _mo.callout(
                _mo.vstack(
                    [
                        _mo.md(
                            strings.pipeline_sync_success_text.format(
                                str(dlt_destination_credentials)
                            )
                        )
                    ]
                ),
                kind="success",
            )

        except Exception:
            sync_result = ui_elements.build_error_callout(strings.pipeline_sync_error_text)

    dlt_pipeline = helpers.get_pipeline(dlt_pipeline_name)
    _mo.vstack(
        [
            _mo.md(strings.pipeline_sync_status),
            sync_result,
            _mo.md(strings.pipeline_details),
            _mo.ui.table(
                [
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
                        "Value": (
                            str(dlt_destination_credentials)
                            if dlt_destination_credentials
                            else "No destination set"
                        ),
                    },
                    {"Key": "Dataset name", "Value": dlt_pipeline.dataset_name},
                    {"Key": "Schema name", "Value": dlt_pipeline.default_schema_name},
                    {
                        "Key": "Local pipeline directory",
                        "Value": dlt_pipelines_dir + "/" + dlt_pipeline_name,
                    },
                ],
                selection=None,
                style_cell=helpers.style_cell,
            ),
        ]
    )
    return


#
# Schema page
#
@app.cell(hide_code=True)
def schema_page_control_panel():
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


@app.cell(hide_code=True)
def schema_page_table_list_section(
    dlt_pipeline_name,
    helpers,
    tabs,
    strings,
    dlt_schema_show_dlt_tables,
    dlt_schema_show_child_tables,
):
    """
    Show schema of the currently selected pipeline
    """
    import marimo as _mo

    _mo.stop(not dlt_pipeline_name or "Schema" not in tabs.value)

    if not helpers.get_pipeline(dlt_pipeline_name).default_schema_name:
        dlt_schem_table_list = _mo.callout(
            _mo.md("No Schema available. Does your pipeline have a completed load?"),
            kind="warn",
        )
    else:
        dlt_schem_table_list = _mo.ui.table(
            helpers.create_table_list(
                dlt_pipeline_name,
                show_internals=dlt_schema_show_dlt_tables.value,
                show_child_tables=dlt_schema_show_child_tables.value,
            ),
            style_cell=helpers.style_cell,
        )

    _mo.vstack(
        [
            _mo.md(
                strings.schema_table_overview.format(
                    helpers.get_pipeline(dlt_pipeline_name).default_schema_name
                    or "<no schema found>"
                )
            ),
            _mo.hstack([dlt_schema_show_dlt_tables, dlt_schema_show_child_tables], justify="start"),
            dlt_schem_table_list,
        ]
    )
    return


@app.cell(hide_code=True)
def schema_page_table_schemas_section(
    dlt_pipeline_name,
    helpers,
    tabs,
    dlt_schem_table_list,
    dtl_schema_show_dlt_columns,
    dtl_schema_show_type_hints,
    dtl_schema_show_custom_hints,
    dlt_schema_show_other_hints,
):
    """
    Show schema of the currently selected table
    """
    import marimo as _mo

    _mo.stop(not dlt_pipeline_name or "Schema" not in tabs.value)
    _mo.stop(
        not helpers.get_pipeline(dlt_pipeline_name).default_schema_name,
    )

    _stack = []

    for table in dlt_schem_table_list.value:
        _stack.append(_mo.md(f"### `{table['Name']}`"))
        _stack.append(
            _mo.ui.table(
                helpers.create_column_list(
                    dlt_pipeline_name,
                    table["Name"],
                    show_internals=dtl_schema_show_dlt_columns.value,
                    show_type_hints=dtl_schema_show_type_hints.value,
                    show_other_hints=dlt_schema_show_other_hints.value,
                    show_custom_hints=dtl_schema_show_custom_hints.value,
                ),
                selection=None,
                style_cell=helpers.style_cell,
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
def schema_page_raw_schema_section(dlt_pipeline_name, helpers, tabs, strings):
    import marimo as _mo

    _mo.stop(not dlt_pipeline_name or "Schema" not in tabs.value)
    _mo.stop(
        not helpers.get_pipeline(dlt_pipeline_name).default_schema_name,
    )

    _mo.vstack(
        [
            _mo.md(strings.schema_raw_title),
            _mo.accordion(
                {
                    "Show raw schema as yaml": _mo.ui.code_editor(
                        helpers.get_pipeline(dlt_pipeline_name).default_schema.to_pretty_yaml(),
                        language="yaml",
                    )
                }
            ),
        ]
    )


#
# Browse data page
#


@app.cell(hide_code=True)
def browse_data_page(dlt_pipeline_name, tabs, strings):
    """
    Show data of the currently selected pipeline
    """
    import marimo as _mo

    _mo.stop(not dlt_pipeline_name)
    _mo.stop("Data" not in tabs.value)

    _mo.md(strings.browse_data_title)

    return


#
# State page
#


@app.cell(hide_code=True)
def state_page(dlt_pipeline_name, helpers, json, tabs, strings):
    """
    Show state of the currently selected pipeline
    """
    import marimo as _mo

    _mo.stop(not dlt_pipeline_name)
    _mo.stop("State" not in tabs.value)

    _mo.vstack(
        [
            _mo.md(strings.state_raw_title),
            _mo.ui.code_editor(
                json.dumps(helpers.get_pipeline(dlt_pipeline_name).state, pretty=True),
                language="json",
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def ibis_browser_page(dlt_pipeline_name, helpers, strings, tabs, ui_elements):
    """
    Connects to ibis backend and makes it available in the datasources panel
    """
    import marimo as _mo

    _mo.stop(not dlt_pipeline_name)
    _mo.stop("Ibis-Browser" not in tabs.value)

    try:
        with _mo.status.spinner(title="Connecting Ibis Backend..."):
            con = helpers.get_pipeline(dlt_pipeline_name).dataset().ibis()
        connect_result = _mo.callout(
            _mo.vstack([_mo.md(strings.ibis_backend_connected)]), kind="success"
        )
    except Exception:
        connect_result = ui_elements.build_error_callout(strings.ibis_connect_error)

    _mo.vstack([_mo.md(strings.ibis_backend_title), connect_result])


#
# Utility cells
#


@app.cell(hide_code=True)
def app_setup():
    """Imports the necessary modules and returns them into global scope"""
    from dlt.common.json import json

    from dlt.helpers.studio import helpers, ui_elements

    return (helpers, json, ui_elements)


@app.cell(hide_code=True)
def discover_pipelines(helpers, strings):
    """
    Discovers local pipelines and returns a multiselect widget to select one of the pipelines
    """

    import marimo as _mo

    query_params = _mo.query_params()
    dlt_pipelines_dir, pipelines = helpers.get_local_pipelines()
    pipelines_count = len(pipelines)
    pipeline_select = _mo.ui.multiselect(
        options=pipelines,
        value=[query_params.get("pipeline")] if query_params.get("pipeline") else None,
        max_selections=1,
        label=strings.pipeline_select_label,
        on_change=lambda value: query_params.set("pipeline", value[0] if value else None),
    )
    return dlt_pipelines_dir, pipeline_select, pipelines_count, query_params


@app.cell(hide_code=True)
def prepare_query_vars(query_params):
    """
    Prepare query params as globals for the following cells
    """
    import marimo as _mo

    dlt_pipeline_name = _mo.query_params().get("pipeline") or None
    page = query_params.get("page") or None
    return (dlt_pipeline_name, page)


@app.cell(hide_code=True)
def app_strings(mo):
    """
    Defines the strings used in the app, can be moved to a separate file if the app grows
    """

    class Strings:
        #
        # App general and welcome page
        #
        app_title = """
        # Welcome to dltHub Studio...
        """
        app_intro = """
        <p align="center">...the hackable data platform for `dlt` developers. Learn how to modify this app and create your personal platform at <a href="https://dlthub.com/docs/studio/overview">dlthub.com</a>.</p>
        """
        app_quick_start_title = """
        ## Quick start: Select a pipeline
        Selecting a pipeline from the list will open the pipeline page.
        """
        app_basics_text = """
        ## dltHub Studio basics

        `dlt studio` has found `{}` pipelines in local directory `{}`. If you select one of the pipelines to inspect. You will be able to:

        * See the current pipeline schema
        * See the pipeline state
        * Browse information about past loads and traces
        * Browse the data in the pipeline's dataset (requires credentials in scope of `dltHub studio`)

        If you want to sync the current schema and state from the destination dataset or inspect data in the destination dataset, the credentials for your destination need to be in scope of `dltHub studio`. Either provide them as environment variables or start dltHub studio from the directory with your `.dlt` folder where the credentials are stored.

        If dlthub Studio can't connect to the destination, you will receive a warning and you can browse the locally stored information about the pipeline.

        ## dltHub Studio CLI commands

        * `dlt studio` - Start the studio, this will take you to this place
        * `dlt studio -p <dlt_pipeline_name>` - directly jump to the pipeline page on launch
        * `dlt studio eject` - Will eject the code for dltHub Studio and allow you to create your own hackable version of the app :)

        ## Learn more

        * [marimo docs](https://docs.marimo.io/) - Learn all about marimo, the amazing framework that powers dltHub Studio
        * [dltHub Studio docs](https://dlthub.com/docs/studio/overview) - Learn all about dltHub Studio, the hackable data platform for `dlt` developers

        <small>
        2025 [dltHub](https://dlthub.com)
        </small>

        """
        app_description = (
            "The hackable data platform for `dlt` developers. Learn how to modify this app and"
            " create your personal platform at"
            " [dlthub.com](https://dlthub.com/docs/studio/overview)."
        )
        app_title_pipeline = "Pipeline `{}`"

        # studio
        pipeline_select_label = f"Pipeline:"
        no_pipeline_selected = "No pipeline selected"

        #
        # Pipeline overview page
        #
        pipeline_sync_status = "## Pipeline sync status"
        pipeline_sync_error_text = (
            "Error syncing pipeline from destination. Make sure the credentials are in scope of"
            " `dltHub studio`. Switching to local mode."
        )
        pipeline_details = "## Pipeline details"
        pipeline_sync_status = """
        ## Pipeline sync status
        <small>Dlt studio will try to sync the pipeline state and schema from the destination. If the sync fails, studio use local mode and inspect the locally stored information about the pipeline.</small>
        """
        pipeline_sync_success_text = "Pipeline state synced successfully from `{}`."

        #
        # Schema page
        #
        schema_raw_title = "## Raw schema"
        schema_table_overview = """
            ## Table overview for default schema `{}`
            <small>The following list shows all the tables found in the dlt schema of the selected pipeline. Please note that in some cases the dlt schema can differ from the actual schema materialized in the destination.
            <strong>Select one or more tables to see their full schema.</strong></small>
            """

        #
        # Browse data page
        #
        browse_data_title = "## Browse data"

        #
        # State page
        #
        state_raw_title = """
        ## Raw state
        <small>A raw view of the currently stored pipeline state.</small>
        """
        #
        # Ibis backend page
        #
        ibis_backend_title = """
        ## Connect to Ibis Backend
        <small>This page will automatically connect to the ibis backend of the selected pipeline. This will make the destination available in the datasources panel. Please note that this is a raw view on all tables and data in the destination and might differ from the tables you see in the dlt schema.</small>
        """
        ibis_backend_connected = (
            "Ibis Backend connected successfully. If you are in marimo edit mode, you can now see"
            " the connected database in the datasources panel."
        )
        ibis_connect_error = (
            "Error connecting to Ibis Backend. Has your pipeline been run and are your credentials"
            " in scope of `dltHub studio`?"
        )

    strings = Strings()

    return (strings,)


if __name__ == "__main__":
    app.run()
