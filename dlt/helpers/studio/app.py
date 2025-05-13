# mypy: disable-error-code="no-untyped-def"
# flake8: noqa: F841

import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium", css_file="style.css")


@app.cell(hide_code=True)
def welcome_page(
    dlt_pipelines_dir,
    mo,
    pipeline_select,
    pipelines_count,
    strings,
):
    """
    Displays the welcome page with the pipeline select widget, will only display pipeline title if a pipeline is selected
    """

    _selected_pipeline_name = pipeline_select.value[0] if pipeline_select.value else None

    if not _selected_pipeline_name:
        stack = [
            mo.image(
                "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
            ).style(padding_bottom="2em"),
            mo.md(strings.app_title).center(),
            mo.md(strings.app_intro).center(),
            mo.callout(
                mo.vstack([mo.md(strings.app_quick_start_title), pipeline_select]), kind="info"
            ),
            mo.md(strings.app_basics_text.format(pipelines_count, dlt_pipelines_dir)),
        ]
    else:
        stack = [
            mo.hstack(
                [
                    mo.image(
                        "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
                    ).style(padding_bottom="1em"),
                    mo.center(
                        mo.md(f"### {strings.app_title_pipeline.format(_selected_pipeline_name)}")
                    ),
                    pipeline_select,
                ]
            ),
        ]
    mo.vstack(stack)
    return


@app.cell(hide_code=True)
def pipeline_sync_and_tabs(dlt_pipeline_name, helpers, mo, strings):
    """
    Syncs the pipeline and renders the result of the sync
    """

    mo.stop(not dlt_pipeline_name)

    # build tabs
    tabs = mo.ui.tabs(
        {
            f"{mo.icon('lucide:home')} Overview": "",
            f"{mo.icon('lucide:table-properties')} Schema": "",
            f"{mo.icon('lucide:database')} Browse Data": "",
            f"{mo.icon('lucide:file-chart-column')} State": "",
            f"{mo.icon('lucide:view')} Ibis-Browser": "",
        }
    )

    mo.center(tabs)
    return tabs


@app.cell(hide_code=True)
def overview_page(
    dlt_destination_credentials,
    dlt_pipeline_name,
    dlt_pipelines_dir,
    helpers,
    mo,
    strings,
    tabs,
):
    """
    Overview page of currently selected pipeline
    """

    dlt_destination_credentials = None
    mo.stop(not dlt_pipeline_name)
    mo.stop("Overview" not in tabs.value)

    # sync pipeline
    with mo.status.spinner(title="Syncing pipeline state from destination..."):
        try:
            helpers.get_pipeline(dlt_pipeline_name).sync_destination()
            dlt_destination_credentials = (
                helpers.get_pipeline(dlt_pipeline_name)
                .dataset()
                .destination_client.config.credentials
            )
            sync_result = mo.callout(
                mo.vstack(
                    [
                        mo.md(
                            strings.pipeline_sync_success_text.format(
                                str(dlt_destination_credentials)
                            )
                        )
                    ]
                ),
                kind="success",
            )

        except Exception as e:
            sync_result = mo.callout(
                mo.vstack(
                    [
                        mo.md(strings.pipeline_sync_error_text),
                        mo.accordion({"Show stacktrace": mo.ui.code_editor(e, language="shell")}),
                    ]
                ),
                kind="warn",
            )

    dlt_pipeline = helpers.get_pipeline(dlt_pipeline_name)
    mo.vstack(
        [
            mo.md(strings.pipeline_sync_status),
            sync_result,
            mo.md(strings.pipeline_details),
            mo.ui.table(
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
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def schema_page(dlt_pipeline_name, helpers, mo, tabs, strings):
    """
    Show schema of the currently selected pipeline
    """

    mo.stop(not dlt_pipeline_name)
    mo.stop("Schema" not in tabs.value)
    mo.stop(
        not helpers.get_pipeline(dlt_pipeline_name).default_schema_name,
        mo.callout(
            mo.md("No Schema available. Does your pipeline have a completed load?"), kind="warn"
        ),
    )

    mo.vstack(
        [
            mo.md(
                strings.schema_table_overview.format(
                    helpers.get_pipeline(dlt_pipeline_name).default_schema_name
                )
            ),
            mo.ui.table(helpers.create_table_list(dlt_pipeline_name)),
            mo.md(strings.schema_raw_title),
            mo.accordion(
                {
                    "Show": mo.ui.code_editor(
                        helpers.get_pipeline(dlt_pipeline_name).default_schema.to_pretty_yaml(),
                        language="yaml",
                    )
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def state_page(dlt_pipeline_name, helpers, json, mo, tabs, strings):
    """
    Show state of the currently selected pipeline
    """

    mo.stop(not dlt_pipeline_name)
    mo.stop("State" not in tabs.value)

    mo.vstack(
        [
            mo.md(strings.state_raw_title),
            mo.ui.code_editor(
                json.dumps(helpers.get_pipeline(dlt_pipeline_name).state, pretty=True),
                language="json",
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def ibis_browser_page(dlt_pipeline_name, helpers, mo, strings, tabs):
    """
    Connects to ibis backend and makes it available in the datasources panel
    """

    mo.stop(not dlt_pipeline_name)
    mo.stop("Ibis-Browser" not in tabs.value)

    with mo.status.spinner(title="Connecting Ibis Backend..."):
        con = helpers.get_pipeline(dlt_pipeline_name).dataset().ibis()

    mo.callout(
        mo.vstack([mo.md(strings.ibis_backend_connected)]),
        kind="success",
    )
    return


#
# Utility cells
#


@app.cell(hide_code=True)
def app_setup():
    """Imports the necessary modules and returns them into global scope"""
    import marimo as mo
    from dlt.helpers.studio import helpers
    from dlt.common.json import json

    return (helpers, json, mo)


@app.cell(hide_code=True)
def discover_pipelines(helpers, mo, strings):
    """
    Discovers local pipelines and returns a multiselect widget to select one of the pipelines
    """

    query_params = mo.query_params()
    dlt_pipelines_dir, pipelines = helpers.get_local_pipelines()
    pipelines_count = len(pipelines)
    pipeline_select = mo.ui.multiselect(
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

    dlt_pipeline_name = query_params.get("pipeline") or None
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
        ### Quick start: Select a pipeline
        Selecting a pipeline from the list will open the pipeline page.
        """
        app_basics_text = """
        ### dltHub Studio basics

        `dlt studio` has found `{}` pipelines in local directory `{}`. If you select one of the pipelines to inspect. You will be able to:

        * See the current pipeline schema
        * See the pipeline state
        * Browse information about past loads and traces
        * Browse the data in the pipeline's dataset (requires credentials in scope of `dltHub studio`)

        If you want to sync the current schema and state from the destination dataset or inspect data in the destination dataset, the credentials for your destination need to be in scope of `dltHub studio`. Either provide them as environment variables or start dltHub studio from the directory with your `.dlt` folder where the credentials are stored.

        If dlthub Studio can't connect to the destination, you will receive a warning and you can browse the locally stored information about the pipeline.

        ### dltHub Studio CLI commands

        * `dlt studio` - Start the studio, this will take you to this place
        * `dlt studio -p <dlt_pipeline_name>` - directly jump to the pipeline page on launch
        * `dlt studio eject` - Will eject the code for dltHub Studio and allow you to create your own hackable version of the app :)

        ### Learn more

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
        pipeline_sync_status = "### Pipeline sync status"
        pipeline_sync_error_text = (
            "Error syncing pipeline from destination. Make sure the credentials are in scope of"
            " `dltHub studio`. Switching to local mode."
        )
        pipeline_details = "### Pipeline details"
        pipeline_sync_status = """
        ### Pipeline sync status
        <small>Dlt studio will try to sync the pipeline state and schema from the destination. If the sync fails, studio use local mode and inspect the locally stored information about the pipeline.</small>
        """
        pipeline_sync_success_text = "Pipeline state synced successfully from `{}`."

        #
        # Schema page
        #
        schema_raw_title = "### Raw schema"
        schema_table_overview = """
            ### Schema table overview for default schema `{}`
            <small>The following list shows all the tables found in the dlt schema of the selected pipeline. Please note that in some cases the dlt schema can differ from the actual schema materialized in the destination.
            Select one or more tables to see their full schema.</small>
            """

        #
        # State page
        #
        state_raw_title = "### Raw state"
        #
        # Ibis backend page
        #
        ibis_backend_connected = (
            "Ibis Backend connected successfully. If you are in marimo edit mode, you can now see"
            " the connected database in the datasources panel."
        )

    strings = Strings()

    return (strings,)


if __name__ == "__main__":
    app.run()
