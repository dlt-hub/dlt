import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium", css_file="style.css")


@app.cell(hide_code=True)
def app_setup():
    """Imports the necessary modules and returns them into global scope"""
    import marimo as mo
    import dlt
    import helpers

    return dlt, helpers, mo


@app.cell(hide_code=True)
def discover_pipelines(helpers, mo, strings):
    """Discovers local pipelines and returns a multiselect widget to select one of the pipelines"""
    pipelines_dir, pipelines = helpers.get_local_pipelines()
    pipelines_count = len(pipelines)
    pipeline_select = mo.ui.multiselect(
        options=pipelines, max_selections=1, label=strings.pipeline_select_label
    )
    return pipeline_select, pipelines_count, pipelines_dir


@app.cell(hide_code=True)
def welcome_page(mo, pipeline_select, pipelines_count, pipelines_dir, strings):
    """Displays the welcome page with the pipeline select widget, will only display pipeline title if a pipeline is selected"""

    stack = []
    if not pipeline_select.value:
        stack = [
            mo.image(
                "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
            ).style(padding_bottom="2em"),
            mo.md(strings.app_title).center(),
            mo.md(strings.app_intro).center(),
            mo.callout(mo.vstack([mo.md(strings.quick_start_title), pipeline_select]), kind="info"),
            mo.md(strings.basics_text.format(pipelines_count, pipelines_dir)),
        ]
    else:
        stack = [
            mo.hstack(
                [
                    mo.image(
                        "https://dlthub.com/docs/img/dlthub-logo.png", width=100, alt="dltHub logo"
                    ).style(padding_bottom="1em"),
                    pipeline_select,
                ]
            ),
            mo.center(mo.md(f"# {strings.app_title_pipeline.format(pipeline_select.value[0])}")),
        ]
    mo.vstack(stack)
    return


@app.cell(hide_code=True)
def tabs(mo, pipeline_select):
    mo.stop(len(pipeline_select.value) == 0)

    tabs = mo.ui.tabs({"Overview": "", "Schema": "", "State": "", "Data": "", "Ibis-Browser": ""})
    mo.center(tabs)
    return (tabs,)


@app.cell(hide_code=True)
def sync_pipeline(dlt, mo, pipeline_select, strings, tabs, helpers):
    """Syncs the pipeline and returns a button to inspect the pipeline"""
    mo.stop(len(pipeline_select.value) == 0)
    mo.stop(tabs.value != "Overview")

    result = ""
    could_sync = False

    with mo.status.spinner(title="Syncing pipeline state from destination...") as _spinner:
        try:
            helpers.get_pipeline(dlt, pipeline_select).sync_destination()
            result = mo.callout(mo.md(strings.sync_success_text), kind="success")
            could_sync = True
        except Exception as e:
            result = mo.callout(
                mo.vstack(
                    [
                        mo.md(strings.sync_error_text),
                        mo.accordion(
                            {"Show stacktrace": mo.ui.code_editor(str(e), language="shell")}
                        ),
                    ]
                ),
                kind="warn",
            )

    result
    return


@app.cell(hide_code=True)
def state_page(mo, tabs, pipeline_select, dlt, helpers):
    mo.stop(tabs.value != "State")
    helpers.get_pipeline(dlt, pipeline_select).state


@app.cell(hide_code=True)
def schema_page(mo, tabs, pipeline_select, dlt, helpers):
    mo.stop(tabs.value != "Schema")
    helpers.get_pipeline(dlt, pipeline_select).default_schema.to_dict()


@app.cell(hide_code=True)
def ibis_browser_page(mo, tabs, pipeline_select, dlt, helpers):
    mo.stop(tabs.value != "Ibis-Browser")
    mo.callout(mo.md("Ibis Browser not implemented yet"), kind="info")

    with mo.status.spinner(title="Connecting Ibis Backend...") as _spinner:
        con = helpers.get_pipeline(dlt, pipeline_select).dataset().ibis()

    mo.callout(
        mo.vstack(
            [
                mo.md(
                    "Ibis Backend connected successfully. If you are in marimo edit mode, you can"
                    " now see the connected database in the datasources panel."
                )
            ]
        ),
        kind="success",
    )


@app.cell(hide_code=True)
def app_strings():
    """Defines the strings used in the app, can be moved to a separate file if the app grows"""

    class Strings:
        # general
        app_title = """
        # Welcome to dltHub Studio...
        """

        app_intro = """
        <p align="center">...the hackable data platform for `dlt` developers. Learn how to modify this app and create your personal platform at <a href="https://dlthub.com/docs/studio/overview">dlthub.com</a>.</p>
        """

        quick_start_title = """
        ### Quick start: Select a pipeline
        Selecting a pipeline from the list will open the pipeline page.
        """

        basics_text = """

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
        * `dlt studio -p <pipeline_name>` - directly jump to the pipeline page on launch
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

        app_title_pipeline = 'Pipeline "{}"'

        # studio
        pipeline_select_label = "Select a pipeline:"

        no_pipeline_selected = "No pipeline selected"

        sync_success_text = "Pipeline state synced successfully"

        sync_error_text = (
            "Error syncing pipeline from destination. Make sure the credentials are in scope of"
            " `dltHub studio`. Switching to local mode."
        )

    strings = Strings()

    return (strings,)


if __name__ == "__main__":
    app.run()
