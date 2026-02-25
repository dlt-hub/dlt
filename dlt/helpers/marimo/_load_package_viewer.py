import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    from dlt.helpers.marimo.utils import _load_file

    pipeline_path = None


@app.cell
def pipeline_browser():
    mo.stop(pipeline_path is None)
    pipeline_details = mo.ui.file_browser(
        pipeline_path,
        selection_mode="file",
        multiple=False,
        restrict_navigation=True,
        label="Select file to load",
    )
    pipeline_details
    return (pipeline_details,)


@app.cell
def file_viewer(pipeline_details):
    if pipeline_details.value:
        obj = _load_file(pipeline_details.value[0].path)
    else:
        obj = None

    obj
    return


if __name__ == "__main__":
    app.run()
