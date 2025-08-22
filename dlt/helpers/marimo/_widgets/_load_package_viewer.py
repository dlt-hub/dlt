import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    from dlt.common.storages import FileStorage
    from dlt.common.pipeline import get_dlt_pipelines_dir
    from dlt.helpers.marimo.utils import _load_file


@app.cell
def _():
    storage = FileStorage(get_dlt_pipelines_dir())

    try:
        pipelines = storage.list_folder_dirs(".", to_root=False)
    except Exception:
        pipelines = []

    pipelines_options = {p: storage.storage_path + "/" + p for p in sorted(pipelines)}
    return (pipelines_options,)


@app.cell
def _(pipelines_options):
    first_key = next(iter(pipelines_options.keys()))
    select_pipeline = mo.ui.dropdown(
        pipelines_options,
        value=first_key,
        label="Pipeline",
    )
    select_pipeline
    return (select_pipeline,)


@app.cell
def _(select_pipeline):
    # TODO set `filetypes` args to match implementations
    # in `file_loader()`
    pipeline_details = mo.ui.file_browser(
        select_pipeline.value,
        selection_mode="file",
        multiple=False,
        restrict_navigation=True,
        label="Select file to load",
    )
    pipeline_details
    return (pipeline_details,)


@app.cell
def _(pipeline_details):
    if pipeline_details.value:
        obj = _load_file(pipeline_details.value[0].path)
    else:
        obj = None

    obj
    return


if __name__ == "__main__":
    app.run()
