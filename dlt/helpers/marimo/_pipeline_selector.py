import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium", app_title="pipeline_selector")

with app.setup:
    import marimo as mo
    from dlt.common.storages import FileStorage
    from dlt.common.pipeline import get_dlt_pipelines_dir


@app.cell
def _():
    _storage = FileStorage(get_dlt_pipelines_dir())

    try:
        _pipelines = _storage.list_folder_dirs(".", to_root=False)
    except Exception:
        _pipelines = []

    pipelines_locations = {p: _storage.storage_path + "/" + p for p in sorted(_pipelines)}
    return (pipelines_locations,)


@app.cell
def _(pipelines_locations):
    _first_pipeline_name = next(iter(pipelines_locations.keys()))
    pipeline_selector = mo.ui.dropdown(
        pipelines_locations.keys(),
        value=_first_pipeline_name,
        label="Pipeline",
    )
    pipeline_selector
    return (pipeline_selector,)


@app.cell
def _(pipeline_selector, pipelines_locations):
    pipeline_name = pipeline_selector.value
    pipeline_path = pipelines_locations[pipeline_name]  # noqa: F841
    return


if __name__ == "__main__":
    app.run()
