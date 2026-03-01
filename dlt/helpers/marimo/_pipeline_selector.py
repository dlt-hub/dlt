import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium", app_title="pipeline_selector")

with app.setup:
    import marimo as mo
    from dlt._workspace.cli.utils import list_local_pipelines


@app.cell
def pipeline_locations():
    _pipelines_dir, _pipelines = list_local_pipelines()
    pipelines_locations = {p["name"]: _pipelines_dir + "/" + p["name"] for p in _pipelines}
    return (pipelines_locations,)


@app.cell
def pipeline_selector(pipelines_locations):
    try:
        _first_pipeline_name = next(iter(pipelines_locations.keys()))
    except StopIteration:
        _first_pipeline_name = None

    pipeline_selector = mo.ui.dropdown(
        pipelines_locations.keys(),
        value=_first_pipeline_name,
        label="Pipeline",
    )
    pipeline_selector
    return (pipeline_selector,)


@app.cell
def outputs(pipeline_selector, pipelines_locations):
    pipeline_name = pipeline_selector.value
    pipeline_path = pipelines_locations.get(pipeline_name, None)  # noqa: F841
    return


if __name__ == "__main__":
    app.run()
