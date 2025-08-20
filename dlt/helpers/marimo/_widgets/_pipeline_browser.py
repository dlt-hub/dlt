import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


with app.setup:
    import marimo as mo
    import pathlib


@app.cell
def _():
    base_path = "~/.dlt/pipelines"
    return (base_path,)


@app.cell
def _(base_path):
    pipelines = [p.name for p in pathlib.Path(base_path).expanduser().iterdir()]
    select_pipeline = mo.ui.dropdown(pipelines, value="pokemon")
    select_pipeline
    return (select_pipeline,)


@app.cell
def _(base_path, select_pipeline):
    pipeline_dir = pathlib.Path(base_path).expanduser() / f"{select_pipeline.value}"
    pipeline_details = mo.ui.file_browser(pipeline_dir)
    pipeline_details
    return


if __name__ == "__main__":
    app.run()
