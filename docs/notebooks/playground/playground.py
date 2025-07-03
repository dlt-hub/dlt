import marimo

__generated_with = "0.13.15"
app = marimo.App()


with app.setup:
    import sys
    import marimo as mo


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md(r"""# `dlt` playground"""),
            mo.md(
                "This example marimo notebook demonstrates the basic usage of `dlt` by loading"
                " python data structures. This notebook is running in WASM mode fully in your"
                " browser. Some features available in 'real' python are not available in WASM."
            ),
            mo.md(
                "You can run this example locally in 'real' python with `uv run marimo edit"
                " docs/notebooks/playground/playground.py` from the dltHub repo root."
            ),
        ]
    )
    return


@app.cell(hide_code=True)
async def _():
    # NOTE: this installs the dependencies for the notebook if run on pyodide
    if sys.platform == "emscripten":
        import micropip

        await micropip.install("duckdb")
        await micropip.install("sqlite3")
        await micropip.install("pandas")
        await micropip.install("ibis-framework[duckdb]")
        await micropip.install("dlt==1.12.4a0")


@app.cell
def _():
    import os

    os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "False"
    os.environ["WORKERS"] = "1"
    import dlt

    return dlt


@app.cell
def _(dlt):
    @dlt.resource(table_name="items")
    def foo():
        for i in range(50):
            yield {"id": i, "name": f"This is item {i}"}

    pipeline = dlt.pipeline(
        pipeline_name="python_data_example",
        destination="duckdb",
    )

    load_info = pipeline.run(foo)
    return (pipeline,)


@app.cell
def _(pipeline):
    pipeline.dataset(dataset_type="default").items.df()
    return


@app.cell
def _(pipeline):
    con = pipeline.dataset().ibis()
    return


if __name__ == "__main__":
    app.run()
