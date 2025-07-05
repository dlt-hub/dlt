import marimo

__generated_with = "0.13.15"
app = marimo.App()


@app.cell(hide_code=True)
async def initialize():
    import sys
    import marimo as mo

    # NOTE: the three lines below can be removed after new dlt 1.13 is released
    import os

    os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "False"
    os.environ["WORKERS"] = "1"

    # NOTE: this installs the dependencies for the notebook if run on pyodide
    if sys.platform == "emscripten":
        import micropip

        await micropip.install("duckdb")
        await micropip.install("sqlite3")
        await micropip.install("pandas")
        await micropip.install("ibis-framework[duckdb]")
        await micropip.install("dlt==1.12.4a0")

    return sys, mo


@app.cell
def run(dlt):
    import dlt

    @dlt.resource(table_name="items")
    def foo():
        for i in range(50):
            yield {"id": i, "name": f"This is item {i}"}

    pipeline = dlt.pipeline(
        pipeline_name="python_data_example",
        destination="duckdb",
        dev_mode=True,
    )

    load_info = pipeline.run(foo)
    return (pipeline,)


@app.cell
def view(pipeline):
    # NOTE: This line displays the data of the items table in a marimo table
    pipeline.dataset().items.df()
    return


@app.cell
def connect(pipeline):
    # NOTE: This line allows your data to be explored in the marimo datasources which is the third item from the top in the left sidebar
    con = pipeline.dataset().ibis()
    return


@app.cell(hide_code=True)
def tests(pipeline):
    # NOTE: this cell is only needed for testing this notebook on ci
    assert pipeline.dataset().items.df().shape[0] == 50
    return


if __name__ == "__main__":
    app.run()
