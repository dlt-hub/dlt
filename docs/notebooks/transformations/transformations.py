import marimo

__generated_with = "0.14.10-dev0"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""# `dlt` simple transformations""")
    return


@app.cell
async def _():
    # NOTE: this installs the dependencies for the notebook in WASM mode
    try:
        import micropip
        await micropip.install("duckdb")
        await micropip.install("sqlite3")
        await micropip.install("pandas")
        await micropip.install("ibis-framework[duckdb]")
        await micropip.install("dlt==1.12.4a0")
    except ModuleNotFoundError:
        pass

    return


@app.cell
def _():
    import os

    os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "False"
    os.environ["WORKERS"] = "1"

    import dlt
    import random

    # from dlt.common.runtime import telemetry
    # telemetry.stop_telemetry()
    return dlt, random


@app.cell
def _(dlt, random):
    @dlt.resource(table_name="items")
    def foo():
        for i in range(50):
            yield {"id": i, "name": f"This is item {i}", "random_int": random.randint(0,10)}

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
def _(dlt, pipeline):
    @dlt.transformation()
    def stats(dataset):
        items_table = dataset.items
        return items_table.group_by(items_table.random_int).aggregate(count=items_table.count())

    pipeline.run(stats(pipeline.dataset(dataset_type="ibis")))
    return


@app.cell
def _(pipeline):
    pipeline.dataset(dataset_type="default").stats.df()
    return


@app.cell
def _(pipeline):
    con = pipeline.dataset().ibis()
    return


if __name__ == "__main__":
    app.run()
