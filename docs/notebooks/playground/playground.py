import marimo

__generated_with = "0.14.10"
app = marimo.App()


@app.cell(hide_code=True)
async def initialize():
    import sys

    # NOTE: this installs the dependencies for the notebook if run on pyodide
    if sys.platform == "emscripten":
        import micropip

        # dependencies needed for dlt
        await micropip.install("dlt[duckdb]")
        await micropip.install("pandas")
        # dependencies needed for ibis
        await micropip.install("requests")
        await micropip.install("ibis-framework[duckdb]")

    return


@app.cell
def run():
    import dlt
    import requests

    @dlt.resource(name="customers")
    def fetch_customers():
        response = requests.get("https://jaffle-shop.dlthub.com/api/v1/customers?limit=100")
        yield response.json()

    pipeline = dlt.pipeline(
        pipeline_name="customers_pipeline",
        destination="duckdb",
        dataset_name="raw_data",
        dev_mode=True,
    )
    load_info = pipeline.run(fetch_customers())
    print(load_info)
    return (pipeline,)


@app.cell
def view(pipeline):
    # NOTE: This line displays the data of the customers table in a marimo table
    pipeline.dataset().customers.df()
    return


@app.cell
def connect(pipeline):
    # NOTE: This line allows your data to be explored in the marimo datasources which is the third item from the top in the left sidebar
    con = pipeline.dataset().ibis()
    return (con,)


@app.cell(hide_code=True)
def tests(pipeline):
    # NOTE: this cell is only needed for testing this notebook on ci
    assert pipeline.dataset().customers.df().shape[0] == 100
    return


if __name__ == "__main__":
    app.run()
