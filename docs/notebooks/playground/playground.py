import marimo

__generated_with = "0.13.15"
app = marimo.App()


@app.cell(hide_code=True)
async def initialize():
    import sys
    import marimo as mo

    # NOTE: this installs the dependencies for the notebook if run on pyodide
    if sys.platform == "emscripten":
        import micropip

        # dependencies needed for dlt
        await micropip.install("dlt[duckdb]")
        await micropip.install("pandas")
        # dependencies needed for ibis
        await micropip.install("sqlite3")
        await micropip.install("ibis-framework[duckdb]")

    return sys, mo


@app.cell
def run(dlt):
    import dlt, requests
    

    
    @dlt.resource(table_name="users")
    def users():
        resp = requests.get("https://dummyjson.com/users", timeout=30)
        yield from resp.json()["users"]

    pipe = dlt.pipeline("dummy_users", "duckdb")
    info = pipe.run(users())
    print(info)
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
