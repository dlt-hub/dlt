import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    import pyarrow
    import pyarrow_hotfix
    import pandas
    import polars

    p = dlt.attach("arrow")
    con = p.dataset().ibis()
    return


if __name__ == "__main__":
    app.run()
