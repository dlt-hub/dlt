import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")

with app.setup:
    import dlt
    import marimo as mo


@app.cell(hide_code=True)
def home():
    mo.md(r"""
    # Setup
    The code loads the Jaffleshop data from a REST API and a parquet file.
    """)
    return


print(home)
