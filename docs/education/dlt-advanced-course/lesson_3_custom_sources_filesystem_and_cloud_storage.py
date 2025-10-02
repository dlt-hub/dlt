import marimo

__generated_with = "0.16.4"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""# Building Custom Sources with the Filesystem in `dlt` [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_3_custom_sources_filesystem_and_cloud_storage.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_3_custom_sources_filesystem_and_cloud_storage.ipynb)"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## What you will learn""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    You will learn how to:

    - Use the `filesystem` resource to build real custom sources
    - Apply filters to file metadata (name, size, date)
    - Implement and register custom transformers
    - Enrich records with file metadata
    - Use incremental loading both for files and content

    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Setup: Download real data""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Install dlt""")
    return


@app.cell
def _():
    # magic command not supported in marimo; please file an issue to add support
    # %%capture
    # # (use marimo's built-in package management features instead) !pip install dlt[duckdb]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""We’ll use a real `.parquet` file from [TimeStored.com](https://www.timestored.com/data/sample/userdata.parquet)"""
    )
    return


app._unparsable_cell(
    r"""
    !mkdir -p local_data && wget -O local_data/userdata.parquet https://www.timestored.com/data/sample/userdata.parquet
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 1: Load Parquet file from Local Filesystem

    **What the script below does**: Lists and reads all `.parquet` files in `./local_data` and loads them into a table named `userdata`.
    """)
    return


@app.cell
def _():
    import dlt
    from dlt.sources.filesystem import filesystem, read_parquet

    # Point to the local file directory
    fs = filesystem(bucket_url="./local_data", file_glob="**/*.parquet")

    # Add a transformer
    parquet_data = fs | read_parquet()

    # Create and run pipeline
    pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
    load_info = pipeline.run(parquet_data.with_name("userdata"))
    print(load_info)

    # Inspect data
    pipeline.dataset().userdata.df().head()
    return dlt, filesystem, pipeline, read_parquet


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### **Question 1**:

    In the `my_pipeline` pipeline, and the `userdata` dataset, what is the ratio of men:women in decimal?
    """)
    return


@app.cell
def _(pipeline):
    # check out the numbers below and answer 👀
    df = pipeline.dataset().userdata.df()
    df.groupby("gender").describe()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 2: Enrich records with file metadata

    Let’s add the file name to every record to track the data origin.
    """)
    return


@app.cell
def _(dlt, filesystem):
    from dlt.common.typing import TDataItems

    @dlt.transformer()
    def read_parquet_with_filename(files: TDataItems) -> TDataItems:
        import pyarrow.parquet as pq

        for file_item in files:
            with file_item.open() as f:
                table = pq.read_table(f).to_pandas()
                table["source_file"] = file_item["file_name"]
                yield table.to_dict(orient="records")

    fs_1 = filesystem(bucket_url="./local_data", file_glob="*.parquet")
    pipeline_1 = dlt.pipeline("meta_pipeline", destination="duckdb")
    load_info_1 = pipeline_1.run((fs_1 | read_parquet_with_filename()).with_name("userdata"))
    print(load_info_1)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 3: Filter files by metadata


    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Only load files matching custom logic:""")
    return


@app.cell
def _(dlt, filesystem, read_parquet):
    fs_2 = filesystem(bucket_url="./local_data", file_glob="**/*.parquet")
    fs_2.add_filter(lambda f: "user" in f["file_name"] and f["size_in_bytes"] < 1000000)
    # Only include files that contain "user" and are < 1MB
    pipeline_2 = dlt.pipeline("filtered_pipeline", destination="duckdb")
    load_info_2 = pipeline_2.run((fs_2 | read_parquet()).with_name("userdata_filtered"))
    print(load_info_2)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 4: Load files incrementally
    Avoid reprocessing the same file twice.
    """)
    return


@app.cell
def _(dlt, filesystem, read_parquet):
    fs_3 = filesystem(bucket_url="./local_data", file_glob="**/*.parquet")
    fs_3.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    data = (fs_3 | read_parquet()).with_name("userdata")
    pipeline_3 = dlt.pipeline("incremental_pipeline", destination="duckdb")
    load_info_3 = pipeline_3.run(data)
    print(load_info_3)
    return (pipeline_3,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 5: Create a custom transformer

    Let’s read structured data from `.json` files.
    """)
    return


app._unparsable_cell(
    r"""
    @dlt.transformer(standalone=True)
    def read_json(items: TDataItems) -> TDataItems:
        from dlt.common import json

        for file_obj in items:
            with file_obj.open() as f:
                yield json.load(f)


    # Download a JSON file
    !wget -O local_data/sample.json https://jsonplaceholder.typicode.com/users

    fs = filesystem(bucket_url=\"./local_data\", file_glob=\"sample.json\")
    pipeline = dlt.pipeline(\"json_pipeline\", destination=\"duckdb\")

    load_info = pipeline.run((fs | read_json()).with_name(\"users\"))
    print(load_info)
    """,
    name="_",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    📁 You will see that this file also exists in your local_data directory.

    > A **standalone** resource is defined on a function that is top-level in a module (not an inner function) that accepts config and secrets values. Additionally, if the standalone flag is specified, the decorated function signature and docstring will be preserved. `dlt.resource` will just wrap the decorated function, and the user must call the wrapper to get the actual resource.

    Let's inspect the `users` table in your DuckDB dataset:
    """)
    return


@app.cell
def _(pipeline_3):
    pipeline_3.dataset().users.df().head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 6: Copy files before loading

    Copy files locally as part of the pipeline. This is useful for backups or post-processing.

    """)
    return


@app.cell
def _(dlt, filesystem):
    import os
    from dlt.common.storages.fsspec_filesystem import FileItemDict

    def copy_local(item: FileItemDict) -> FileItemDict:
        local_path = os.path.join("copied", item["file_name"])
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        item.fsspec.download(item["file_url"], local_path)
        return item

    fs_4 = filesystem(bucket_url="./local_data", file_glob="**/*.parquet").add_map(copy_local)
    pipeline_4 = dlt.pipeline("copy_pipeline", destination="duckdb")
    load_info_4 = pipeline_4.run(fs_4.with_name("copied_files"))
    print(load_info_4)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Next steps

    - Try building a transformer for `.xml` using `xmltodict`
    - Combine multiple directories or buckets in a single pipeline
    - Explore [more examples](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem/advanced)

    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""✅ ▶ Proceed to the [next lesson](https://colab.research.google.com/drive/14br3TZTRFwTSwpDyom7fxlZCeRF4efMk#forceEdit=true&sandboxMode=true)!"""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""![Lesson_3_Custom_sources_Filesystem_and_cloud_storage_img1](https://storage.googleapis.com/dlt-blog-images/dlt-advanced-course/Lesson_3_Custom_sources_Filesystem_and_cloud_storage_img1.webp)"""
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
