import os
import time
import shutil

import pytest
import polars as pl

import dlt
from dlt.destinations import filesystem
from dlt.common.typing import TLoaderFileFormat


NUM_ROWS = 10
ROW = {
    "ctext": "foo",
    "cbigint": 1,
    "cbool": True,
    "ctimestamp": "2023-07-26T14:45:00Z",
    "ccomplexlisttext": ["foo", "bar"],
    "ccomplexlistbigint": [1, 2],
}


@pytest.mark.parametrize("loader_file_format", ("parquet", "jsonl"))
def test_normalize_polars(loader_file_format: TLoaderFileFormat) -> None:
    def get_lazyframe(path: str, file_format: TLoaderFileFormat) -> pl.LazyFrame:  # type: ignore[return]
        if file_format == "parquet":
            return pl.scan_parquet(path)
        elif file_format == "jsonl":
            return pl.scan_ndjson(path)

    @dlt.resource
    def some_data():
        yield [ROW] * NUM_ROWS

    print("number of rows: ", NUM_ROWS)
    print("columns: ", ", ".join(ROW))
    print("loader_file_format: ", loader_file_format)
    print()

    schemas = dict()
    dataframes = dict()

    for normalizer in ("original", "polars"):
        if normalizer != "original":
            os.environ["NORMALIZER"] = "polars"

        pipe = dlt.pipeline(
            pipeline_name=f"{normalizer}_pipe",
            destination=filesystem("_storage"),
        )

        t1 = time.time()
        pipe.extract(some_data())
        t2 = time.time()
        pipe.normalize(loader_file_format=loader_file_format)
        t3 = time.time()
        pipe.load()
        t4 = time.time()

        print("normalizer:", normalizer)
        print(f"    extract: {(t2 - t1):.1f}")
        print(f"    normalize: {(t3 - t2):.1f}")
        print(f"    load: {(t4 - t3):.1f}")
        print()

        # store dlt Schema objects to assert equality later
        schema = pipe.default_schema

        # hack "seen-data" key because "polars" normalizer does not yet create DataWriterMetrics
        if normalizer == "polars":
            for table_name in schema.data_table_names():
                schema.tables[table_name]["x-normalizer"] = {"seen-data": True}

        schemas[normalizer] = schema

        # store loaded files as lazyframes to assert equality later
        dataframes[normalizer] = {
            table_name: get_lazyframe(
                path=f"_storage/{pipe.dataset_name}/{table_name}", file_format=loader_file_format
            )
            for table_name in schema.data_table_names()
        }
    print("(all runtimes are seconds)")

    # assert dlt schema equality
    assert schemas["original"].tables == schemas["polars"].tables

    # assert dataframe equality
    assert dataframes["original"].keys() == dataframes["polars"].keys()
    for table_name in dataframes["original"].keys():
        df_original = dataframes["original"][table_name].collect()
        df_polars = dataframes["polars"][table_name].collect()

        # assert dataframe schema equality
        assert df_original.schema.to_python() == df_polars.schema.to_python()

        # assert dataframe contents equality (after removing "random" columns)
        system_cols = ["_dlt_id", "_dlt_load_id", "_dlt_parent_id"]
        assert df_original.drop(system_cols, strict=False).equals(
            df_polars.drop(system_cols, strict=False)
        )

    # teardown
    shutil.rmtree("_storage")
