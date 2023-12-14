---
title: Control nested MongoDB data
description: Learn how control nested data
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="This example demonstrates how to control nested data using Python and the dlt library. It covers working with MongoDB, incremental loading, limiting nesting levels, and applying data type hints."
    slug="nested_data"
    run_file="nested_data"
    destination="duckdb"/>

## Control nested data

In this example, you'll find a Python script that demonstrates how to control nested data using the `dlt` library.

We'll learn how to:
- [Adjust maximum nesting level in three ways:](../../general-usage/source#reduce-the-nesting-level-of-generated-tables)
  - Limit nesting levels with dlt decorator.
  - Dynamic nesting level adjustment.
  - Apply data type hints.
- Work with [MongoDB](../../dlt-ecosystem/verified-sources/mongodb) in Python and `dlt`.
- Enable [incremental loading](../../general-usage/incremental-loading) for efficient data extraction.

### Install pymongo

```shell
 pip install pymongo>=4.3.3
```

### Loading code

<!--@@@DLT_SNIPPET_START code/nested_data-snippets.py::nested_data-->
```py
from itertools import islice
from typing import Any, Dict, Iterator, Optional

from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from pendulum import _datetime
from pymongo import MongoClient

import dlt
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TDataItem
from dlt.common.utils import map_nested_in_place

CHUNK_SIZE = 10000

# You can limit how deep dlt goes when generating child tables.
# By default, the library will descend and generate child tables
# for all nested lists, without a limit.
# In this example, we specify that we only want to generate child tables up to level 2,
# so there will be only one level of child tables within child tables.
@dlt.source(max_table_nesting=2)
def mongodb_collection(
    connection_url: str = dlt.secrets.value,
    database: Optional[str] = dlt.config.value,
    collection: str = dlt.config.value,
    incremental: Optional[dlt.sources.incremental] = None,  # type: ignore[type-arg]
    write_disposition: Optional[str] = dlt.config.value,
) -> Any:
    # set up mongo client
    client: Any = MongoClient(connection_url, uuidRepresentation="standard", tz_aware=True)
    mongo_database = client.get_default_database() if not database else client[database]
    collection_obj = mongo_database[collection]

    def collection_documents(
        client: Any,
        collection: Any,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
    ) -> Iterator[TDataItem]:
        LoaderClass = CollectionLoader

        loader = LoaderClass(client, collection, incremental=incremental)
        yield from loader.load_documents()

    return dlt.resource(  # type: ignore
        collection_documents,
        name=collection_obj.name,
        primary_key="_id",
        write_disposition=write_disposition,
    )(client, collection_obj, incremental=incremental)
```
<!--@@@DLT_SNIPPET_END code/nested_data-snippets.py::nested_data-->

### Run the pipeline

<!--@@@DLT_SNIPPET_START code/nested_data-snippets.py::nested_data_run-->
```py
if __name__ == "__main__":
    # When we created the source, we set max_table_nesting to 2.
    # This ensures that the generated tables do not have more than two
    # levels of nesting, even if the original data structure is more deeply nested.
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_pipeline",
        destination="duckdb",
        dataset_name="unpacked_data",
    )
    source_data = mongodb_collection(
        collection="movies", write_disposition="replace"
    )
    load_info = pipeline.run(source_data)
    print(load_info)

    # The second method involves setting the max_table_nesting attribute directly
    # on the source data object.
    # This allows for dynamic control over the maximum nesting
    # level for a specific data source.
    # Here the nesting level is adjusted before running the pipeline.
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_pipeline",
        destination="duckdb",
        dataset_name="not_unpacked_data",
    )
    source_data = mongodb_collection(
        collection="movies", write_disposition="replace"
    )
    source_data.max_table_nesting = 0
    load_info = pipeline.run(source_data)
    print(load_info)

    # The third method involves applying data type hints to specific columns in the data.
    # In this case, we tell dlt that column 'cast' (containing a list of actors)
    # in 'movies' table should have type complex which means
    # that it will be loaded as JSON/struct and not as child table.
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_pipeline",
        destination="duckdb",
        dataset_name="unpacked_data_without_cast",
    )
    source_data = mongodb_collection(
        collection="movies", write_disposition="replace"
    )
    source_data.movies.apply_hints(columns={"cast": {"data_type": "complex"}})
    load_info = pipeline.run(source_data)
    print(load_info)
```
<!--@@@DLT_SNIPPET_END code/nested_data-snippets.py::nested_data_run-->
