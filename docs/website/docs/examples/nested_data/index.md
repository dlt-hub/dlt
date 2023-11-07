---
title: Control nested data
description: Learn how control nested data
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this tutorial, you will learn how to control nested data"
    slug="nested_data"
    run_file="nested_data" />

## Control nested data

In this example, you'll find a Python script that

We'll learn:

### Install pymongo

```shell
 pip install pymongo>=4.3.3
```

### Loading code

<!--@@@DLT_SNIPPET_START code/nested_data-snippets.py::nested_data-->
```py
from itertools import islice
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

import dlt
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TDataItem
from dlt.common.utils import map_nested_in_place
from pendulum import _datetime
from pymongo import ASCENDING, DESCENDING, MongoClient

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
    parallel: Optional[bool] = dlt.config.value,
) -> Any:
    # set up mongo client
    client = MongoClient(
        connection_url, uuidRepresentation="standard", tz_aware=True
    )
    mongo_database = client.get_default_database() if not database else client[database]
    collection_obj = mongo_database[collection]

    def collection_documents(
        client,
        collection,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
    ) -> Iterator[TDataItem]:
        LoaderClass = CollectionLoader

        loader = LoaderClass(client, collection, incremental=incremental)
        for data in loader.load_documents():
            yield data

    return dlt.resource(  # type: ignore
        collection_documents,
        name=collection_obj.name,
        primary_key="_id",
        write_disposition=write_disposition,
    )(client, collection_obj, incremental=incremental, parallel=parallel)
```
<!--@@@DLT_SNIPPET_END code/nested_data-snippets.py::nested_data-->

### Run the pipeline

<!--@@@DLT_SNIPPET_START code/nested_data-snippets.py::nested_data_run-->
```py
if __name__ == "__main__":
    # build duck db pipeline
    pipeline = dlt.pipeline(
        pipeline_name="mongodb_pipeline", destination="duckdb", dataset_name="pokemon_data"
    )
    source_data = mongodb_collection()
    source_data.max_table_nesting = 1

    load_info = pipeline.run(source_data)
    print(load_info)
```
<!--@@@DLT_SNIPPET_END code/nested_data-snippets.py::nested_data_run-->
