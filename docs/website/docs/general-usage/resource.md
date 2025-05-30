---
title: Resource
description: Explanation of what a dlt resource is
keywords: [resource, api endpoint, dlt.resource]
---

# Resource

## Declare a resource

A [resource](glossary.md#resource) is an ([optionally async](../reference/performance.md#parallelism-within-a-pipeline)) function that yields data. To create a resource, we add the `@dlt.resource` decorator to that function.

Commonly used arguments:

- `name`: The name of the table generated by this resource. Defaults to the decorated function name.
- `write_disposition`: How should the data be loaded at the destination? Currently supported: `append`,
  `replace`, and `merge`. Defaults to `append.`

Example:

```py
@dlt.resource(name='table_name', write_disposition='replace')
def generate_rows():
	for i in range(10):
		yield {'id': i, 'example_string': 'abc'}

@dlt.source
def source_name():
    return generate_rows
```

To get the data of a resource, we could do:

```py
for row in generate_rows():
    print(row)

for row in source_name().resources.get('table_name'):
    print(row)
```

Typically, resources are declared and grouped with related resources within a [source](source.md) function.

### Define schema

`dlt` will infer the [schema](schema.md) for tables associated with resources from the resource's data.
You can modify the generation process by using the table and column hints. The resource decorator accepts the following arguments:

1. `table_name`: the name of the table, if different from the resource name.
1. `primary_key` and `merge_key`: define the name of the columns (compound keys are allowed) that will receive those hints. Used in [incremental loading](incremental-loading.md) and [merge loading](merge-loading.md).
1. `columns`: lets you define one or more columns, including the data types, nullability, and other hints. The column definition is a `TypedDict`: `TTableSchemaColumns`. In the example below, we tell `dlt` that the column `tags` (containing a list of tags) in the `user` table should have type `json`, which means that it will be loaded as JSON/struct and not as a separate nested table.

  ```py
  @dlt.resource(name="user", columns={"tags": {"data_type": "json"}})
  def get_users():
    ...

  # the `table_schema` method gets the table schema generated by a resource
  print(get_users().compute_table_schema())
  ```

:::note
You can pass dynamic hints which are functions that take the data item as input and return a hint value. This lets you create table and column schemas depending on the data. See an [example below](#adjust-schema-when-you-yield-data).
:::

### Put a contract on tables, columns, and data
Use the `schema_contract` argument to tell dlt how to [deal with new tables, data types, and bad data types](schema-contracts.md). For example, if you set it to **freeze**, `dlt` will not allow for any new tables, columns, or data types to be introduced to the schema - it will raise an exception. Learn more about available contract modes [here](schema-contracts.md#setting-up-the-contract).

### Define schema of nested tables

`dlt` creates [nested tables](schema.md#nested-references-root-and-nested-tables) to store [list of objects](destination-tables.md#nested-tables) if present in your data.
You can define the schema of such tables with `nested_hints` argument to `@dlt.resource`:
```py
import dlt

@dlt.resource(
    nested_hints={
        "purchases": dlt.mark.make_nested_hints(
            columns=[{"name": "price", "data_type": "decimal"}],
            schema_contract={"columns": "freeze"},
        )
    },
)
def customers():
    """Load customer data from a simple python list."""
    yield [
        {
            "id": 1,
            "name": "simon",
            "city": "berlin",
            "purchases": [{"id": 1, "name": "apple", "price": "1.50"}],
        },
    ]
```
Here we convert the `price` field in list of `purchases` to decimal type and set the schema contract to lock the list
of columns in it. We use convenience function `dlt.mark.make_nested_hints` to generate nested hints dictionary. You are
free to use it directly.

Mind that `purchases` list will be stored as table with name `customers__purchases`. When declaring nested hints you just need
to specify nested field(s) name(s). In case of deeper nesting ie. let's say each `purchase` has a list of `coupons` applied,
you can apply hints to coupons and define `customers__purchases__coupons` table schema:
```py
import dlt

@dlt.resource(
    nested_hints={
        "purchases": {},
        ("purchases", "coupons"): {
            "columns": {"registered_at": {"data_type": "timestamp"}}
        }
    },
)
def customers():
    ...
```
Here we use `("purchases", "coupons")` to locate list at the depth of 2 and set the data type on `registered_at` column
to `timestamp`. We do that by directly using nested hints dict.
Note that we specified `purchases` with an empty list of hints. **You are required to specify all parent hints, even if they 
are empty. Currently we are not adding missing path elements automatically**.

You can use `nested_hints` primarily to set column hints and schema contract, those work exactly as in case of root tables.
* `file_format` has no effect (not implemented yet)
* `write_disposition` works as expected but leads to unintended consequences (ie. you can set nested table to `replace`) while root table is `append`.
* `references` will create [table references](schema.md#table-references-1) (annotations) as expected.
* `primary_key` and `merge_key`: **setting those will convert nested table into a regular table, with a separate write disposition, file format etc.**
[It allows you to create custom table relationships ie. using natural primary and foreign keys present in the data.](schema.md#generate-custom-linking-for-nested-tables)

:::tip
[REST API Source](../dlt-ecosystem/verified-sources/rest_api/basic.md) accepts `nested_hints` argument as well.

You can apply nested hints after the resource was created by using [apply_hints](#set-table-name-and-adjust-schema).
:::


### Define a schema with Pydantic

You can alternatively use a [Pydantic](https://pydantic-docs.helpmanual.io/) model to define the schema.
For example:

```py
from pydantic import BaseModel
from typing import List, Optional, Union

class Address(BaseModel):
    street: str
    city: str
    postal_code: str

class User(BaseModel):
    id: int
    name: str
    tags: List[str]
    email: Optional[str]
    address: Address
    status: Union[int, str]

@dlt.resource(name="user", columns=User)
def get_users():
    ...
```

The data types of the table columns are inferred from the types of the Pydantic fields. These use the same type conversions
as when the schema is automatically generated from the data.

Pydantic models integrate well with [schema contracts](schema-contracts.md) as data validators.

Things to note:

- Fields with an `Optional` type are marked as `nullable`.
- Fields with a `Union` type are converted to the first (not `None`) type listed in the union. For example, `status: Union[int, str]` results in a `bigint` column.
- `list`, `dict`, and nested Pydantic model fields will use the `json` type, which means they'll be stored as a JSON object in the database instead of creating nested tables.

You can override this by configuring the Pydantic model:

```py
from typing import ClassVar
from dlt.common.libs.pydantic import DltConfig

class UserWithNesting(User):
  dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}

@dlt.resource(name="user", columns=UserWithNesting)
def get_users():
    ...
```

`"skip_nested_types"` omits any `dict`/`list`/`BaseModel` type fields from the schema, so dlt will fall back on the default
behavior of creating nested tables for these fields.

We do not support `RootModel` that validate simple types. You can add such a validator yourself, see [data filtering section](#filter-transform-and-pivot-data).

### Dispatch data to many tables

You can load data to many tables from a single resource. The most common case is a stream of events
of different types, each with a different data schema. To deal with this, you can use the `table_name`
argument on `dlt.resource`. You could pass the table name as a function with the data item as an
argument and the `table_name` string as a return value.

For example, a resource that loads GitHub repository events wants to send `issue`, `pull request`,
and `comment` events to separate tables. The type of the event is in the "type" field.

```py
# send item to a table with name item["type"]
@dlt.resource(table_name=lambda event: event['type'])
def repo_events() -> Iterator[TDataItems]:
    yield item

# the `table_schema` method gets the table schema generated by a resource and takes an optional
# data item to evaluate dynamic hints
print(repo_events().compute_table_schema({"type": "WatchEvent", "id": ...}))
```

In more advanced cases, you can dispatch data to different tables directly in the code of the
resource function:

```py
@dlt.resource
def repo_events() -> Iterator[TDataItems]:
    # mark the "item" to be sent to the table with the name item["type"]
    yield dlt.mark.with_table_name(item, item["type"])
```

### Parametrize a resource

You can add arguments to your resource functions like to any other. Below we parametrize our
`generate_rows` resource to generate the number of rows we request:

```py
@dlt.resource(name='table_name', write_disposition='replace')
def generate_var_rows(nr):
    for i in range(nr):
        yield {'id': i, 'example_string': 'abc'}

for row in generate_var_rows(10):
    print(row)

for row in generate_var_rows(20):
    print(row)
```

:::tip
You can mark some resource arguments as [configuration and credentials](credentials) values so `dlt` can pass them automatically to your functions.
:::

### Process resources with `dlt.transformer`

You can feed data from one resource into another. The most common case is when you have an API that returns a list of objects (i.e., users) in one endpoint and user details in another. You can deal with this by declaring a resource that obtains a list of users and another resource that receives items from the list and downloads the profiles.

```py
@dlt.resource(write_disposition="replace")
def users(limit=None):
    for u in _get_users(limit):
        yield u

# Feed data from users as user_item below,
# all transformers must have at least one
# argument that will receive data from the parent resource
@dlt.transformer(data_from=users)
def users_details(user_item):
    for detail in _get_details(user_item["user_id"]):
        yield detail

# Just load the users_details.
# dlt figures out dependencies for you.
pipeline.run(users_details)
```
In the example above, `users_details` will receive data from the default instance of the `users` resource (with `limit` set to `None`). You can also use the **pipe |** operator to bind resources dynamically.
```py
# You can be more explicit and use a pipe operator.
# With it, you can create dynamic pipelines where the dependencies
# are set at run time and resources are parametrized, i.e.,
# below we want to load only 100 users from the `users` endpoint.
pipeline.run(users(limit=100) | users_details)
```

:::tip
Transformers are allowed not only to **yield** but also to **return** values and can decorate **async** functions and [**async generators**](../reference/performance.md#extract). Below we decorate an async function and request details on two pokemons. HTTP calls are made in parallel via the httpx library.
```py
import dlt
import httpx


@dlt.transformer
async def pokemon(id):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"https://pokeapi.co/api/v2/pokemon/{id}")
        return r.json()

# Get Bulbasaur and Ivysaur (you need dlt 0.4.6 for the pipe operator working with lists).
print(list([1,2] | pokemon()))
```
:::

### Declare a standalone resource
A standalone resource is defined on a function that is top-level in a module (not an inner function) that accepts config and secrets values. Here `dlt.resource` just wraps the decorated function, and the user must call the wrapper to get the actual resource. Below we declare a `filesystem` resource that must be called before use.
```py
@dlt.resource
def fs_resource(bucket_url=dlt.config.value):
  """List and yield files in `bucket_url`."""
  ...

# `filesystem` must be called before it is extracted or used in any other way.
pipeline.run(fs_resource("s3://my-bucket/reports"), table_name="reports")
```

Resource may have a dynamic name that depends on the arguments passed to the decorated function. For example:
```py
@dlt.resource(name=lambda args: args["stream_name"])
def kinesis(stream_name: str):
    ...

kinesis_stream = kinesis("telemetry_stream")
```
`kinesis_stream` resource has a name **telemetry_stream**.

### Declare parallel and async resources
You can extract multiple resources in parallel threads or with async IO.
To enable this for a sync resource, you can set the `parallelized` flag to `True` in the resource decorator:

```py
@dlt.resource(parallelized=True)
def get_users():
    for u in _get_users():
        yield u

@dlt.resource(parallelized=True)
def get_orders():
    for o in _get_orders():
        yield o

# users and orders will be iterated in parallel in two separate threads
pipeline.run([get_users(), get_orders()])
```

Async generators are automatically extracted concurrently with other resources:

```py
@dlt.resource
async def get_users():
    async for u in _get_users():  # Assuming _get_users is an async generator
        yield u
```

Please find more details in [extract performance](../reference/performance.md#extract)

## Customize resources

### Filter, transform, and pivot data

You can attach any number of transformations that are evaluated on an item-per-item basis to your
resource. The available transformation types:

- **map** - transform the data item (`resource.add_map`).
- **filter** - filter the data item (`resource.add_filter`).
- **yield map** - a map that returns an iterator (so a single row may generate many rows -
  `resource.add_yield_map`).

Example: We have a resource that loads a list of users from an API endpoint. We want to customize it
so:

1. We remove users with `user_id == "me"`.
2. We anonymize user data.

Here's our resource:

```py
import dlt

@dlt.resource(write_disposition="replace")
def users():
    ...
    users = requests.get(RESOURCE_URL)
    ...
    yield users
```

Here's our script that defines transformations and loads the data:

```py
from pipedrive import users

def anonymize_user(user_data):
    user_data["user_id"] = _hash_str(user_data["user_id"])
    user_data["user_email"] = _hash_str(user_data["user_email"])
    return user_data

# add the filter and anonymize function to users resource and enumerate
for user in users().add_filter(lambda user: user["user_id"] != "me").add_map(anonymize_user):
    print(user)
```

### Reduce the nesting level of generated tables

You can limit how deep `dlt` goes when generating nested tables and flattening dicts into columns. By default, the library will descend
and generate nested tables for all nested lists, without limit.

:::note
`max_table_nesting` is optional so you can skip it, in this case, dlt will
use it from the source if it is specified there or fallback to the default
value which has 1000 as the maximum nesting level.
:::

```py
import dlt

@dlt.resource(max_table_nesting=1)
def my_resource():
    yield {
        "id": 1,
        "name": "random name",
        "properties": [
            {
                "name": "customer_age",
                "type": "int",
                "label": "Age",
                "notes": [
                    {
                        "text": "string",
                        "author": "string",
                    }
                ]
            }
        ]
    }
```

In the example above, we want only 1 level of nested tables to be generated (so there are no nested
tables of a nested table). Typical settings:

- `max_table_nesting=0` will not generate nested tables and will not flatten dicts into columns at all. All nested data will be
  represented as JSON.
- `max_table_nesting=1` will generate nested tables of root tables and nothing more. All nested
  data in nested tables will be represented as JSON.

You can achieve the same effect after the resource instance is created:

```py
resource = my_resource()
resource.max_table_nesting = 0
```

Several data sources are prone to contain semi-structured documents with very deep nesting, i.e.,
MongoDB databases. Our practical experience is that setting the `max_nesting_level` to 2 or 3
produces the clearest and human-readable schemas.

### Sample from large data

If your resource loads thousands of pages of data from a REST API or millions of rows from a database table, you may want to sample just a fragment of it in order to quickly see the dataset with example data and test your transformations, etc. To do this, you limit how many items will be yielded by a resource (or source) by calling the `add_limit` method. This method will close the generator that produces the data after the limit is reached.

In the example below, we load just the first 10 items from an infinite counter - that would otherwise never end.

```py
r = dlt.resource(itertools.count(), name="infinity").add_limit(10)
assert list(r) == list(range(10))
```

:::note
Note that `add_limit` **does not limit the number of records** but rather the "number of yields". Depending on how your resource is set up, the number of extracted rows may vary. For example, consider this resource:

```py
@dlt.resource
def my_resource():
    for i in range(100):
        yield [{"record_id": j} for j in range(15)]

dlt.pipeline(destination="duckdb").run(my_resource().add_limit(10))
```
The code above will extract `15*10=150` records. This is happening because in each iteration, 15 records are yielded, and we're limiting the number of iterations to 10.
:::

Altenatively you can also apply a time limit to the resource. The code below will run the extraction for 10 seconds and extract how ever many items are yielded in that time. In combination with incrementals, this can be useful for batched loading or for loading on machines that have a run time limit.

```py
dlt.pipeline(destination="duckdb").run(my_resource().add_limit(max_time=10))
```

You can also apply a combination of both limits. In this case the extraction will stop as soon as either limit is reached.

```py
dlt.pipeline(destination="duckdb").run(my_resource().add_limit(max_items=10, max_time=10))
```


Some notes about the `add_limit`:

1. `add_limit` does not skip any items. It closes the iterator/generator that produces data after the limit is reached.
2. You cannot limit transformers. They should process all the data they receive fully to avoid inconsistencies in generated datasets.
3. Async resources with a limit added may occasionally produce one item more than the limit on some runs. This behavior is not deterministic.
4. Calling add limit on a resource will replace any previously set limits settings.
5. For time-limited resources, the timer starts when the first item is processed. When resources are processed sequentially (FIFO mode), each resource's time limit applies also sequentially. In the default round robin mode, the time limits will usually run concurrently.

:::tip
If you are parameterizing the value of `add_limit` and sometimes need it to be disabled, you can set `None` or `-1` to disable the limiting.
You can also set the limit to `0` for the resource to not yield any items.
:::

### Set table name and adjust schema

You can change the schema of a resource, whether it is standalone or part of a source. Look for a method named `apply_hints` which takes the same arguments as the resource decorator. Obviously, you should call this method before data is extracted from the resource. The example below converts an `append` resource loading the `users` table into a [merge](merge-loading.md) resource that will keep just one updated record per `user_id`. It also adds ["last value" incremental loading](incremental/cursor.md) on the `created_at` column to prevent requesting again the already loaded records:

```py
tables = sql_database()
tables.users.apply_hints(
    write_disposition="merge",
    primary_key="user_id",
    incremental=dlt.sources.incremental("updated_at")
)
pipeline.run(tables)
```

To change the name of a table to which the resource will load data, do the following:
```py
tables = sql_database()
tables.users.table_name = "other_users"
```

### Adjust schema when you yield data

You can set or update the table name, columns, and other schema elements when your resource is executed, and you already yield data. Such changes will be merged with the existing schema in the same way the `apply_hints` method above works. There are many reasons to adjust the schema at runtime. For example, when using Airflow, you should avoid lengthy operations (i.e., reflecting database tables) during the creation of the DAG, so it is better to do it when the DAG executes. You may also emit partial hints (i.e., precision and scale for decimal types) for columns to help `dlt` type inference.

```py
@dlt.resource
def sql_table(credentials, schema, table):
    # Create a SQL Alchemy engine
    engine = engine_from_credentials(credentials)
    engine.execution_options(stream_results=True)
    metadata = MetaData(schema=schema)
    # Reflect the table schema
    table_obj = Table(table, metadata, autoload_with=engine)

    for idx, batch in enumerate(table_rows(engine, table_obj)):
      if idx == 0:
        # Emit the first row with hints, table_to_columns and _get_primary_key are helpers that extract dlt schema from
        # SqlAlchemy model
        yield dlt.mark.with_hints(
            batch,
            dlt.mark.make_hints(columns=table_to_columns(table_obj), primary_key=_get_primary_key(table_obj)),
        )
      else:
        # Just yield all the other rows
        yield batch

```

In the example above, we use `dlt.mark.with_hints` and `dlt.mark.make_hints` to emit columns and primary key with the first extracted item. The table schema will be adjusted after the `batch` is processed in the extract pipeline but before any schema contracts are applied, and data is persisted in the load package.

:::tip
You can emit columns as a Pydantic model and use dynamic hints (i.e., lambda for table name) as well. You should avoid redefining `Incremental` this way.
:::

### Import external files
You can import external files, i.e., CSV, Parquet, and JSONL, by yielding items marked with `with_file_import`, optionally passing a table schema corresponding to the imported file. dlt will not read, parse, or normalize any names (i.e., CSV or Arrow headers) and will attempt to copy the file into the destination as is.
```py
import os
import dlt
from dlt.sources.filesystem import filesystem

columns: List[TColumnSchema] = [
    {"name": "id", "data_type": "bigint"},
    {"name": "name", "data_type": "text"},
    {"name": "description", "data_type": "text"},
    {"name": "ordered_at", "data_type": "date"},
    {"name": "price", "data_type": "decimal"},
]

import_folder = "/tmp/import"

@dlt.transformer(columns=columns)
def orders(items: Iterator[FileItemDict]):
  for item in items:
    # copy the file locally
      dest_file = os.path.join(import_folder, item["file_name"])
      # download the file
      item.fsspec.download(item["file_url"], dest_file)
      # tell dlt to import the dest_file as `csv`
      yield dlt.mark.with_file_import(dest_file, "csv")


# use the filesystem core source to glob a bucket

downloader = filesystem(
  bucket_url="s3://my_bucket/csv",
  file_glob="today/*.csv.gz") | orders

info = pipeline.run(orders, destination="snowflake")
```
In the example above, we glob all zipped csv files present on **my_bucket/csv/today** (using the `filesystem` verified source) and send file descriptors to the `orders` transformer. The transformer downloads and imports the files into the extract package. At the end, `dlt` sends them to Snowflake (the table will be created because we use `column` hints to define the schema).

If imported `csv` files are not in `dlt` [default format](../dlt-ecosystem/file-formats/csv.md#default-settings), you may need to pass additional configuration.
```toml
[destination.snowflake.csv_format]
delimiter="|"
include_header=false
on_error_continue=true
```

You can sniff the schema from the data, i.e., using DuckDB to infer the table schema from a CSV file. `dlt.mark.with_file_import` accepts additional arguments that you can use to pass hints at runtime.

:::note
* If you do not define any columns, the table will not be created in the destination. `dlt` will still attempt to load data into it, so if you create a fitting table upfront, the load process will succeed.
* Files are imported using hard links if possible to avoid copying and duplicating the storage space needed.
:::

### Duplicate and rename resources
There are cases when your resources are generic (i.e., bucket filesystem) and you want to load several instances of it (i.e., files from different folders) into separate tables. In the example below, we use the `filesystem` source to load csvs from two different folders into separate tables:

```py
@dlt.resource
def fs_resource(bucket_url):
  # list and yield files in bucket_url
  ...

@dlt.transformer
def csv_reader(file_item):
  # load csv, parse, and yield rows in file_item
  ...

# create two extract pipes that list files from the bucket and send them to the reader.
# by default, both pipes will load data to the same table (csv_reader)
reports_pipe = fs_resource("s3://my-bucket/reports") | csv_reader()
transactions_pipe = fs_resource("s3://my-bucket/transactions") | csv_reader()

# so we rename resources to load to "reports" and "transactions" tables
pipeline.run(
  [reports_pipe.with_name("reports"), transactions_pipe.with_name("transactions")]
)
```

The `with_name` method returns a deep copy of the original resource, its data pipe, and the data pipes of a parent resource. A renamed clone is fully separated from the original resource (and other clones) when loading: it maintains a separate [resource state](state.md#read-and-write-pipeline-state-in-a-resource) and will load to a table.

## Load resources

You can pass individual resources or a list of resources to the `dlt.pipeline` object. The resources loaded outside the source context will be added to the [default schema](schema.md) of the pipeline.

```py
@dlt.resource(name='table_name', write_disposition='replace')
def generate_var_rows(nr):
    for i in range(nr):
        yield {'id': i, 'example_string': 'abc'}

pipeline = dlt.pipeline(
    pipeline_name="rows_pipeline",
    destination="duckdb",
    dataset_name="rows_data"
)
# load an individual resource
pipeline.run(generate_var_rows(10))
# load a list of resources
pipeline.run([generate_var_rows(10), generate_var_rows(20)])
```

### Pick loader file format for a particular resource

You can request a particular loader file format to be used for a resource.

```py
@dlt.resource(file_format="parquet")
def generate_var_rows(nr):
    for i in range(nr):
        yield {'id': i, 'example_string': 'abc'}
```
The resource above will be saved and loaded from a Parquet file (if the destination supports it).

:::note
A special `file_format`: **preferred** will load the resource using a format that is preferred by a destination. This setting supersedes the `loader_file_format` passed to the `run` method.
:::

### Do a full refresh

To do a full refresh of an `append` or `merge` resource, you set the `refresh` argument on the `run` method to `drop_data`. This will truncate the tables without dropping them.

```py
p.run(merge_source(), refresh="drop_data")
```

You can also [fully drop the tables](pipeline.md#refresh-pipeline-data-and-state) in the `merge_source`:

```py
p.run(merge_source(), refresh="drop_sources")
```

