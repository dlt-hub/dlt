---
title: Source
description: Explanation of what a dlt source is
keywords: [source, api, dlt.source]
---

# Source

A [source](glossary.md#source) is a logical grouping of resources, i.e., endpoints of a
single API. The most common approach is to define it in a separate Python module.

- A source is a function decorated with `@dlt.source` that returns one or more resources.
- A source can optionally define a [schema](schema.md) with tables, columns, performance hints, and
  more.
- The source Python module typically contains optional customizations and data transformations.
- The source Python module typically contains the authentication and pagination code for a particular
  API.

## Declare sources

You declare a source by decorating an (optionally async) function that returns or yields one or more resources with `@dlt.source`. Our
[Create a pipeline](../walkthroughs/create-a-pipeline.md) how-to guide teaches you how to do that.

### Create resources dynamically

You can create resources by using `dlt.resource` as a function. In the example below, we reuse a
single generator function to create a list of resources for several Hubspot endpoints.

```py
@dlt.source
def hubspot(api_key=dlt.secrets.value):

    endpoints = ["companies", "deals", "products"]

    def get_resource(endpoint):
        yield requests.get(url + "/" + endpoint).json()

    for endpoint in endpoints:
        # calling get_resource creates a generator,
        # the actual code of the function will be executed in pipeline.run
        yield dlt.resource(get_resource(endpoint), name=endpoint)
```

### Attach and configure schemas

You can [create, attach, and configure schemas](schema.md#attaching-schemas-to-sources) that will be
used when loading the source.

### Avoid long-lasting operations in source function

Do not extract data in the source function. Leave that task to your resources if possible. The source function is executed immediately when called (contrary to resources which delay execution - like Python generators). There are several benefits (error handling, execution metrics, parallelization) you get when you extract data in `pipeline.run` or `pipeline.extract`.

If this is impractical (for example, you want to reflect a database to create resources for tables), make sure you do not call the source function too often. [See this note if you plan to deploy on Airflow](../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md#2-modify-dag-file)


## Customize sources

### Access and select resources to load

You can access resources present in a source and select which of them you want to load. In the case of
the `hubspot` resource above, we could select and load the "companies", "deals", and "products" resources:

```py
from hubspot import hubspot

source = hubspot()
# "resources" is a dictionary with all resources available, the key is the resource name
print(source.resources.keys())  # print names of all resources
# print resources that are selected to load
print(source.resources.selected.keys())
# load only "companies" and "deals" using the "with_resources" convenience method
pipeline.run(source.with_resources("companies", "deals"))
```

Resources can be individually accessed and selected:

```py
# resources are accessible as attributes of a source
for c in source.companies:  # enumerate all data in the companies resource
    print(c)

# check if deals are selected to load
print(source.deals.selected)
# deselect the deals
source.deals.selected = False
```

### Filter, transform, and pivot data

You can modify and filter data in resources, for example, if we want to keep only deals after a certain
date:

```py
source.deals.add_filter(lambda deal: deal["created_at"] > yesterday)
```

Find more on transforms [here](resource.md#filter-transform-and-pivot-data).

### Load data partially

You can limit the number of items produced by each resource by calling the `add_limit` method on a source. This is useful for testing, debugging, and generating sample datasets for experimentation. You can easily get your test dataset in a few minutes, when otherwise you'd need to wait hours for the full loading to complete. Below, we limit the `pipedrive` source to just get **10 pages** of data from each endpoint. Mind that the transformers will be evaluated fully:

```py
from pipedrive import pipedrive_source

pipeline = dlt.pipeline(pipeline_name='pipedrive', destination='duckdb', dataset_name='pipedrive_data')
load_info = pipeline.run(pipedrive_source().add_limit(10))
print(load_info)
```

:::note
Note that `add_limit` **does not limit the number of records** but rather the "number of yields". `dlt` will close the iterator/generator that produces data after the limit is reached.
:::

Find more on sampling data [here](resource.md#sample-from-large-data).

### Add more resources to existing source

You can add a custom resource to a source after it was created. Imagine that you want to score all the deals with a keras model that will tell you if the deal is a fraud or not. In order to do that, you declare a new [transformer that takes the data from](resource.md#feeding-data-from-one-resource-into-another) `deals` resource and add it to the source.

```py
import dlt
from hubspot import hubspot

# source contains `deals` resource
source = hubspot()

@dlt.transformer
def deal_scores(deal_item):
    # obtain the score, deal_items contains data yielded by source.deals
    score = model.predict(featurize(deal_item))
    yield {"deal_id": deal_item, "score": score}

# connect the data from `deals` resource into `deal_scores` and add to the source
source.resources.add(source.deals | deal_scores)
# load the data: you'll see the new table `deal_scores` in your destination!
pipeline.run(source)
```
You can also set the resources in the source as follows:
```py
source.deal_scores = source.deals | deal_scores
```
or
```py
source.resources["deal_scores"] = source.deals | deal_scores
```
:::note
When adding a resource to the source, `dlt` clones the resource so your existing instance is not affected.
:::

### Reduce the nesting level of generated tables

You can limit how deep `dlt` goes when generating nested tables and flattening dicts into columns. By default, the library will descend and generate nested tables for all nested lists and columns from dicts, without limit.

```py
@dlt.source(max_table_nesting=1)
def mongo_db():
    ...
```

In the example above, we want only 1 level of nested tables to be generated (so there are no nested tables of a nested table). Typical settings:

- `max_table_nesting=0` will not generate nested tables and will not flatten dicts into columns at all. All nested data will be represented as JSON.
- `max_table_nesting=1` will generate nested tables of root tables and nothing more. All nested data in nested tables will be represented as JSON.

You can achieve the same effect after the source instance is created:

```py
from mongo_db import mongo_db

source = mongo_db()
source.max_table_nesting = 0
```

Several data sources are prone to contain semi-structured documents with very deep nesting, e.g., MongoDB databases. Our practical experience is that setting the `max_nesting_level` to 2 or 3 produces the clearest and human-readable schemas.

:::tip
The `max_table_nesting` parameter at the source level doesn't automatically apply to individual resources when accessed directly (e.g., using `source.resources["resource_1"]`). To make sure it works, either use `source.with_resources("resource_1")` or set the parameter directly on the resource.
:::

You can directly configure the `max_table_nesting` parameter on the resource level as:

```py
@dlt.resource(max_table_nesting=0)
def my_resource():
    ...
```
or
```py
my_source = source()
my_source.my_resource.max_table_nesting = 0
```

### Modify schema

The schema is available via the `schema` property of the source.
[You can manipulate this schema, i.e., add tables, change column definitions, etc., before the data is loaded.](schema.md#schema-is-modified-in-the-source-function-body)

The source provides two other convenience properties:

1. `max_table_nesting` to set the maximum nesting level for nested tables and flattened columns.
1. `root_key` to propagate the `_dlt_id` from a root table to all nested tables.

## Load sources

You can pass individual sources or a list of sources to the `dlt.pipeline` object. By default, all the
sources will be loaded into a single dataset.

You are also free to decompose a single source into several ones. For example, you may want to break
down a 50-table copy job into an Airflow DAG with high parallelism to load the data faster. To do
so, you could get the list of resources as:

```py
# get a list of resources' names
resource_list = sql_source().resources.keys()

# now we are able to make a pipeline for each resource
for res in resource_list:
    pipeline.run(sql_source().with_resources(res))
```

### Do a full refresh

You can temporarily change the "write disposition" to `replace` on all (or selected) resources within
a source to force a full refresh:

```py
p.run(merge_source(), write_disposition="replace")
```

With selected resources:

```py
p.run(tables.with_resources("users"), write_disposition="replace")
```

