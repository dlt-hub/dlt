---
title: Source
description: Explanation of what a dlt source is
keywords: [source, api, dlt.source]
---

# Source

A [source](glossary.md#source) is a logical grouping of resources i.e. endpoints of a
single API. The most common approach is to define it in a separate Python module.

- A source is a function decorated with `@dlt.source` that returns one or more resources.
- A source can optionally define a [schema](schema.md) with tables, columns, performance hints and
  more.
- The source Python module typically contains optional customizations and data transformations.
- The source Python module typically contains the authentication and pagination code for particular
  API.

## Declare sources

You declare source by decorating a function returning one or more resource with `dlt.source`. Our
[Create a pipeline](../walkthroughs/create-a-pipeline.md) how to guide teaches you how to do that.

### Create resources dynamically

You can create resources by using `dlt.resource` as a function. In an example below we reuse a
single generator function to create a list of resources for several Hubspot endpoints.

```python
@dlt.source
def hubspot(api_key=dlt.secrets.value):

    endpoints = ["companies", "deals", "product"]

    def get_resource(endpoint):
        yield requests.get(url + "/" + endpoint).json()

    for endpoint in endpoints:
        # calling get_resource creates generator,
        # the actual code of the function will be executed in pipeline.run
        yield dlt.resource(get_resource(endpoint), name=endpoint)
```

### Attach and configure schemas

You can [create, attach and configure schema](schema.md#attaching-schemas-to-sources) that will be
used when loading the source.

## Customize sources

### Access and select resources to load

You can access resources present in a source and select which of them you want to load. In case of
`hubspot` resource above we could select and load "companies", "deals" and "products" resources:

```python
from hubspot import hubspot

source = hubspot()
# "resources" is a dictionary with all resources available, the key is the resource name
print(source.resources.keys())  # print names of all resources
# print resources that are selected to load
print(source.resources.selected.keys())
# load only "companies" and "deals" using "with_resources" convenience method
pipeline.run(source.with_resources("companies", "deals"))
```

Resources can be individually accessed and selected:

```python
# resources are accessible as attributes of a source
for c in source.companies:  # enumerate all data in companies resource
    print(c)

# check if deals are selected to load
print(source.deals.selected)
# deselect the deals
source.deals.selected = False
```

### Filter, transform and pivot data

You can modify and filter data in resources, for example if we want to keep only deals after certain
date:

```python
source.deals.add_filter(lambda deal: deal["created_at"] > yesterday)
```

Find more on transforms [here](resource.md#filter-transform-and-pivot-data).

### Load data partially

You can limit the number of items produced by each resource by calling a `add_limit` method on a
source. This is useful for testing, debugging and generating sample datasets for experimentation.
You can easily get your test dataset in a few minutes, when otherwise you'd need to wait hours for
the full loading to complete. Below we limit the `pipedrive` source to just get 10 pages of data
from each endpoint. Mind that the transformers will be evaluated fully:

```python
from pipedrive import pipedrive_source

pipeline = dlt.pipeline(pipeline_name='pipedrive', destination='duckdb', dataset_name='pipedrive_data')
load_info = pipeline.run(pipedrive_source().add_limit(10))
print(load_info)
```

Find more on sampling data [here](resource.md#sample-from-large-data).

### Add more resources to existing source

You can add a custom resource to source after it was created. Imagine that you want to score all the
deals with a keras model that will tell you if the deal is a fraud or not. In order to do that you
declare a new
[transformer that takes the data from](resource.md#feeding-data-from-one-resource-into-another) `deals`
resource and add it to the source.

```python
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
You can also set the resources in the source as follows
```python
source.deal_scores = source.deals | deal_scores
```
or
```python
source.resources["deal_scores"] = source.deals | deal_scores
```
:::note
When adding resource to the source, `dlt` clones the resource so your existing instance is not affected.
:::

### Reduce the nesting level of generated tables

You can limit how deep `dlt` goes when generating child tables. By default, the library will descend
and generate child tables for all nested lists, without limit.

```python
@dlt.source(max_table_nesting=1)
def mongo_db():
    ...
```

In the example above we want only 1 level of child tables to be generates (so there are no child
tables of child tables). Typical settings:

- `max_table_nesting=0` will not generate child tables at all and all nested data will be
  represented as json.
- `max_table_nesting=1` will generate child tables of top level tables and nothing more. All nested
  data in child tables will be represented as json.

You can achieve the same effect after the source instance is created:

```python
from mongo_db import mongo_db

source = mongo_db()
source.max_table_nesting = 0
```

Several data sources are prone to contain semi-structured documents with very deep nesting i.e.
MongoDB databases. Our practical experience is that setting the `max_nesting_level` to 2 or 3
produces the clearest and human-readable schemas.

### Modify schema

The schema is available via `schema` property of the source.
[You can manipulate this schema i.e. add tables, change column definitions etc. before the data is loaded.](schema.md#schema-is-modified-in-the-source-function-body)

Source provides two other convenience properties:

1. `max_table_nesting` to set the maximum nesting level of child tables
1. `root_key` to propagate the `_dlt_id` of from a root table to all child tables.

## Load sources

You can pass individual sources or list of sources to the `dlt.pipeline` object. By default, all the
sources will be loaded to a single dataset.

You are also free to decompose a single source into several ones. For example, you may want to break
down a 50 table copy job into an airflow dag with high parallelism to load the data faster. To do
so, you could get the list of resources as:

```python
# get a list of resources' names
resource_list = sql_source().resources.keys()

#now we are able to make a pipeline for each resource
for res in resource_list:
    pipeline.run(sql_source().with_resources(res))
```

### Do a full refresh

You can temporarily change the "write disposition" to `replace` on all (or selected) resources within
a source to force a full refresh:

```python
p.run(merge_source(), write_disposition="replace")
```

With selected resources:

```python
p.run(tables.with_resources("users"), write_disposition="replace")
```
