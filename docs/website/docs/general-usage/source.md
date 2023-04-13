---
title: Source
description: Explanation of what a dlt source is
keywords: [source, api, dlt.source]
---


# Source

A [source](general-usage/glossary.md#source) is a logical grouping of resources ie. endpoints of a single API. The most common approach is to define it in a separate Python module.
- a source is a function decorated with `@dlt.source` that returns one or more resources
- a source can optionally define a [schema](./schema.md) with tables, columns, performance hints and more
- the source Python module typically contains optional customizations and data transformations.
- the source Python module typically contains the authentication and pagination code for particular API

## Declare sources
You declare source by decorating a function returning one or more resource with `dlt.source`. Our [Create a pipeline](../walkthroughs/create-a-pipeline.md) walkthrough teaches you how to do that.

### Create resources dynamically
You can create resources by using `dlt.resource` as a function. In an example below we reuse a single generator function to create a list of resources for several Hubspot endpoints.

```python

@dlt.source
def hubspot(api_key=dlt.secrets.value):

    endpoints = ["companies", "deals", "product"]

    def get_resource(endpoint):
        yield requests.get(url + "/" + endpoint).json()

    for endpoint in endpoints:
        # calling get_resource creates generator, the actual code of the function will be executed in pipeline.run
        yield dlt.resource(get_resource(endpoint), name=endpoint)
```
### Attach and configure schemas
You can [create, attach and configure schema](schema.md#attaching-schemas-to-sources) that will be used when loading the source.

## Customize sources

### Access and select resources to load
You can access resources present in a source and select which of them you want to load. In case of `hubspot` resource above we could select and load "companies", "deals" and "products" resources:
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
Resources can be individually accessed and selected
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
You can modify and filter data in resources, for example if we want to keep only deals after certain date:
```python
source.deals.add_filter(lambda deal: deal["created_at"] > yesterday)
```
Find more on transforms [here](resource.md#filter-transform-and-pivot-data)

### Add more resources to existing source
You can add a custom resource to source after it was created. Imagine that you want to score all the deals with a keras model that will tell you if the deal is a fraud or not. In order to do that you declare a new [resource that takes the data from](resource.md#feeding-data-from-one-resource-into-another) `deals` resource and add it to the source.
```python
import dlt
from hubspot import hubspot

source = hubspot()

@dlt.transformer(data_from=source.deals)
def deal_scores(deal_item):
  # obtain the score, deal_items contains data yielded by source.deals
  score = model.predict(featurize(deal_item))
  yield {"deal_id": deal_item, "score": score}

# add the deal_scores to the source
source.deal_scores = deal_scores
source.resources["deal_scores"] = deal_scores  # this also works
# load the data: you'll see the new table `deal_scores` in your destination!
pipeline.run(source)
```

### Modify schema
The schema is available via `schema` property of the source. You can manipulate this schema ie. add tables, change column definitions etc. before the data is loaded. Source provides two other convenience properties:
1. `max_table_nesting` to set the maximum nesting level of child tables
2. `root_key` to propagate the `_dlt_id` of from a root table to all child tables.

## Load sources
You can pass individual sources or list of sources to the `dlt.pipeline` object. By default all the sources will be loaded to a single dataset.

You are also free to decompose a single source into several ones. For example, you may want to break down a 50 table copy job into an airflow dag with high parallelism to load the data faster. To do so, you could get the list of resources as

```python
# get a list of resources' names
resource_list = sql_source().resources.keys()

#now we are able to make a pipeline for each resource
for res in resource_list:
		pipeline.run(sql_source().with_resources(res))
```

### Do a full refresh
You can temporarily change the write disposition to `replace` on all (or selected) resources within a source to force a full refresh:
```python
p.run(merge_source(), write_disposition="replace")
```
With selected resources:
```python
p.run(tables.with_resources("users"), write_disposition="replace")
```
