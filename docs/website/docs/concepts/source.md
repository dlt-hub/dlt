---
sidebar_position: 2
---

# Source

A [source](../glossary.md#source) contains:
- API methods for authentication, pagination
- Resources are functions that produce data. Resources are to sources what endpoints are to an API.
- workflow and customisation code.

A source is the piece of code that produces data. A source might typically be built around a single api. It contains resources, which are data prod

A source contains the **resources**, `source_name().resources` produces a dictionary of resources with the name as key. This allows you to select and execute them individually or in your preferred patterns.

For example, you may want to break down a 50 table copy job into an airflow dag with high parallelism to load the data faster. To do so, you could get the list of resources as

```python
# get a list of resources' names
resource_list = sql_source().resources.keys()

#now we are able to make a pipeline for each resource
for res in resource_list:
		pipeline.run(sql_source().with_resources(res))
```