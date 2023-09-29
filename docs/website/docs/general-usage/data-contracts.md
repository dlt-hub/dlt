---
title: Data Contracts
description: Data contracts and controlling schema evolution
keywords: [data contracts, schema, dlt schema, pydantic]
---

## Data contracts and controlling schema evolution

`dlt` will evolve the schema of the destination to accomodate the structure and data types of the extracted data. There are several settings
that you can use to control this automatic schema evolution, from the default settings where all changes to the schema are accepted to
a frozen schema that does not change at all. 

Consider this example:

```py
@dlt.resource(schema_contract={"tables": "evolve", "columns": "freeze"})
def items():
    ...
```

This resource will allow new subtables to be created, but will throw an exception if data is extracted for an existing table which 
contains a new column. 

The `schema_contract` exists on the `source` decorator as a directive for all resources of that source and on the 
`resource` decorator as a directive for the individual resource.  Additionally it exists on the `pipeline.run()` method, which will override all existing settings. 
The `schema_contract` is a dictionary with keys that control the following:

* `table` creating of new tables and subtables
* `columns` creating of new columns on an existing table
* `data_type` creating of new variant columns, which happens if a different datatype is discovered in the extracted data than exists in the schema

Each property can be set to one of three values:
* `freeze`: This will raise an exception if data is encountered that does not fit the existing schema, so no data will be loaded to the destination
* `discard_row`: This will discard any extracted row if it does not adhere to the existing schema, and this row will not be loaded to the destination. All other rows will be.
* `discard_value`: This will discard data in an extracted row that does not adhere to the existing schema and the row will be loaded without this data.

### Code Examples

The below code will silently ignore new subtables, allow new columns to be added to existing tables and raise an error if a variant of a column is discovered.

```py
@dlt.resource(schema_contract={"tables": "discard_row", "columns": "evolve", "data_type": "freeze"})
def items():
    ...
```

The below Code will raise on any encountered schema change. Note: You can always set a string which will be interpreted as though all keys are set to these values.

```py
pipeline.run(my_source(), schema_contract="freeze")
```

The below code defines some settings on the source which can be overwritten on the resource which in turn can be overwritten by the global override on the `run` method.
Here for all resources variant columns are frozen and raise an error if encountered, on `items` new columns are allowed but `other_items` inherits the `freeze` setting from
the source, thus new columns are frozen there. New tables are allowed.

```py
@dlt.resource(schema_contract={"columns": "evolve"})
def items():
    ...

@dlt.resource()
def other_items():
    ...

@dlt.source(schema_contract={"columns": "freeze", "data_type": "freeze"}):
def source():
  return [items(), other_items()]


# this will use the settings defined by the decorators
pipeline.run(source())

# this will freeze the whole schema, regardless of the decorator settings
pipeline.run(source(), schema_contract="freeze")

```