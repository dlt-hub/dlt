---
title: 🧪 Schema and data contracts
description: Controlling schema evolution and validating data
keywords: [data contracts, schema, dlt schema, pydantic]
---

`dlt` will evolve the schema at the destination by following the structure and data types of the extracted data. There are several modes
that you can use to control this automatic schema evolution, from the default modes where all changes to the schema are accepted to
a frozen schema that does not change at all.

Consider this example:

```py
@dlt.resource(schema_contract={"tables": "evolve", "columns": "freeze"})
def items():
    ...
```

This resource will allow new tables (both nested tables and [tables with dynamic names](resource.md#dispatch-data-to-many-tables)) to be created, but will throw an exception if data is extracted for an existing table which contains a new column.

### Setting up the contract
You can control the following **schema entities**:
* `tables` - the contract is applied when a new table is created
* `columns` - the contract is applied when a new column is created on an existing table
* `data_type` - the contract is applied when data cannot be coerced into a data type associated with an existing column.

You can use **contract modes** to tell `dlt` how to apply the contract for a particular entity:
* `evolve`: No constraints on schema changes.
* `freeze`: This will raise an exception if data is encountered that does not fit the existing schema, so no data will be loaded to the destination.
* `discard_row`: This will discard any extracted row if it does not adhere to the existing schema, and this row will not be loaded to the destination.
* `discard_value`: This will discard data in an extracted row that does not adhere to the existing schema, and the row will be loaded without this data.

:::note
The default mode (**evolve**) works as follows:
1. New tables may always be created.
2. New columns may always be appended to the existing table.
3. Data that do not coerce to the existing data type of a particular column will be sent to a [variant column](schema.md#variant-columns) created for this particular type.
:::

#### Passing the schema_contract argument
The `schema_contract` exists on the [dlt.source](source.md) decorator as a default for all resources in that source and on the
[dlt.resource](source.md) decorator as a directive for the individual resource - and as a consequence - on all tables created by this resource.
Additionally, it exists on the `pipeline.run()` method, which will override all existing settings.

The `schema_contract` argument accepts two forms:
1. **full**: a mapping of schema entities to contract modes
2. **shorthand**: a contract mode (string) that will be applied to all schema entities.

For example, setting `schema_contract` to *freeze* will expand to the full form:
```py
{"tables": "freeze", "columns": "freeze", "data_type": "freeze"}
```

You can change the contract on the **source** instance via the `schema_contract` property. For **resource**, you can use [apply_hints](resource#set-table-name-and-adjust-schema).


#### Nuances of contract modes
1. Contracts are applied **after names of tables and columns are normalized**.
2. A contract defined on a resource is applied to all root tables and nested tables created by that resource.
3. `discard_row` works on the table level. For example, if you have two tables in a nested relationship, i.e., *users* and *users__addresses*, and the contract is violated in the *users__addresses* table, the row of that table is discarded while the parent row in the *users* table will be loaded.

### Use Pydantic models for data validation

Pydantic models can be used to [define table schemas and validate incoming data](resource.md#define-a-schema-with-pydantic). You can use any model you already have. `dlt` will internally synthesize (if necessary) new models that conform to the **schema contract** on the resource.

Just passing a model in the `column` argument of the [dlt.resource](resource.md#define-a-schema-with-pydantic) sets a schema contract that conforms to the default Pydantic behavior:
```py
{
  "tables": "evolve",
  "columns": "discard_value",
  "data_type": "freeze"
}
```
New tables are allowed, extra fields are ignored, and invalid data raises an exception.

If you pass a schema contract explicitly, the following happens to schema entities:
1. **tables** do not impact the Pydantic models.
2. **columns** modes are mapped into the **extra** modes of Pydantic (see below). `dlt` will apply this setting recursively if models contain other models.
3. **data_type** supports the following modes for Pydantic: **evolve** will synthesize a lenient model that allows for any data type. This may result in variant columns upstream.
**freeze** will re-raise `ValidationException`. **discard_row** will remove the non-validating data items.
**discard_value** is not currently supported. We may eventually do that in Pydantic v2.

`dlt` maps column contract modes into the extra fields settings as follows.

Note that this works in two directions. If you use a model with such a setting explicitly configured, `dlt` sets the column contract mode accordingly. This also avoids synthesizing modified models.

| column mode   | pydantic extra |
| ------------- | -------------- |
| evolve        | allow          |
| freeze        | forbid         |
| discard_value | ignore         |
| discard_row   | forbid         |

`discard_row` requires additional handling when a ValidationError is raised.

:::tip
Model validation is added as a [transform step](resource.md#filter-transform-and-pivot-data) to the resource. This step will convert the incoming data items into instances of validating models. You could easily convert them back to dictionaries by using `add_map(lambda item: item.dict())` on a resource.
:::

:::note
Pydantic models work on the **extracted** data **before names are normalized or nested tables are created**. Make sure to name model fields as in your input data and handle nested data with nested models.

As a consequence, `discard_row` will drop the whole data item - even if a nested model was affected.
:::

### Set contracts on Arrow tables and Pandas

All contract settings apply to [arrow tables and panda frames](../dlt-ecosystem/verified-sources/arrow-pandas.md) as well.
1. **tables** mode is the same - no matter what the data item type is.
2. **columns** will allow new columns, raise an exception, or modify tables/frames still in the extract step to avoid rewriting Parquet files.
3. **data_type** changes to data types in tables/frames are not allowed and will result in a data type schema clash. We could allow for more modes (evolving data types in Arrow tables sounds weird but ping us on Slack if you need it.)

Here's how `dlt` deals with column modes:
1. **evolve** new columns are allowed (the table may be reordered to put them at the end).
2. **discard_value** the column will be deleted.
3. **discard_row** rows with the column present will be deleted and then the column will be deleted.
4. **freeze** an exception on a new column.

### Get context from DataValidationError in freeze mode
When a contract is violated in freeze mode, `dlt` raises a `DataValidationError` exception. This exception provides access to the full context and passes the evidence to the caller.
As with any other exception coming from a pipeline run, it will be re-raised via a `PipelineStepFailed` exception, which you should catch in an except block:

```py
try:
  pipeline.run()
except PipelineStepFailed as pip_ex:
  if pip_ex.step == "normalize":
    if isinstance(pip_ex.__context__.__context__, DataValidationError):
      ...
  if pip_ex.step == "extract":
    if isinstance(pip_ex.__context__, DataValidationError):
      ...
```

`DataValidationError` provides the following context:
1. `schema_name`, `table_name`, and `column_name` provide the logical "location" at which the contract was violated.
2. `schema_entity` and `contract_mode` indicate which contract was violated.
3. `table_schema` contains the schema against which the contract was validated. It may be a Pydantic model or a dlt `TTableSchema` instance.
4. `schema_contract` is the full, expanded schema contract.
5. `data_item` is the causing data item (Python dict, arrow table, Pydantic model, or list thereof).

### Contracts on new tables
If a table is a **new table** that has not been created on the destination yet, dlt will allow the creation of new columns. For a single pipeline run, the column mode is changed (internally) to **evolve** and then reverted back to the original mode. This allows for initial schema inference to happen, and then on subsequent runs, the inferred contract will be applied to the new data.

The following tables are considered new:
1. Child tables inferred from nested data.
2. Dynamic tables created from the data during extraction.
3. Tables containing **incomplete** columns - columns without a data type bound to them.

For example, such a table is considered new because the column **number** is incomplete (defined as primary key and NOT null but no data type):
```yaml
blocks:
  description: Ethereum blocks
  write_disposition: append
  columns:
    number:
      nullable: false
      primary_key: true
      name: number
```

Tables that are not considered new:
1. Those with columns defined by Pydantic models.

### Working with datasets that have manually added tables and columns on the first load

In some cases, you might be working with datasets that have tables or columns created outside of dlt. If you are loading to a table not created by dlt for the first time, dlt will not know about this table while enforcing schema contracts. This means that if you do a load where the `tables` are set to `evolve`, all will work as planned. If you have `tables` set to `freeze`, dlt will raise an exception because it thinks you are creating a new table (which you are from dlt's perspective). You can allow `evolve` for one load and then switch back to `freeze`.

The same thing will happen if `dlt` knows your table, but you have manually added a column to your destination and you have `columns` set to `freeze`.

### Code examples

The below code will silently ignore new subtables, allow new columns to be added to existing tables, and raise an error if a variant of a column is discovered.

```py
@dlt.resource(schema_contract={"tables": "discard_row", "columns": "evolve", "data_type": "freeze"})
def items():
    ...
```

The below code will raise an error on any encountered schema change. Note: You can always set a string which will be interpreted as though all keys are set to these values.

```py
pipeline.run(my_source(), schema_contract="freeze")
```

The below code defines some settings on the source which can be overwritten on the resource, which in turn can be overwritten by the global override on the `run` method.
Here, for all resources, variant columns are frozen and raise an error if encountered. On `items`, new columns are allowed, but `other_items` inherits the `freeze` setting from
the source, thus new columns are frozen there. New tables are allowed.

```py
@dlt.resource(schema_contract={"columns": "evolve"})
def items():
    ...

@dlt.resource()
def other_items():
    ...

@dlt.source(schema_contract={"columns": "freeze", "data_type": "freeze"})
def source():
  return [items(), other_items()]


# this will use the settings defined by the decorators
pipeline.run(source())

# this will freeze the whole schema, regardless of the decorator settings
pipeline.run(source(), schema_contract="freeze")

```

