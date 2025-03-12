---
title: Transformations tutorial
description: Using the dlt+ transformations to transform data
keywords: [transformations, dlt+, project]
---

### Topics covered in this tutorial

This tutorial introduces you to the dlt+ transformations basics. You will learn how to:

* transform existing tables with ibis expressions
* sync datasets, remote to local, local to remote or remote to remote
* use a local cache to run transformations across multiple datasets
* retain full column level lineage during syncs and transformations

### Topics covered in a future tutorial

* Incremental transformations
* Incremental and partial syncing
* dbt based transformations
* transformations dlt+ project and CLI integration

## Prerequisites

To follow this tutorial, make sure that:

- dlt+ is set up according to the [installation guide](installation.md)
- ibis-framework is installed in your environment
- you're familiar with the [core concepts of dlt](../../reference/explainers/how-dlt-works.md)
- you're familiar with our read access based on [ibis expressions](../../general-usage/dataset-access/dataset#modifying-queries-with-ibis-expressions)


## Introductory notes

To work through this tutorial, we will be using multiple local DuckDB instances to transform and sync data between them. In the real world, you will be able to use any destination/dataset that supports ibis expressions. We will also create an example dataset with a couple of tables to work with to demonstrate how you can make use of transformations.

## Our example dataset

Copy the pipeline below and run it in your environment:

<details>

<summary>Pipeline code</summary>


```python

import dlt
import dlt_plus
from dlt.common.destination.dataset import SupportsReadableDataset


# Cities data
@dlt.resource(write_disposition="replace")
def offices():
    yield [
        {"id": 1, "city": "New York"},
        {"id": 2, "city": "Berlin"},
    ]

@dlt.resource(write_disposition="replace")
def departments():
    yield [
        {"id": 1, "department": "Engineering"},
        {"id": 2, "department": "Marketing"},
    ]

# Employee data
@dlt.resource(write_disposition="replace")
def employees():
    yield [
        {"id": 1, "name": "Alice", "department_id": 1, "office_id": 1},
        {"id": 2, "name": "Bob", "department_id": 2, "office_id": 2},
        {"id": 3, "name": "Charlie", "department_id": 1, "office_id": 2},
        {"id": 4, "name": "Diana", "department_id": 2, "office_id": 1},
    ]

# adds a custom hint to the name column, we will use this to test lineage later
employees.apply_hints(columns={"name": {"x-pii": True}})  # type: ignore

# Payroll data
@dlt.resource(write_disposition="replace")
def payroll():
    yield [
        {"id": 1, "employee_id": 1, "month": "2024-01", "amount": 7000},
        {"id": 2, "employee_id": 1, "month": "2024-02", "amount": 7100},
        {"id": 3, "employee_id": 2, "month": "2024-01", "amount": 5500},
        {"id": 4, "employee_id": 2, "month": "2024-02", "amount": 5600},
        {"id": 5, "employee_id": 3, "month": "2024-01", "amount": 5000},
        {"id": 6, "employee_id": 3, "month": "2024-02", "amount": 5100},
        {"id": 7, "employee_id": 4, "month": "2024-01", "amount": 6000},
        {"id": 8, "employee_id": 4, "month": "2024-02", "amount": 6100},
    ]

# Create a DLT source with three resources
@dlt.source
def employee_data():
    return offices(), employees(), payroll(), departments()

# Create and run pipeline
company_pipeline = dlt.pipeline(
    pipeline_name="company_pipeline",
    destination="duckdb",
    dataset_name="company_dataset"
)

# Execute pipeline
info = company_pipeline.run(employee_data())
print(info)
```
</details>

We have a straightforward company database with live employee master data containing names and payroll payments, as well as two tables for offices and departments. As always, you can inspect the contents of the dataset by running `dlt pipeline company_pipeline show` to open the dlt streamlit app.


## A first few transformations 

Transformations work very similarly to regular dlt.resources, which they are under the hood, and can be grouped into a regular `dlt.source`. Transformations take a populated dataset as input to build transformations from this source and move the computed data into the same or another dataset. If data stays within the same physical destination, you can execute the transformation as a SQL statement and no data needs to be moved to the machine where the dlt pipeline is running. For all other cases, data will be extracted the same way as with a regular dlt pipeline.

Let's move some data from the company dataset into a fictional warehouse dataset. Let's assume we want the employee master data to be available in the warehouse dataset. We want to make sure not to include employee names in the warehouse dataset for privacy reasons.

```python

# get the company dataset
company_dataset = company_pipeline.dataset()

# print the employee table without the name column as arrow table
print(company_dataset.employees.drop("name").arrow())

# create the above select as a transformation in the warehouse dataset
@dlt_plus.transform(transformation_type="python")
def cleaned_employees(dataset: SupportsReadableDataset):
    return dataset.employees.drop("name")

# create the warehouse pipeline and run it:
warehouse_pipeline = dlt.pipeline(
    pipeline_name="warehouse_pipeline",
    destination="duckdb",
    dataset_name="warehouse_dataset"
)

# run the transformation
info = warehouse_pipeline.run(cleaned_employees(company_dataset))
print(info)

```

You can now inspect your warehouse pipeline with the dlt Streamlit app by running `dlt pipeline warehouse_pipeline show`. You should see the cleaned_employees table in the warehouse dataset, which is missing the name column.

What is happening here?

* We have defined one new transformation function with the `dlt.transform` decorator. This function does not yield rows but returns a single object. This can be a SQL query as a string, but preferably it is a ReadableIbisRelation object that can be converted into a SQL query by dlt. You can use all ibis expressions detailed in the dlt oss docs. Note that you do not need to execute the query by running `.arrow()` or `.iter_df()` or something similar on it.

* The resulting table name is derived from the function name but may be set with the `table_name` parameter, like the `dlt.resource`.

* For now, we always need to set a transformation type. This can be `python` or `sql`. `sql` transformations will be executed directly as SQL on the dataset and are only available for transformations where the source_dataset and destination_dataset are the same or on the same physical destination. More on this below.

* All other resource parameters are available on the dlt.transform decorator, like `write_disposition`, `primary_key`, `columns`, etc. We will cover these in more detail below or in another tutorial.

* Running the transformations is just like a regular pipeline run - you will have a complete schema and entries in the dlt_loads table.


Here are a few more examples to illustrate what can be done with transformations using our company and warehouse pipelines:

### Copy multiple tables at once into the warehouse dataset

Copy two tables and clean up employee data for privacy:

```python

@dlt_plus.transform(transformation_type="python")
def copied_employees(dataset: SupportsReadableDataset):
    return dataset.employees.drop("name")

@dlt_plus.transform(transformation_type="python")
def copied_payroll(dataset: SupportsReadableDataset):
    return dataset.payroll

# we can group transformnations into a regular source
def copy_source(dataset: SupportsReadableDataset):
    return copied_employees(dataset), copied_payroll(dataset)

# create the warehouse pipeline and run it:
warehouse_pipeline.run(copy_source(company_dataset))

```

### Create a new joined and aggregated table

We want a table in the warehouse that shows how many payments were made per employee in total. Fully in ibis expressions.

```python

@dlt_plus.transform(transformation_type="python")
def payment_per_employee(dataset: SupportsReadableDataset):
    employee_tables = dataset.employees
    payroll_table = dataset.payroll
    joined_table = employee_tables.join(payroll_table, employee_tables.id == payroll_table.employee_id)
    return joined_table.group_by(joined_table.employee_id).aggregate(total_payments=joined_table.amount.sum())
    

# create the warehouse pipeline and run it:
warehouse_pipeline.run(payment_per_employee(company_dataset))
```


Inspect your warehouse dataset with Streamlit - you will see the total amount per employee_id.

### Create aggregated tables in the same dataset

You can run a transformation against the same dataset that the source data comes from. This might be useful if you need some pre-computed stats (similar to a materialized view),
or you need to do some cleanup after a load on the same dataset. Let's use the same transformation as above plus another one in a group. Since the source and destination datasets are the same, we can use SQL transformations, which are much faster.

```python

@dlt_plus.transform(transformation_type="sql", write_disposition="replace")
def payment_per_employee_sql(dataset: SupportsReadableDataset):
    employee_tables = dataset.employees
    payroll_table = dataset.payroll
    joined_table = employee_tables.join(payroll_table, employee_tables.id == payroll_table.employee_id)
    return joined_table.group_by(joined_table.employee_id).aggregate(total_payments=joined_table.amount.sum())

@dlt_plus.transform(transformation_type="python", write_disposition="replace")
def rows_per_table(dataset: SupportsReadableDataset):
    # this is a speical relation on the dataset which computes row_counts
    return dataset.row_counts()


# we can group transformnations into a regular source
def stats_source(dataset: SupportsReadableDataset):
    return rows_per_table(dataset), payment_per_employee_sql(dataset)

# now run this transformation on the SAME pipeline
company_pipeline.run(stats_source(company_dataset))
```

Check the company pipeline to see the aggregated tables in the same dataset.

:::tip
For the row_counts transformation, we use the Python transformation type, as dlt+ currently cannot compute the needed column hints for this table. More on SQL transformations below. You can observe that you can easily mix Python and SQL transformations in the same source.
:::


## A star schema example

A common use case is to create a star schema with a fact table and a few dimension tables where each important column is at most one join away from the fact table. Let's create a star schema for our company dataset with payroll payments as facts and make the office and department tables directly joinable from the fact table:

```python

# let's create a new star warehouse for simplicity
star_pipeline = dlt.pipeline(
    pipeline_name="star_pipeline",
    destination="duckdb",
    dataset_name="star_dataset"
)

# the dimension tables can just be copied over, again we clean the personal data from the employees table

@dlt_plus.transform(transformation_type="python")
def employees(dataset: SupportsReadableDataset):
    return dataset.employees.drop("name")

@dlt_plus.transform(transformation_type="python")
def departments(dataset: SupportsReadableDataset):
    return dataset.departments

@dlt_plus.transform(transformation_type="python")
def offices(dataset: SupportsReadableDataset):
    return dataset.offices

# create the fact table from the payroll table with some joins
@dlt_plus.transform(transformation_type="python")
def payroll_facts(dataset: SupportsReadableDataset):
    employee_tables = dataset.employees
    payroll_table = dataset.payroll
    joined_table = employee_tables.join(payroll_table, employee_tables.id == payroll_table.employee_id)
    # we select the relevant columns, making department and office id directly available
    return joined_table.select(
        payroll_table.id,
        payroll_table.employee_id,
        payroll_table.month,
        payroll_table.amount,
        employee_tables.department_id,
        employee_tables.office_id
    )

@dlt.source()
def star_source(dataset: SupportsReadableDataset):
    return payroll_facts(dataset),employees(dataset), departments(dataset), offices(dataset)


# create the star schema in the star warehouse
info = star_pipeline.run(star_source(company_dataset))
print(info)

# now that we have a nice star schema, presumable in a column oriented warehouse, we can aggregate
# payments per department with only a single join:
star_dataset = star_pipeline.dataset()
fact_table = star_dataset.payroll_facts
office_table = star_dataset.offices

joined_table = fact_table.join(office_table, fact_table.office_id == office_table.id)
print(joined_table.group_by(office_table.city).aggregate(total_payments=joined_table.amount.sum()).arrow())


```

:::tip
For simply copying table data, you can also use the sync source demonstrated below. Also note that in future versions of the dlt+ transformation framework, there will be a template generator to create a star schema from a given dataset to give you a starting point.
:::


## Lineage

dlt+ transformations contain a lineage engine that can trace the origin of columns resulting from transformations. You may have noticed that we added a custom hint to the name column in the employees table at the beginning of the page. This hint is a custom hint that we decided to add to all columns containing very sensitive data. Ideally, we would like to know which columns in a result are derived from columns containing sensitive data. dlt+ lineage will do just that for you. Let's run an aggregated join query into our warehouse again, but this time we will not drop the name column:

```python

@dlt_plus.transform(transformation_type="python")
def payment_per_employee(dataset: SupportsReadableDataset):
    employee_tables = dataset.employees
    payroll_table = dataset.payroll
    joined_table = employee_tables.join(payroll_table, employee_tables.id == payroll_table.employee_id)
    return joined_table.group_by(joined_table.employee_id, joined_table.name).aggregate(total_payments=joined_table.amount.sum())

# run the transformation
info = warehouse_pipeline.run(payment_per_employee(company_dataset))
print(info)

```

You can now inspect the schema of the warehouse and see that the name column of the aggregated table is also marked with our hint: `uv run dlt pipeline warehouse_pipeline schema`. dlt+ uses the same mechanism to derive precision and scale information for columns.

You can control the lineage mode with the `lineage_mode` parameter on the dlt.transform decorator, which can be set to `strict`, `best_effort`, or `disabled`. The default is `best_effort`, which will try to trace the lineage for each column and set the derived hints but will not fail if this is not possible. SQL-based transformations that introduce non-traceable columns will require you to supply column type hints for any columns that could not be traced. For Python-based transformations, the column type can be derived from the incoming data. In `strict` mode, any transformation that introduces non-traceable columns will fail with a useful error. This mode is recommended if you must ensure which columns may contain sensitive data and need to prevent them from being unmarked. You can also disable lineage for a specific transformation by setting the `lineage_mode` to `disabled`. In this mode, the full schema will be derived from the incoming data and no additional hints will be added to the resulting dataset.

### Untraceable columns

In a couple of scenarios the current implementation is not able to trace the lineage of a column. In some cases you may opt to add a static value to an expression, which is not traceable by definitions. We are planning to allow to add exceptions to the lineage tracing for cases like these. Another case is where a column is the result of some operation on multiple source columns, such as concatenating the value of two separate columns or mathematical operations that use more than one source column. These cases are still being worked on and will be supported in future versions of dlt+.

### Some examples from our company dataset

```python

# the following transformation will fail because the static_column is not traceable
@dlt_plus.transform(transformation_type="python", lineage_mode="strict")
def mutated_transformation(dataset: SupportsReadableDataset):
    return dataset.employees.mutate(static_column=1234)

# we can switch it to best effort mode, which will work
@dlt_plus.transform(transformation_type="python", lineage_mode="best_effort")
def mutated_transformation(dataset: SupportsReadableDataset):
    return dataset.employees.mutate(static_column=1234)

# if we use best effort but switch the transformation type to sql, it will fail
@dlt_plus.transform(transformation_type="sql", lineage_mode="best_effort")
def mutated_transformation(dataset: SupportsReadableDataset):
    return dataset.employees.mutate(static_column=1234)

# we can supply a column hint for the static_column, which will work for sql transformations
@dlt_plus.transform(transformation_type="sql", lineage_mode="best_effort", column_hints={"static_column": {"type": "double"}})
def mutated_transformation(dataset: SupportsReadableDataset):
    return dataset.employees.mutate(static_column=1234)

```

## SQL vs Python transformations

As you have seen above, you can choose to execute any transformation either as a Python or SQL transformation. For both transformation types, the basis is a decorated function that returns something that can be evaluated into a SQL query, either a string or a ReadableIbisRelation object. Both transformation types can be mixed and they both result in new tables in the destination dataset with a schema derived from the source dataset. The key differences are:

Python transformations
* Can be run from any dataset to any destination dataset, you only need to ensure that the destination supports all column types you are using in the transformation or clean them up
* Will run the select query on the source dataset but extract all resulting rows into the pipeline as Arrow tables / Parquet files and push them into the destination
* Chunk size for Arrow table extract can be set with the `chunk_size` parameter on the dlt.transform decorator: `@dlt.transform(transformation_type="python", chunk_size=1000)`
* When instantiated, behaves like a regular dlt.resource - you can pipe it into a dlt.transformer, add filters and maps with 'add_filter' and 'add_map' methods
* Is generally much slower than pure SQL transformations
* Will also work if not all resulting columns can be traced to origin columns (unless specified otherwise, more on lineage below)

SQL transformations
* Can only be run if the source and destination dataset are the same or on the same physical destination. Currently there is no proper check, so using SQL in a place where you should not will result in an obscure error
* Are much faster than Python transformations and should be used if the use case allows for it
* Only work if the full resulting schema is known ahead - this means lineage must pass in strict mode. Alternatively, you can supply column hints for columns that lineage cannot trace to the origin. This is the case, for example, if you add a static column in a .mutate() call on a ReadableIbisRelation
* You cannot pipe the transformation into a dlt.transformer or do any kind of other data manipulation on dataitems, since the data never leaves the database


## Sync Source
As you have observed above, there are cases where you essentially would like to copy a table from one dataset to another but do not really need to build any SQL expressions, because you do not want to filter or join. Let's say you simply want to copy the full company dataset into the warehouse dataset without any kind of data cleanup - you can very easily do this with the sync source. Under the hood, the sync source is a regular dlt source with a list of `dlt.transform` objects that run this sync.

```python
from dlt_plus.transform.sync_source import sync_source

# get a full sync source from the company pipeline
s = sync_source(company_pipeline.dataset())

# run it on the star pipeline
info = star_pipeline.run(s)
print(info)
```

Creating a sync source from any dataset without additional parameters will copy all rows of all tables and retain the full lineage of the data. Lineage is set to `strict` mode. If you want to copy just a few tables, you can do so by passing a list of tables to the sync source:

```python
# copy only the employees and payroll tables
s = sync_source(company_pipeline.dataset(), config={"tables": ["employees", "payroll"]})
star_pipeline.run(s)
```

Renaming tables while syncing is also possible by passing a mapping of old table names to new table names in the config:

```python
# rename the employees table to employee_data during the sync
s = sync_source(company_pipeline.dataset(), config={"tables": {"copied_employee_table": "employees"}})
star_pipeline.run(s)
```

You can also switch the synching to a `sql` based sync if you are copying a table into the same dataset or one on the same destination:

```python
# This copies the employees table into a new table called copied_employee_table on the same dataset
s = sync_source(company_pipeline.dataset(), config={"transformation_type": "sql", "tables": {"copied_employee_table": "employees"}})
company_pipeline.run(s)
```

:::tip
Incremental support for syncing, which is an essential feature for incremental data pipelines, is still in development and will be available in future versions of the dlt+ transformation framework. Support for column subselection and column renaming will also be included.
:::


## Homework: Combining data from multiple datasets in a local cache

If you are excited about dlt transformations, you can try to build an advanced case that simulates combining data from multiple databases in a local cache and then pushing it to a warehouse. You can use duckdb destinations for all of these and write one long script that does all the steps in succession.

Let's say we have a secure database with employee and payroll data and a company-wide public database with office and departmental information. Your goal as a data engineer is to audit the spending of each department by joining the payroll data with the department and office information in a secure way and pushing the result to a warehouse dataset.

Here is what you should do:

* Take the company database example from above, but load the data into two different datasets: one dataset called "secure" where the employee and payroll data is loaded, and one called "public" where the departments and offices are loaded. This is what you are given by the CEO.

* Create a local cache dataset that you can use to combine the data from the secure and public datasets.

* Sync the secure and public datasets into the local cache dataset. You could already strip the personal data from the employees table here; it is up to you.

* Run SQL-based transformations on the local dataset to create the desired audit data (you can come up with anything that joins tables from the two original datasets).

* Push the resulting audit data into a warehouse dataset of your choice.


