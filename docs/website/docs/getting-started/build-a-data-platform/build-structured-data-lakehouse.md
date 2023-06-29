---
title: Build a structure data lake
description: How to build a data lake, with example dlt
keywords: [data lake, lakehouse, structured]
---

# What is a structured data lake?

A structured data lake is a data lake that contains data that is as structured as can be.

It's similar to a lakehouse concept except where in the lakehouse data is ingested raw, with `dlt` it
is possible to automate this first step and instead curate structured data directly.

In effect, this adds more structure and accelerates development by providing an easier to discover
and more robust raw layer.

### Unstructured data is like raw oil: Dirty

- As with data we just extracted, it needs to be “refined” for usage.
- Using it directly is known as “garbage in - garbage out” at best, failure to deliver at worst.

### Structured data is like the electrical grid: Ready to drive value

- You know what to expect, and it just works.
- The infrastructure ensures predictability in quality and availability.

![Structured data lake](/img/structured_lake.png)

# Should everything be structured?

Not all data can or should be structured.

### Which data should be structured?

All the data that we intend to use, and that can be structured without information loss, should
be structured. Since we can automate the structuring of data, we should do it as soon as we know we
will use it. If we don’t, then we are choosing to structure the data later manually, implicitly, in
a very time consuming and error prone way.

### Which data should stay unstructured?

Some types of data are natively more rich in a raw format. For example, raw text or images: To use
them, we would need to extract features from the data, which are essentially a subset of
information. So, if we want to retain all the info, we would need to keep the data in its
unstructured format.

### Which data should stay semi structured?

The data we do not intend to use. The only reason to store semi-structured data is that it’s
simple to store.

If we intend to simply store data that we may later decide to delete without looking into it, we do
not need to structure it. We can decide to structure it when we decide to look into it or use it.

If we intend to use data, given that automatic structuring is a small effort that much accelerates
and improves usage, we should structure the data before usage.

### How is this different from lakehouse?

- Similarly, a lakehouse also contains all types of data.
- Similarly, in a lakehouse business users use structured data.
- Differently, in a lakehouse the analysts may use unstructured data, and the curation of data may
  happen in an unstructured space. The structuring process may be manual and there may be implicit
  “schema on read” used.

# Solutions for building structured data lakes

## Delta lake framework

Delta lake framework offers the possibility to build a “lakehouse” or a collection of both structured
data that can be easily used, and unstructured data that may need extra processing.

The delta lake framework is a technology, not a paradigm, so you can build with it a structured data
lake, or a lakehouse with partly structured data.

### Delta lake: Progress towards **Schema Enforcement and Evolution**

Delta Lake provides schema enforcement, which helps prevent bad data from being written into the
lake by ensuring that the data matches the schema. It also enables schema evolution, allowing you to
change the schema of your data over time, which is particularly useful when working with
semi-structured data or when your data's structure changes over time.

Delta Lake provides schema enforcement, which helps prevent bad data from being written into the
lake by ensuring that the data matches the schema.

### What is Delta lake?

Delta Lake is an open-source storage layer that brings ACID (Atomicity, Consistency, Isolation,
Durability) transactions to Apache Spark and big data workloads. It was originally developed by
Databricks and is now a project under the Linux Foundation.

Delta Lake is designed to bring reliability to data lakes. It provides various features to handle
the challenges faced with data lakes, such as ensuring data integrity with ACID transactions, schema
enforcement, and schema evolution.

Key Features of Delta Lake:

1. **ACID Transactions**: Delta Lake provides ACID transactions that allow multiple writers to
   concurrently modify a dataset and also read consistent snapshots of data at the same time.
1. **Schema Enforcement and Evolution**: Delta Lake provides schema enforcement, which helps prevent
   bad data from being written into the lake by ensuring that the data matches the schema. It also
   enables schema evolution, allowing you to change the schema of your data over time, which is
   particularly useful when working with semi-structured data or when your data's structure changes
   over time.
1. **Time Travel (Data Versioning)**: Delta Lake provides snapshots of data, enabling developers to
   access and revert to earlier versions of data for audits, rollbacks, or to reproduce experiments.
1. **Scalable Metadata handling**: With Delta Lake, you can handle millions of files are they are
   stored in the form of Parquet, and transaction logs are used for tracking changes, which is much
   more scalable than simply storing data in a Hive Metastore.

### How does Delta lake relate to the other technologies? Python, Spark, SQL, Parquet, and Databricks

- **Databricks**: As the original creator of Delta Lake, Databricks provides a unified platform for
  data engineering and data science. Delta Lake is a key component of the Databricks Lakehouse
  Platform, which combines the best elements of data lakes and data warehouses.
- **Python and Spark**: Delta Lake integrates with Spark APIs, so you can use it with Python
  (PySpark), Scala, Java, and SQL languages. You can read and write data in the Delta format using
  the usual DataFrame APIs.
- **SQL**: Delta Lake supports standard SQL queries. You can use your existing BI tools to query
  Delta Lake tables.
- **Parquet**: Delta Lake uses Apache Parquet as its storage format. It adds a transaction log that
  keeps track of all changes to the data, which provides the ability to transact on the data and
  keep a version history.

Schema inference is a capability of Apache Spark. When you load data into a DataFrame, Spark can
automatically infer the schema based on the data. However, this is not always perfect, especially
for complex or semi-structured data. Delta Lake complements this by enforcing the schema when data
is written, which helps prevent inconsistencies and errors in your data.

## `dlt` python library enables structured lakes on common technologies

`dlt` is not a framework, meaning you can easily use it in various contexts, enabling it to be tech
agnostic.

In `dlt`’s paradigm, semi-structured data should always be structured.

- You can use `dlt` to directly ingest structured data.
- You can use it to structure any stored semi-structured or unstructured data before curation and
  usage.

### So to create a structured data lake with `dlt`?

- If you have unstructured data such as text or images for later structuring, store it in storage.
- If you have semi-structured data, let `dlt` structure it and manage schemas, and curate the
  structured data in your structured destination.
- Use [data evolution alerts](../../reference/explainers/schema-evolution) to curate the data:
  notify changes and curate it together with the producer and consumer, without the engineer.

# In conclusion

- Classic data lakes existed primarily due to tech limitations of the past - those limitations are
  now removed.
- Moving forward we should stop manually curating and structuring semi structured data, and let
  machines handle it.
- Humans should work with clean structured data whenever possible to ensure the best outcomes.
- Currently, delta lake framework and `dlt` library support this paradigm in the open source. Delta
  lake is Spark file centric, while `dlt` supports common destinations.
