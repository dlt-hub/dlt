---
title: Naming Convention
description: Control how dlt creates table, column and other identifiers
keywords: [identifiers, snake case, case sensitive, case insensitive, naming]
---

# Naming Convention
`dlt` creates tables, child tables and column identifiers from the data. The data source,
typically JSON documents, contains identifiers (i.e. key names in a dictionary) with any Unicode
characters, any lengths and naming styles. On the other hand destinations accept very strict
namespaces for their identifiers. Like [Redshift](../dlt-ecosystem/destinations/redshift.md#naming-convention) that accepts case-insensitive alphanumeric
identifiers with maximum 127 characters.

`dlt` groups tables belonging to [resources](resource.md) from a single [source](source.md) in a [schema](schema.md).

Each schema contains **naming convention** that tells `dlt` how to translate identifiers to the
namespace that the destination understands. Naming conventions are in essence functions translating strings from the source identifier format into destination identifier format. For example our **snake_case** (default) naming convention will translate `DealFlow` into `deal_flow` identifier.

You have control over which naming convention to use and dlt provides a few to choose from ie. `sql_cs_v1`



* Each destination has a preferred naming convention.
* This naming convention is used when new schemas are created.
* Schemas preserve naming convention when saved
* `dlt` applies final naming convention in `normalize` stage. Naming convention comes from (1) explicit configuration (2) from destination capabilities. Naming convention
in schema will be ignored.
* You can change the naming convention in the capabilities: (name, case-folding, case sensitivity)

## Case sensitivity


## Default naming convention (snake_case)

1. Converts identifiers to **snake_case**, small caps. Removes all ascii characters except ascii
   alphanumerics and underscores.
1. Adds `_` if name starts with number.
1. Multiples of `_` are converted into single `_`.
1. The parent-child relation is expressed as double `_` in names.
1. It shorts the identifier if it exceed the length at the destination.

> 💡 Standard behavior of `dlt` is to **use the same naming convention for all destinations** so
> users see always the same tables and columns in their databases.

> 💡 If you provide any schema elements that contain identifiers via decorators or arguments (i.e.
> `table_name` or `columns`) all the names used will be converted via the naming convention when
> adding to the schema. For example if you execute `dlt.run(... table_name="CamelCase")` the data
> will be loaded into `camel_case`.

> 💡 Use simple, short small caps identifiers for everything!

## Set and adjust naming convention explicitly

## Configure naming convention

The naming convention is configurable and users can easily create their own
conventions that i.e. pass all the identifiers unchanged if the destination accepts that (i.e.
DuckDB).

## Avoid identifier clashes

## Available naming conventions

## Write your own naming convention
