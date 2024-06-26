---
title: Naming Convention
description: Control how dlt creates table, column and other identifiers
keywords: [identifiers, snake case, case sensitive, case insensitive, naming]
---

# Naming Convention
`dlt` creates table and column identifiers from the data. The data source that ie. a stream of JSON documents may have identifiers (i.e. key names in a dictionary) with any Unicode characters, of any length and naming style. On the other hand, destinations require that you follow strict rules when you name tables, columns or collections.
A good example is [Redshift](../dlt-ecosystem/destinations/redshift.md#naming-convention) that accepts case-insensitive alphanumeric identifiers with maximum 127 characters.

`dlt` groups tables from a single [source](source.md) in a [schema](schema.md).

Each schema defines **naming convention** that tells `dlt` how to translate identifiers to the
namespace that the destination understands. Naming conventions are in essence functions that map strings from the source identifier format into destination identifier format. For example our **snake_case** (default) naming convention will translate `DealFlow` into `deal_flow` identifier.

You can pick which naming convention to use. `dlt` provides a few to [choose from](#available-naming-conventions) or you can [easily add your own](#write-your-own-naming-convention).

:::tip
* Standard behavior of `dlt` is to **use the same naming convention for all destinations** so users see always the same tables and column names in their databases.
* Use simple, short small caps identifiers for everything so no normalization is needed
:::

### Use default naming convention (snake_case)
`dlt` most used and tested with default, case insensitive, lower case naming convention called **snake_case**

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

:::tip
If you do not like **snake_case** your next safe option is **sql_ci** which generates SQL-safe, lower-case, case-insensitive identifiers without any
other transformations. To permanently change the default naming convention on a given machine:
1. set an environment variable `SCHEMA__NAMING` to `sql_ci_v1` OR
2. add the following line to your global `config.toml` (the one in your home dir ie. `~/.dlt/config.toml`)
```toml
[schema]
naming="sql_ci_v1"
```
:::

## Source identifiers vs destination identifiers
### Pick the right identifier form when defining resources
`dlt` keeps source (not normalized) identifiers during data [extraction](../reference/explainers/how-dlt-works.md#extract) and translates them during [normalization](../reference/explainers/how-dlt-works.md#normalize). For you it means:
1. If you write a [transformer](resource.md#process-resources-with-dlttransformer) or a [mapping/filtering function](resource.md#filter-transform-and-pivot-data), you will see the original data, without any normalization. Use the source key names to access the dicts!
2. If you define a `primary_key` or `cursor` that participate in [incremental loading](incremental-loading.md#incremental-loading-with-a-cursor-field) use the source identifiers (as `dlt` will inspect the source data).
3. When defining any other hints ie. `columns` or `merge_key` you can pick source or destination identifiers. `dlt` normalizes all hints together with your data.
4. `Schema` object (ie. obtained from the pipeline or from `dlt` source via `discover_schema`) **always contains destination (normalized) identifiers**.

In the snippet below, we define a resource with various "illegal" unicode characters in table name and other hint and demonstrate how they get normalized in the schema object.
```py
```

### Understand the identifier normalization
Identifiers are translated from source to destination form in **normalize** step. Here's how `dlt` picks the right naming convention:

* Each destination has a preferred naming convention.
* This naming convention is used when new schemas are created.
* Schemas preserve naming convention when saved
* `dlt` applies final naming convention in `normalize` step. Naming convention comes from (1) explicit configuration (2) from destination capabilities. Naming convention
in schema will be ignored.
* You can change the naming convention in the capabilities: (name, case-folding, case sensitivity)

### Case sensitive and insensitive destinations
Naming conventions come in two types.
* **case sensitive** naming convention normalize source identifiers into case sensitive identifiers where character
* **case insensitive**

Case sensitive naming convention will put a destination in [case sensitive mode](destination.md#control-how-dlt-creates-table-column-and-other-identifiers). Identifiers that
differ only in casing will not [collide](#avoid-identifier-collisions). Note that many destinations are exclusively case insensitive, of which some preserve casing of identifiers (ie. **duckdb**) and some will case-fold identifiers when creating tables (ie. **Redshift**, **Athena** do lower case on the names).

## Identifier shortening
Identifier shortening happens during normalization. `dlt` takes the maximum length of the identifier from the destination capabilities and will trim the identifiers that are
too long. The default shortening behavior generates short deterministic hashes of the source identifiers and places them in the middle of the destination identifier. This
(with a high probability) avoids shortened identifier collisions.


## Pick your own naming convention

### Configure naming convention
The naming convention is configurable and users can easily create their own
conventions that i.e. pass all the identifiers unchanged if the destination accepts that (i.e.
DuckDB).


### Available naming conventions

### Set and adjust naming convention explicitly

## Avoid identifier collisions


`dlt` detects various types of collisions and ignores the others.


## Write your own naming convention
Naming conventions reside in separate Python modules, are classes with `NamingConvention` name and must derive from `BaseNamingConvention`. We include two examples of
naming conventions that you may find useful

1. A variant of `sql_ci` that generates identifier collisions with a low (user defined) probability by appending a deterministic tag to each name.
2. A variant of `sql_cs` that allows for LATIN-2 (ie. umlaut) characters
