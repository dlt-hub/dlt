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
Case insensitive naming convention, converting source identifiers into lower case snake case with reduced alphabet.

- Spaces around identifier are trimmed
- Keeps ascii alphanumerics and underscores, replaces all other characters with underscores (with the exceptions below)
- Replaces `+` and `*` with `x`, `-` with `_`, `@` with `a` and `|` with `l`
- Prepends `_` if name starts with number.
- Multiples of `_` are converted into single `_`.
- Replaces all trailing `_` with `x`

Uses __ as patent-child separator for tables and flattened column names.

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
2. If you define a `primary_key` or `cursor` that participate in [cursor field incremental loading](incremental-loading.md#incremental-loading-with-a-cursor-field) use the source identifiers (`dlt` uses them to inspect source data, `Incremental` class is a filtering function).
3. When defining any other hints ie. `columns` or `merge_key` you can pick source or destination identifiers. `dlt` normalizes all hints together with your data.
4. `Schema` object (ie. obtained from the pipeline or from `dlt` source via `discover_schema`) **always contains destination (normalized) identifiers**.

In the snippet below, we define a resource with various "illegal" unicode characters in table name and other hint and demonstrate how they get normalized in the schema object.
```py
```

### Understand the identifier normalization
Identifiers are translated from source to destination form in **normalize** step. Here's how `dlt` picks the right naming convention:

* Each destination may define a preferred naming convention (ie. Weaviate), otherwise **snake case** will be used
* This naming convention is used when new schemas are created. This happens when pipeline is run for a first time.
* Schemas preserve naming convention when saved. Your running pipelines will maintain existing naming conventions if not requested otherwise
* `dlt` applies final naming convention in `normalize` step. Naming convention comes from (1) explicit configuration (2) from destination capabilities.
* Naming convention will be used to put destination is case sensitive/insensitive mode and apply the right case folding function.

:::caution
If you change naming convention and `dlt` detects that it changes the destination identifiers for tables/collection/files that already exist and store data,
the normalize process will fail.
:::

### Case sensitive and insensitive destinations
Naming conventions come in two types.
* **case sensitive**
* **case insensitive**

Case sensitive naming convention will put a destination in [case sensitive mode](destination.md#control-how-dlt-creates-table-column-and-other-identifiers). Identifiers that
differ only in casing will not [collide](#avoid-identifier-collisions). Note that many destinations are exclusively case insensitive, of which some preserve casing of identifiers (ie. **duckdb**) and some will case-fold identifiers when creating tables (ie. **Redshift**, **Athena** do lower case on the names).

## Identifier shortening
Identifier shortening happens during normalization. `dlt` takes the maximum length of the identifier from the destination capabilities and will trim the identifiers that are
too long. The default shortening behavior generates short deterministic hashes of the source identifiers and places them in the middle of the destination identifier. This
(with a high probability) avoids shortened identifier collisions.


## Pick your own naming convention

### Configure naming convention
tbd.


### Available naming conventions

* snake_case
* duck_case - case sensitive, allows all unicode characters like emoji 💥
* direct - case sensitive, allows all unicode characters, does not contract underscores
* `sql_cs_v1` - case sensitive, generates sql-safe identifiers
* `sql_ci_v1` - case insensitive, generates sql-safe lower case identifiers

### Set and adjust naming convention explicitly
tbd.

## Avoid identifier collisions
`dlt` detects various types of collisions and ignores the others.
1. `dlt` detects collisions if case sensitive naming convention is used on case insensitive destination
2. `dlt` detects collisions if change of naming convention changes the identifiers of tables already created in the destination
3. `dlt` detects collisions when naming convention is applied to column names of arrow tables

`dlt` will not detect collision when normalizing source data. If you have a dictionary, keys will be merged if they collide after being normalized.
You can use a naming convention that does not generate collisions, see examples below.


## Write your own naming convention
Custom naming conventions are classes that derive from `NamingConvention` that you can import from `dlt.common.normalizers.naming`. We recommend the following module layout:
1. Each naming convention resides in a separate Python module (file)
2. The class is always named `NamingConvention`

In that case you can use a fully qualified module name in [schema configuration](#configure-naming-convention) or pass module [explicitly](#set-and-adjust-naming-convention-explicitly).

We include [two examples](../examples/custom_naming) of naming conventions that you may find useful:

1. A variant of `sql_ci` that generates identifier collisions with a low (user defined) probability by appending a deterministic tag to each name.
2. A variant of `sql_cs` that allows for LATIN (ie. umlaut) characters

:::note
Note that a fully qualified name of your custom naming convention will be stored in the `Schema` and `dlt` will attempt to import it when schema is loaded from storage.
You should distribute your custom naming conventions with your pipeline code via an installable package with a defined namespace.
:::
