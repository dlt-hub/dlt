---
title: Naming Convention
description: Control how dlt creates table, column and other identifiers
keywords: [identifiers, snake case, case sensitive, case insensitive, naming]
---

# Naming Convention
`dlt` creates table and column identifiers from the data. The data source ie. a stream of JSON documents may have identifiers (i.e. key names in a dictionary) with any Unicode characters, of any length and naming style. On the other hand, destinations require that you follow strict rules when you name tables, columns or collections.
A good example is [Redshift](../dlt-ecosystem/destinations/redshift.md#naming-convention) that accepts case-insensitive alphanumeric identifiers with maximum 127 characters.

`dlt` groups tables from a single [source](source.md) in a [schema](schema.md). Each schema defines **naming convention** that tells `dlt` how to translate identifiers to the
namespace that the destination understands. Naming conventions are in essence functions that map strings from the source identifier format into destination identifier format. For example our **snake_case** (default) naming convention will translate `DealFlow` source identifier into `deal_flow` destination identifier.

You can pick which naming convention to use. `dlt` provides a few to [choose from](#available-naming-conventions). You can [easily add your own](#write-your-own-naming-convention) as well.

:::tip
Standard behavior of `dlt` is to **use the same naming convention for all destinations** so users see always the same table and column names in their databases.
:::

### Use default naming convention (snake_case)
**snake_case** is case insensitive naming convention, converting source identifiers into lower case snake case identifiers with reduced alphabet.

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
1. If you write a [transformer](resource.md#process-resources-with-dlttransformer) or a [mapping/filtering function](resource.md#filter-transform-and-pivot-data), you will see the original data, without any normalization. Use the source identifiers to access the dicts!
2. If you define a `primary_key` or `cursor` that participate in [cursor field incremental loading](incremental-loading.md#incremental-loading-with-a-cursor-field) use the source identifiers (`dlt` uses them to inspect source data, `Incremental` class is just a filtering function).
3. When defining any other hints ie. `columns` or `merge_key` you can pick source or destination identifiers. `dlt` normalizes all hints together with your data.
4. `Schema` object (ie. obtained from the pipeline or from `dlt` source via `discover_schema`) **always contains destination (normalized) identifiers**.

### Understand the identifier normalization
Identifiers are translated from source to destination form in **normalize** step. Here's how `dlt` picks the naming convention:

* The default naming convention is **snake_case**.
* Each destination may define a preferred naming convention in [destination capabilities](destination.md#pass-additional-parameters-and-change-destination-capabilities). Some destinations (ie. Weaviate) need specialized naming convention and will override the default.
* You can [configure a naming convention explicitly](#set-and-adjust-naming-convention-explicitly). Such configuration overrides the destination settings.
* This naming convention is used when new schemas are created. It happens when pipeline is run for the first time.
* Schemas preserve naming convention when saved. Your running pipelines will maintain existing naming conventions if not requested otherwise.
* `dlt` applies final naming convention in `normalize` step. Jobs (files) in load package now have destination identifiers. Pipeline schema is duplicated, locked and saved in the load package and will be used by the destination.

:::caution
If you change naming convention and `dlt` detects that a change in the destination identifiers for tables/collection/files that already exist and store data,
the normalize process will fail. This prevents an unwanted schema migration. New columns and tables will be created for identifiers that changed.
:::

### Case sensitive and insensitive destinations
Naming convention declare if the destination identifiers they produce are case sensitive or insensitive. This helps `dlt` to [generate case sensitive / insensitive identifiers for the destinations that support both](destination.md#control-how-dlt-creates-table-column-and-other-identifiers). For example: if you pick case insensitive naming like **snake_case** or **sql_ci_v1**, with Snowflake, `dlt` will generate all upper-case identifiers that Snowflake sees as case insensitive. If you pick case sensitive naming like **sql_cs_v1**, `dlt` will generate quoted case-sensitive identifiers that preserve identifier capitalization.

Note that many destinations are exclusively case insensitive, of which some preserve casing of identifiers (ie. **duckdb**) and some will case-fold identifiers when creating tables (ie. **Redshift**, **Athena** do lower case on the names). `dlt` is able to detect resulting identifier [collisions](#avoid-identifier-collisions) and stop the load process before data is mangled.

### Identifier shortening
Identifier shortening happens during normalization. `dlt` takes the maximum length of the identifier from the destination capabilities and will trim the identifiers that are
too long. The default shortening behavior generates short deterministic hashes of the source identifiers and places them in the middle of the destination identifier. This
(with a high probability) avoids shortened identifier collisions.

### 🚧 [WIP] Name convention changes are lossy
`dlt` does not store the source identifiers in the schema so when naming convention changes (or we increase the maximum identifier length), it is not able to generate a fully correct set of new identifiers. Instead it will re-normalize already normalized identifiers. We are currently working to store full identifier lineage - source identifiers will be stored and mapped to the destination in the schema.

## Pick your own naming convention

### Configure naming convention
You can use `config.toml`, environment variables or any other configuration provider to set the naming convention name. Configured naming convention **overrides all other settings**
- changes the naming convention stored in the already created schema
- overrides the destination capabilities preference.
```toml
[schema]
naming="sql_ci_v1"
```
Configuration above will request **sql_ci_v1** for all pipelines (schemas). An environment variable `SCHEMA__NAMING` set to `sql_ci_v1` has the same effect.

You have an option to set naming convention per source:
```toml
[sources.zendesk]
config="prop"
[sources.zendesk.schema]
naming="sql_cs_v1"
[sources.zendesk.credentials]
password="pass"
```
Snippet above demonstrates how to apply certain naming for an example `zendesk` source.

You can use naming conventions that you created yourself or got from other users. In that case you should pass a full Python import path to the [module that contain the naming convention](#write-your-own-naming-convention):
```toml
[schema]
naming="tests.common.cases.normalizers.sql_upper"
```
`dlt` will import `tests.common.cases.normalizers.sql_upper` and use `NamingConvention` class found in it as the naming convention.


### Available naming conventions
You can pick from a few built-in naming conventions.

* `snake_case` - the default
* `duck_case` - case sensitive, allows all unicode characters like emoji 💥
* `direct` - case sensitive, allows all unicode characters, does not contract underscores
* `sql_cs_v1` - case sensitive, generates sql-safe identifiers
* `sql_ci_v1` - case insensitive, generates sql-safe lower case identifiers

### Set and adjust naming convention explicitly
You can modify destination capabilities to

## Avoid identifier collisions
`dlt` detects various types of identifier collisions and ignores the others.
1. `dlt` detects collisions if case sensitive naming convention is used on case insensitive destination
2. `dlt` detects collisions if change of naming convention changes the identifiers of tables already created in the destination
3. `dlt` detects collisions when naming convention is applied to column names of arrow tables

`dlt` will not detect collision when normalizing source data. If you have a dictionary, keys will be merged if they collide after being normalized.
You can create a custom naming convention that does not generate collisions on data, see examples below.


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
You should distribute your custom naming conventions with your pipeline code or via a pip package from which it can be imported.
:::
