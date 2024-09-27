---
title: INSERT
description: The INSERT file format
keywords: [insert values, file formats]
---
import SetTheFormat from './_set_the_format.mdx';

# SQL INSERT file format

This file format contains an INSERT...VALUES statement to be executed on the destination during the `load` stage.

Additional data types are stored as follows:

- `datetime` and `date` are stored as ISO strings;
- `decimal` is stored as a text representation of a decimal number;
- `binary` storage depends on the format accepted by the destination;
- `json` storage also depends on the format accepted by the destination.

This file format is [compressed](../../reference/performance.md#disabling-and-enabling-file-compression) by default.

## Supported destinations

This format is used by default by: **DuckDB**, **Postgres**, **Redshift**, **Synapse**, **MSSQL**, **Motherduck**

It is also supported by: **Filesystem** if you'd like to store INSERT VALUES statements for some reason.

## How to configure

<SetTheFormat file_type="insert_values"/>

