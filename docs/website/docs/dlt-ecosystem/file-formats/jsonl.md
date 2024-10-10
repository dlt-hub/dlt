---
title: JSONL
description: The JSONL file format or JSON Delimited stores several JSON documents in one file. The JSON documents are separated by a new line.
keywords: [jsonl, file formats, json delimited, jsonl file format]
---
import SetTheFormat from './_set_the_format.mdx';

# JSONL - JSON Lines - JSON Delimited

JSON delimited is a file format that stores several JSON documents in one file. The JSON documents are separated by a new line.

Additional data types are stored as follows:

- `datetime` and `date` are stored as ISO strings;
- `decimal` is stored as a text representation of a decimal number;
- `binary` is stored as a base64 encoded string;
- `HexBytes` is stored as a hex encoded string;
- `json` is serialized as a string.

This file format is [compressed](../../reference/performance.md#disabling-and-enabling-file-compression) by default.

## Supported destinations

This format is used by default by: **BigQuery**, **Snowflake**, **Filesystem**.

## How to configure

<SetTheFormat file_type="jsonl"/>

