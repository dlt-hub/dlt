---
title: jsonl
description: The jsonl file format
keywords: [jsonl, file formats]
---

# jsonl - JSON Delimited

JSON Delimited is a file format that stores several JSON documents in one file. The JSON
documents are separated by a new line.

Additional data types are stored as follows:

- `datetime` and `date` are stored as ISO strings;
- `decimal` is stored as a text representation of a decimal number;
- `binary` is stored as a base64 encoded string;
- `HexBytes` is stored as a hex encoded string;
- `complex` is serialized as a string.

This file format is
[compressed](../../reference/performance.md#disabling-and-enabling-file-compression) by default.

## Supported Destinations

This format is used by default by: **BigQuery**, **Snowflake**, **filesystem**.

By setting the `loader_file_format` argument to `jsonl` in the run command, the pipeline will store
your data in the jsonl format at the destination:

```py
info = pipeline.run(some_source(), loader_file_format="jsonl")
```
