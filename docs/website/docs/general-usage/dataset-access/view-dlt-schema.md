---
title: Review dlt schema
description: View your dlt schema via files, CLI, static and interactive diagram
keywords: [schema, dataset, view, dbml, graphviz]
---

# Review dlt schema

During the first `dlt.Pipeline` run, dlt produces a `dlt.Schema` from the data processed. This schema tells how data was stored on destination. 

You can access the live `dlt.Schema` via the property `dlt.Pipeline.default_schema`.

```python
pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination="duckdb")
pipeline.run(chess_source())

print(pipeline.default_schema)
```

For review, testing, data validation, documentation, etc. you can get a read-only and human-readable copy using Python code or the `dlt` CLI.


## Export to `dict`
Export to a Python dictionary. This is ideal for programmatic use (data validation, testing, manipulating metadata).

The conversion is lossless and allows you to reconstruct the `dlt.Schema` object. This is not available via the CLI.

```python
schema_dict = pipeline.default_schema.to_dict()
```

<details>
  <summary>See <code>dict</code></summary>

  ```python
{
    "version": 2,
    "version_hash": "iW0MtTw8NXm1r/amMiYpOF63Of44Mx5VfYOh5DM6/7s=",
    "engine_version": 11,
    "name": "fruit_with_ref",
    "tables": {
        "_dlt_version": {
            "name": "_dlt_version",
            "columns": {
                "version": {"name": "version", "data_type": "bigint", "nullable": False},
                "engine_version": {
                    "name": "engine_version",
                    "data_type": "bigint",
                    "nullable": False,
                },
                "inserted_at": {"name": "inserted_at", "data_type": "timestamp", "nullable": False},
                "schema_name": {"name": "schema_name", "data_type": "text", "nullable": False},
                "version_hash": {"name": "version_hash", "data_type": "text", "nullable": False},
                "schema": {"name": "schema", "data_type": "text", "nullable": False},
            },
            "write_disposition": "skip",
            "resource": "_dlt_version",
            "description": "Created by DLT. Tracks schema updates",
        },
        "_dlt_loads": {
            "name": "_dlt_loads",
            "columns": {
                "load_id": {"name": "load_id", "data_type": "text", "nullable": False},
                "schema_name": {"name": "schema_name", "data_type": "text", "nullable": True},
                "status": {"name": "status", "data_type": "bigint", "nullable": False},
                "inserted_at": {"name": "inserted_at", "data_type": "timestamp", "nullable": False},
                "schema_version_hash": {
                    "name": "schema_version_hash",
                    "data_type": "text",
                    "nullable": True,
                },
            },
            "write_disposition": "skip",
            "resource": "_dlt_loads",
            "description": "Created by DLT. Tracks completed loads",
        },
        "customers": {
            "columns": {
                "id": {"name": "id", "nullable": False, "primary_key": True, "data_type": "bigint"},
                "name": {
                    "x-annotation-pii": True,
                    "name": "name",
                    "data_type": "text",
                    "nullable": True,
                },
                "city": {"name": "city", "data_type": "text", "nullable": True},
                "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
                "_dlt_id": {
                    "name": "_dlt_id",
                    "data_type": "text",
                    "nullable": False,
                    "unique": True,
                    "row_key": True,
                },
            },
            "write_disposition": "append",
            "name": "customers",
            "resource": "customers",
            "x-normalizer": {"seen-data": True},
        },
        "purchases": {
            "columns": {
                "id": {"name": "id", "nullable": False, "primary_key": True, "data_type": "bigint"},
                "customer_id": {"name": "customer_id", "data_type": "bigint", "nullable": True},
                "inventory_id": {"name": "inventory_id", "data_type": "bigint", "nullable": True},
                "quantity": {"name": "quantity", "data_type": "bigint", "nullable": True},
                "date": {"name": "date", "data_type": "text", "nullable": True},
                "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
                "_dlt_id": {
                    "name": "_dlt_id",
                    "data_type": "text",
                    "nullable": False,
                    "unique": True,
                    "row_key": True,
                },
            },
            "write_disposition": "append",
            "references": [
                {
                    "columns": ["customer_id"],
                    "referenced_table": "customers",
                    "referenced_columns": ["id"],
                }
            ],
            "name": "purchases",
            "resource": "purchases",
            "x-normalizer": {"seen-data": True},
        },
        "_dlt_pipeline_state": {
            "columns": {
                "version": {"name": "version", "data_type": "bigint", "nullable": False},
                "engine_version": {
                    "name": "engine_version",
                    "data_type": "bigint",
                    "nullable": False,
                },
                "pipeline_name": {"name": "pipeline_name", "data_type": "text", "nullable": False},
                "state": {"name": "state", "data_type": "text", "nullable": False},
                "created_at": {"name": "created_at", "data_type": "timestamp", "nullable": False},
                "version_hash": {"name": "version_hash", "data_type": "text", "nullable": True},
                "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
                "_dlt_id": {
                    "name": "_dlt_id",
                    "data_type": "text",
                    "nullable": False,
                    "unique": True,
                    "row_key": True,
                },
            },
            "write_disposition": "append",
            "file_format": "preferred",
            "name": "_dlt_pipeline_state",
            "resource": "_dlt_pipeline_state",
            "x-normalizer": {"seen-data": True},
        },
        "purchases__items": {
            "name": "purchases__items",
            "columns": {
                "name": {"name": "name", "data_type": "text", "nullable": True},
                "price": {"name": "price", "data_type": "bigint", "nullable": True},
                "_dlt_parent_id": {
                    "name": "_dlt_parent_id",
                    "data_type": "text",
                    "nullable": False,
                    "parent_key": True,
                },
                "_dlt_list_idx": {
                    "name": "_dlt_list_idx",
                    "data_type": "bigint",
                    "nullable": False,
                },
                "_dlt_id": {
                    "name": "_dlt_id",
                    "data_type": "text",
                    "nullable": False,
                    "unique": True,
                    "row_key": True,
                },
            },
            "parent": "purchases",
            "x-normalizer": {"seen-data": True},
        },
    },
    "settings": {
        "detections": ["iso_timestamp"],
        "default_hints": {
            "not_null": [
                "_dlt_id",
                "_dlt_root_id",
                "_dlt_parent_id",
                "_dlt_list_idx",
                "_dlt_load_id",
            ],
            "parent_key": ["_dlt_parent_id"],
            "root_key": ["_dlt_root_id"],
            "unique": ["_dlt_id"],
            "row_key": ["_dlt_id"],
        },
    },
    "normalizers": {
        "names": "snake_case",
        "json": {"module": "dlt.common.normalizers.json.relational"},
    },
    "previous_hashes": [
        "+stnjP5XdPbykNQJVpK/zpfo0iVbyRFfSIIRzuPzcI4=",
        "nTU+qnLwEmiMSWTwu+QH321j4zl8NrOVL4Hx/GxQAHE=",
    ],
}

  ```

</details>



## Export to JSON
Export to a JSON string. This is useful for passing between services, store to a file, or add to documentation. The conversion is lossless.

```python
# it's "pretty" JSON because the output is indented for human-readability
schema_json = pipeline.default_schema.to_pretty_json()
```

```sh
# `chess_pipeline` is the name of the pipeline
dlt pipeline chess_pipeline schema --format json
```


<details>
  <summary>See JSON</summary>

  ```json
{
  "version": 2,
  "version_hash": "iW0MtTw8NXm1r/amMiYpOF63Of44Mx5VfYOh5DM6/7s=",
  "engine_version": 11,
  "name": "fruit_with_ref",
  "tables": {
    "_dlt_version": {
      "columns": {
        "version": {
          "data_type": "bigint",
          "nullable": false
        },
        "engine_version": {
          "data_type": "bigint",
          "nullable": false
        },
        "inserted_at": {
          "data_type": "timestamp",
          "nullable": false
        },
        "schema_name": {
          "data_type": "text",
          "nullable": false
        },
        "version_hash": {
          "data_type": "text",
          "nullable": false
        },
        "schema": {
          "data_type": "text",
          "nullable": false
        }
      },
      "write_disposition": "skip",
      "resource": "_dlt_version",
      "description": "Created by DLT. Tracks schema updates"
    },
    "_dlt_loads": {
      "columns": {
        "load_id": {
          "data_type": "text",
          "nullable": false
        },
        "schema_name": {
          "data_type": "text",
          "nullable": true
        },
        "status": {
          "data_type": "bigint",
          "nullable": false
        },
        "inserted_at": {
          "data_type": "timestamp",
          "nullable": false
        },
        "schema_version_hash": {
          "data_type": "text",
          "nullable": true
        }
      },
      "write_disposition": "skip",
      "resource": "_dlt_loads",
      "description": "Created by DLT. Tracks completed loads"
    },
    "customers": {
      "columns": {
        "id": {
          "nullable": false,
          "primary_key": true,
          "data_type": "bigint"
        },
        "name": {
          "x-annotation-pii": true,
          "data_type": "text",
          "nullable": true
        },
        "city": {
          "data_type": "text",
          "nullable": true
        },
        "_dlt_load_id": {
          "data_type": "text",
          "nullable": false
        },
        "_dlt_id": {
          "data_type": "text",
          "nullable": false,
          "unique": true,
          "row_key": true
        }
      },
      "write_disposition": "append",
      "resource": "customers",
      "x-normalizer": {
        "seen-data": true
      }
    },
    "purchases": {
      "columns": {
        "id": {
          "nullable": false,
          "primary_key": true,
          "data_type": "bigint"
        },
        "customer_id": {
          "data_type": "bigint",
          "nullable": true
        },
        "inventory_id": {
          "data_type": "bigint",
          "nullable": true
        },
        "quantity": {
          "data_type": "bigint",
          "nullable": true
        },
        "date": {
          "data_type": "text",
          "nullable": true
        },
        "_dlt_load_id": {
          "data_type": "text",
          "nullable": false
        },
        "_dlt_id": {
          "data_type": "text",
          "nullable": false,
          "unique": true,
          "row_key": true
        }
      },
      "write_disposition": "append",
      "references": [
        {
          "columns": [
            "customer_id"
          ],
          "referenced_table": "customers",
          "referenced_columns": [
            "id"
          ]
        }
      ],
      "resource": "purchases",
      "x-normalizer": {
        "seen-data": true
      }
    },
    "_dlt_pipeline_state": {
      "columns": {
        "version": {
          "data_type": "bigint",
          "nullable": false
        },
        "engine_version": {
          "data_type": "bigint",
          "nullable": false
        },
        "pipeline_name": {
          "data_type": "text",
          "nullable": false
        },
        "state": {
          "data_type": "text",
          "nullable": false
        },
        "created_at": {
          "data_type": "timestamp",
          "nullable": false
        },
        "version_hash": {
          "data_type": "text",
          "nullable": true
        },
        "_dlt_load_id": {
          "data_type": "text",
          "nullable": false
        },
        "_dlt_id": {
          "data_type": "text",
          "nullable": false,
          "unique": true,
          "row_key": true
        }
      },
      "write_disposition": "append",
      "file_format": "preferred",
      "resource": "_dlt_pipeline_state",
      "x-normalizer": {
        "seen-data": true
      }
    },
    "purchases__items": {
      "columns": {
        "name": {
          "data_type": "text",
          "nullable": true
        },
        "price": {
          "data_type": "bigint",
          "nullable": true
        },
        "_dlt_parent_id": {
          "data_type": "text",
          "nullable": false,
          "parent_key": true
        },
        "_dlt_list_idx": {
          "data_type": "bigint",
          "nullable": false
        },
        "_dlt_id": {
          "data_type": "text",
          "nullable": false,
          "unique": true,
          "row_key": true
        }
      },
      "parent": "purchases",
      "x-normalizer": {
        "seen-data": true
      }
    }
  },
  "settings": {
    "detections": [
      "iso_timestamp"
    ],
    "default_hints": {
      "not_null": [
        "_dlt_id",
        "_dlt_root_id",
        "_dlt_parent_id",
        "_dlt_list_idx",
        "_dlt_load_id"
      ],
      "parent_key": [
        "_dlt_parent_id"
      ],
      "root_key": [
        "_dlt_root_id"
      ],
      "unique": [
        "_dlt_id"
      ],
      "row_key": [
        "_dlt_id"
      ]
    }
  },
  "normalizers": {
    "names": "snake_case",
    "json": {
      "module": "dlt.common.normalizers.json.relational"
    }
  },
  "previous_hashes": [
    "+stnjP5XdPbykNQJVpK/zpfo0iVbyRFfSIIRzuPzcI4=",
    "nTU+qnLwEmiMSWTwu+QH321j4zl8NrOVL4Hx/GxQAHE="
  ]
}
  ```

</details>


## Export to YAML
Export to a YAML string. It serves the same purposes a JSON export. The conversion is lossless.

```python
# it's "pretty" YAML because the output is indented for human-readability
schema_yaml = pipeline.default_schema.to_pretty_yaml()
```

```sh
# `chess_pipeline` is the name of the pipeline
dlt pipeline chess_pipeline schema --format yaml
```

<details>
  <summary>See YAML</summary>

  ```yaml
version: 2
version_hash: iW0MtTw8NXm1r/amMiYpOF63Of44Mx5VfYOh5DM6/7s=
engine_version: 11
name: fruit_with_ref
tables:
  _dlt_version:
    columns:
      version:
        data_type: bigint
        nullable: false
      engine_version:
        data_type: bigint
        nullable: false
      inserted_at:
        data_type: timestamp
        nullable: false
      schema_name:
        data_type: text
        nullable: false
      version_hash:
        data_type: text
        nullable: false
      schema:
        data_type: text
        nullable: false
    write_disposition: skip
    resource: _dlt_version
    description: Created by DLT. Tracks schema updates
  _dlt_loads:
    columns:
      load_id:
        data_type: text
        nullable: false
      schema_name:
        data_type: text
        nullable: true
      status:
        data_type: bigint
        nullable: false
      inserted_at:
        data_type: timestamp
        nullable: false
      schema_version_hash:
        data_type: text
        nullable: true
    write_disposition: skip
    resource: _dlt_loads
    description: Created by DLT. Tracks completed loads
  customers:
    columns:
      id:
        nullable: false
        primary_key: true
        data_type: bigint
      name:
        x-annotation-pii: true
        data_type: text
        nullable: true
      city:
        data_type: text
        nullable: true
      _dlt_load_id:
        data_type: text
        nullable: false
      _dlt_id:
        data_type: text
        nullable: false
        unique: true
        row_key: true
    write_disposition: append
    resource: customers
    x-normalizer:
      seen-data: true
  purchases:
    columns:
      id:
        nullable: false
        primary_key: true
        data_type: bigint
      customer_id:
        data_type: bigint
        nullable: true
      inventory_id:
        data_type: bigint
        nullable: true
      quantity:
        data_type: bigint
        nullable: true
      date:
        data_type: text
        nullable: true
      _dlt_load_id:
        data_type: text
        nullable: false
      _dlt_id:
        data_type: text
        nullable: false
        unique: true
        row_key: true
    write_disposition: append
    references:
    - columns:
      - customer_id
      referenced_table: customers
      referenced_columns:
      - id
    resource: purchases
    x-normalizer:
      seen-data: true
  _dlt_pipeline_state:
    columns:
      version:
        data_type: bigint
        nullable: false
      engine_version:
        data_type: bigint
        nullable: false
      pipeline_name:
        data_type: text
        nullable: false
      state:
        data_type: text
        nullable: false
      created_at:
        data_type: timestamp
        nullable: false
      version_hash:
        data_type: text
        nullable: true
      _dlt_load_id:
        data_type: text
        nullable: false
      _dlt_id:
        data_type: text
        nullable: false
        unique: true
        row_key: true
    write_disposition: append
    file_format: preferred
    resource: _dlt_pipeline_state
    x-normalizer:
      seen-data: true
  purchases__items:
    columns:
      name:
        data_type: text
        nullable: true
      price:
        data_type: bigint
        nullable: true
      _dlt_parent_id:
        data_type: text
        nullable: false
        parent_key: true
      _dlt_list_idx:
        data_type: bigint
        nullable: false
      _dlt_id:
        data_type: text
        nullable: false
        unique: true
        row_key: true
    parent: purchases
    x-normalizer:
      seen-data: true
settings:
  detections:
  - iso_timestamp
  default_hints:
    not_null:
    - _dlt_id
    - _dlt_root_id
    - _dlt_parent_id
    - _dlt_list_idx
    - _dlt_load_id
    parent_key:
    - _dlt_parent_id
    root_key:
    - _dlt_root_id
    unique:
    - _dlt_id
    row_key:
    - _dlt_id
normalizers:
  names: snake_case
  json:
    module: dlt.common.normalizers.json.relational
previous_hashes:
- +stnjP5XdPbykNQJVpK/zpfo0iVbyRFfSIIRzuPzcI4=
- nTU+qnLwEmiMSWTwu+QH321j4zl8NrOVL4Hx/GxQAHE=
  ```

</details>

## Export to DBML
[DBML (Database Markup Language)](https://dbml.dbdiagram.io/home) is an open-source DSL to define and document database schemas and structures. Exporting your `dlt.Schema` to DBML, allows you to view it in a DBML frontend such as [dbdiagram.io](https://dbdiagram.io/) or [chartdb.io](https://chartdb.io/).

Note that the conversion is lossy. You can't fully recreate `dlt.Schema` from a DBML schema. However, this is a planned feature!

```python
schema_dbml = pipeline.default_schema.to_dbml()
```

```sh
# `chess_pipeline` is the name of the pipeline
dlt pipeline chess_pipeline schema --format dbml
```

<details>
  <summary>See DBML</summary>

  ```dbml
Table "customers" {
    "id" bigint [pk, not null]
    "name" text
    "city" text
    "_dlt_load_id" text [not null]
    "_dlt_id" text [unique, not null]
}

Table "purchases" {
    "id" bigint [pk, not null]
    "customer_id" bigint
    "inventory_id" bigint
    "quantity" bigint
    "date" text
    "_dlt_load_id" text [not null]
    "_dlt_id" text [unique, not null]
}

Table "purchases__items" {
    "name" text
    "price" bigint
    "_dlt_parent_id" text [not null]
    "_dlt_list_idx" bigint [not null]
    "_dlt_id" text [unique, not null]
}

Table "_dlt_version" {
    "version" bigint [not null]
    "engine_version" bigint [not null]
    "inserted_at" timestamp [not null]
    "schema_name" text [not null]
    "version_hash" text [not null]
    "schema" text [not null]
    Note {
        'Created by DLT. Tracks schema updates'
    }
}

Table "_dlt_loads" {
    "load_id" text [not null]
    "schema_name" text
    "status" bigint [not null]
    "inserted_at" timestamp [not null]
    "schema_version_hash" text
    Note {
        'Created by DLT. Tracks completed loads'
    }
}

Table "_dlt_pipeline_state" {
    "version" bigint [not null]
    "engine_version" bigint [not null]
    "pipeline_name" text [not null]
    "state" text [not null]
    "created_at" timestamp [not null]
    "version_hash" text
    "_dlt_load_id" text [not null]
    "_dlt_id" text [unique, not null]
}

Ref {
    "customers"."_dlt_load_id" > "_dlt_loads"."load_id"
}

Ref {
    "purchases"."customer_id" <> "customers"."id"
}

Ref {
    "purchases"."_dlt_load_id" > "_dlt_loads"."load_id"
}

Ref {
    "_dlt_pipeline_state"."_dlt_load_id" > "_dlt_loads"."load_id"
}

Ref {
    "purchases__items"."_dlt_parent_id" > "purchases"."_dlt_id"
}

Ref {
    "_dlt_version"."version_hash" < "_dlt_loads"."schema_version_hash"
}

Ref {
    "_dlt_version"."schema_name" <> "_dlt_loads"."schema_name"
}

TableGroup "customers" {
    "customers"
}

TableGroup "purchases" {
    "purchases"
    "purchases__items"
}

TableGroup "_dlt" {
    "_dlt_version"
    "_dlt_loads"
    "_dlt_pipeline_state"
}
  ```

</details>


![chartdb dbml render](./static/chartdb_schema.png)

