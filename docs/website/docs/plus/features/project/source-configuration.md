# Configuration of Sources

<img src="https://storage.googleapis.com/dlt-blog-images/plus/dlt_plus_projects.png" width="500"/>


The `dlt.yml` file is used to declaratively configure data sources for use with the dlt CLI. This setup provides a flexible way to work manage data extraction for the core sources REST APIs, SQL databases and file systems using YAML syntax.

Credential placeholders for the defined sources are automatically generated in `.dlt/secrets.toml`. Alternatively, credentials may also be provided directly within `dlt.yml`.


## REST API

The built-in rest_api type enables configuration of REST-based integrations. Multiple endpoints can be defined under a single source.


```yaml
sources:
  pokemon_api:
    type: rest_api
    client:
      base_url: https://pokeapi.co/api/v2/
      paginator: auto
    resource_defaults:
      primary_key: name
    resources:
      - pokemon
      - berry
      - 
        name: encounter_conditions
        endpoint:
          path: encounter-condition
          params:
            offset:
              type: incremental
              cursor_path: name
        write_disposition: append
```

* `type: rest_api:` specifies the use of the built-in REST API source.
* `client.base_url` sets the root URL for all API requests.
* `paginator: auto` enables automatic detection and handling of pagination.
* `resource_defaults`: Contains the default values to configure the dlt resources. This configuration is applied to all resources unless overridden by the resource-specific configuration.
* Each item in `resources`defines an endpoint to extract. Simple entries like `pokemon`and `berry`will fetch from `/pokemon` and `/berry`, respectively.
* The `encounter-condition` resource uses an advanced configuration:
  * `path`: Point to the `/encounter-condition`endpoint.
  * `params.offset`: Enables incremental loading using the `name` field as the cursor.
  * `write_disposition`: append ensures new data is appended rather than overwriting previous loads.



## SQL database

For SQL-base extractions that require no table-specific parameter configuration, it's possible to initialize `type = sql_database` and declare multiple tables at once.
 
### General SQL Database Source

```yaml 
sources:
   sql_source:
    type: dlt.sources.sql_database.sql_database
    table_names: 
      - family
      - clan
    incremental:
        cursor_path: updated
        initial_value: 2023-01-12T11:21:28Z 
```

This defines a connection to a SQL database with incremental loading applied across multiple tables.

* `type: sql_database`: Specifies the SQL database connector.

* `table_names`: List of tables to extract.

* `incremental`: Global configuration for incremental extraction.

### Table-Specific Configuration

For table specific configurationssettings such as different `primary_key`s, individual tables can be defined as standalone sources using the `sql_table` type.


```yaml
sources: 
  sql_family:
      type: dlt.sources.sql_database.sql_table
      table: family
      incremental:
        cursor_path: updated
        initial_value: 2023-01-12T11:21:28Z 
      primary_key: rfam_id
```

* `type: sql_table`: Indicates a single-table extraction.

* `table`: Name of the table to extract.

* `incremental`: Enables incremental loading for the table.

* `primary_key`: Specifies the table's unique identifier for deduplication and merges.


## Filesystem

Filesystem sources can be set via the readers type and the filesystem specific resources can be called via the CLI `run pipline` command


```yaml
sources: 
 file_source:
    type: filesystem.readers
    bucket_url: file://Users/admin/Documents/csv_files
    file_glob: '*.csv'
```

`dlt pipeline file_pipeline run --resources read_csv`


``
:::tip
Source **type** is used to refer to the location in Python code where the `@dlt.source` decorated function is present. You can
always use a full path to a function name in a Python module, but we also support shorthand and relative notations. For example:
* `rest_api` will be expanded to `dlt.sources.rest_api.rest_api` where `dlt.sources.rest_api` is a Python module in OSS dlt and `rest_api` is a name of a function in that module.
* `github.source` will be expanded to `sources.github.sources` in the current project.
* `filesystem.readers` will be expanded to `dlt.sources.filesystem.readers`

If the **type** cannot be resolved, dlt+ will provide you with a detailed list of all candidate types that were looked up
so you can make required corrections.
:::
