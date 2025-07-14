# Configuration of Sources

<img src="https://storage.googleapis.com/dlt-blog-images/plus/dlt_plus_projects.png" width="500"/>


## REST API

This setup provides a flexible, zero-code way to work with REST APIs and manage data extraction across multiple endpoints in a single source block.

```yaml
sources:
  github:
    type: github.github_reactions

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

* `type: rest_api` specifies the use of the built-in REST API source.
* `client.base_url` sets the root URL for all API requests.
* `paginator: auto` enables automatic detection and handling of pagination.
* `resource_defaults`: Contains the default values to configure the dlt resources. This configuration is applied to all resources unless overridden by the resource-specific configuration.
* Each item in `resources`defines an endpoint to extract. Simple entries like `pokemon`and `berry`will fetch from `/pokemon` and `/berry`, respectively.
* The `encounter-condition` resource uses an advanced configuration:
  * `path`: Point to the `/encounter-condition`endpoint.
  * `params.offset`: Enables incremental loading using the `name` field as the cursor.
  * `write_disposition`: append ensures new data is appended rather than overwriting previous loads.



## SQL database

```yaml 
sources:
  arrow:
    type: sources.arrow.source

  sql_db_1:
    type: sql_database
```

The corresponding credential placeholders will be added to `.dlt/secrets.toml`, but you can also define them in `dlt.yml`.
