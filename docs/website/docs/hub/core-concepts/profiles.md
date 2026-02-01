---
title: Profiles
keywords: [dltHub, profiles]
---

A profile is a set of configurations and secrets defined for a specific use case. Profiles provide a way to manage different configurations for different environments.

They are defined in the `dlt.yml` under the `profiles` section.

```yaml
profiles:
  # profiles allow you to configure different settings for different environments
  dev:
    sources:
      my_arrow_source:
        row_count: 100
    runtime:
      log_level: DEBUG
  prod:
    sources:
      my_arrow_source:
        row_count: 200
    runtime:
      log_level: INFO
    destinations:
      my_duckdb_destination:
        credentials: my_data_prod.duckdb
```

Every project includes two implicit profiles by default: `dev` and `tests`. If no profile is specified, the `dev` profile is loaded by default.
All CLI commands that run on a project support the `--profile` option, allowing you to specify the desired profile. For example,

```sh
dlt project --profile dev my_pipeline run
dlt dataset --profile prod my_duckdb_destination_dataset row-counts
```

## Using config files with profiles

All the configuration and secrets for profiles can also be placed in TOML files, as [described in dlt OSS documentation](../../general-usage/credentials/).
Each profile can have its own `secrets.toml` file, which is only loaded when that profile is active.

For example, if you have two secrets files under `.dlt`:

```sh
.
├── .dlt/                 # your dlt settings including profile settings
│   ├── config.toml
│   ├── dev.secrets.toml
│   └── tests.secrets.toml
```

You can run a pipeline with different profiles as follows:

```sh
dlt pipeline --profile dev my_pipeline run
dlt pipeline --profile tests my_pipeline run
```

:::warning
Please note the following inconsistencies between the YAML and TOML files that will be fixed in the future:

* The YAML `destinations` section is singularized to `destination` in the TOML file.
* The project variables such as `tmp_dir` are not available in the TOML files.
:::

## Pinning profiles
You can pin a profile locally, making the given profile name the default one. This is useful, for example, when deploying your project in a production or staging environment.
```sh
dlt profile prod pin
```
will pin the `prod` profile and from now on all Python scripts and cli commands will see it as the default and switch to it automatically.
The profile pin is kept in the `.dlt/profile-name` file. Remove this file to unpin. Note that our default `.gitignore` prevents this file from being added.

### Settings in the `dlt.yml` file vs TOML files

For dltHub Projects, it's best practice to keep all non-secret settings in `dlt.yml` and store secrets only in `.dlt/secrets.toml`. This ensures that sensitive data is only available in the necessary profiles or environments.

In the example above, some non-secret values were moved to `.dlt/secrets.toml` for demonstration purposes only - this is not the recommended approach.
