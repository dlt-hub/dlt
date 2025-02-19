# Project

import Link from '../../_plus_admonition.md';

<Link/>


<img src="https://storage.googleapis.com/dlt-blog-images/plus/dlt_plus_projects.png" width="500"/>


[dlt+ Project](../core-concepts/project.md) provides a structured and opinionated approach to organizing data workflows while implementing best practices for data engineering teams. dlt+ Project automates key processes such as data loading, data transformations, data catalogs, and data governance, and enables different members of the data teams to work more easily with each other.

With dlt+ Project, you can efficiently manage your data workflows by:

1. [Using a declarative `dlt.yml` file](#the-dlt-manifest-file-dltyml) to define sources, destinations, pipelines, and transformations.
2. Configuring [different profiles](../core-concepts/profiles.md) for various use cases and environments.
3. Ensuring data quality by defining tests with [dlt+ tests utils](./quality/tests.md).
4. Packaging your project as a Python package and distributing it via PyPI or a git repository [Coming Soon!]

This structured approach allows teams to work efficiently while maintaining flexibility and control over their data workflows.

## Project structure

A dlt+ Project has the following general structure:
```text
├── .dlt/                 # folder containing dlt configurations and profile settings
│   ├── config.toml
│   ├── dev.secrets.toml  # credentials for access profile 'dev'
│   └── secrets.toml
├── _data/                # local storage for your project, excluded from git
├── sources/              # modules containing the source code for sources
│   └── github.py         # source code for a github source
├── transformations/      # modules containing the source code for transformations
├── .gitignore
└── dlt.yml               # the main project manifest
```

## The dlt manifest file (dlt.yml)

The main component of a dlt+ Project is the dlt manifest file (`dlt.yml`). It marks the root of your project and contains the main configurations. Here you can declare all of your data platform entities in a YAML format. It contains the following sections:

### Sources

This section lets you define sources either declaratively or by referencing an implementation from a Python module inside `sources/`. In the example below, two sources are declared:
1. a dlt REST API source whose parameters are passed within the manifest
2. a GitHub source defined in a function `source` whose source code inside `sources/github.py` is referenced

```yaml
sources:
  pokemon:
    type: rest_api
    client:
      base_url: https://pokeapi.co/api/v2/
    resource_defaults:
      endpoint:
        params:
          limit: 1000
    resources:
      - pokemon
      - berry

  github:
    type: github.source
```
:::tip
Source **type** is used to refer to the location in Python code where the `@dlt.source` decorated function is present. You can
always use a full path to a function name in a Python module, but we also support shorthand and relative notations. For example:
* `rest_api` will be expanded to `dlt.sources.rest_api.rest_api` where `dlt.sources.rest_api` is a Python module in OSS dlt and
`rest_api` is a name of a function in that module.
* `github.source` will be expanded to `sources.github.sources` in the current project.

If the **type** cannot be resolved, dlt+ will provide you with a detailed list of all candidate types that were looked up
so you can make required corrections.
:::


### Destinations

The destinations section defines dlt destinations in a similar way to how you would define them in a pure Python dlt project. As with sources, you can also create a `destinations/` folder and reference custom implementations of destinations inside it.

```yaml
destinations:
    duckdb:
        type: duckdb
```

### Pipelines

Pipelines can be used to load data from sources to destinations. The pipeline defined below loads data from the GitHub source to a dataset named "github_events_dataset" inside the duckdb destination.

```yaml
github_pipeline:
  source: github
  destination: duckdb
  dataset_name: github_events_dataset
```
You can declare all arguments of `dlt.pipeline` in this section. For a full list of arguments, refer to the [docstrings](https://github.com/dlt-hub/dlt/blob/71b4975c70d1931750b3245e919a520a2400e870/dlt/pipeline/__init__.py#L30).

### Datasets

The datasets section defines datasets that live on a destination (defined in the destinations section). Any datasets declared in the [pipeline section](#pipelines) are automatically created if not declared here. Read more about datasets in dlt+ [here](../core-concepts/datasets.md).

```yaml
datasets:
  github_events_dataset:
    destination:
      - duckdb
```

### Cache 🧪

In this section, you specify the input table(s) that you want to transform, and the output table(s) that you want to write after performing the transformations. The example below loads the table "events" from the destination dataset "github_events_dataset" into a local cache, then transforms it using the transformations inside the `transformations/` folder, and finally writes two tables back into the dataset "github_events_dataset": the original "events" table, and the transformed "events_aggregated" table. Read more about how local cache is used for transformations [here](../core-concepts/datasets.md).

The cache feature is currently limited to specific use cases and is only compatible with data stored in filesystem-based destinations. Please make sure that the input dataset for the cache is located in the filesystem-based destination ([Iceberg](../ecosystem/iceberg.md), [Delta](../ecosystem/delta.md), or [Cloud storage and filesystem](../../dlt-ecosystem/destinations/filesystem.md)).

```yaml
caches:
  github_events_cache:
    inputs:
      - dataset: github_events_dataset
        tables:
          events: events
    outputs:
      - dataset: github_events_dataset
        tables:
          events: events
          events_aggregated: events_aggregated
```
:::note
🚧 This feature is under development. Interested in becoming an early tester? [Join dlt+ early access](https://info.dlthub.com/waiting-list)
:::

### Transformations 🧪

Here you specify the settings for your transformations. In the code example, we define an arrow-based transformation that will operate on the cache "github_events_cache". It will make use of code in the `transformations/` folder. Read more about how transformations are done [here](../features/transformations/index.md).

```yaml
transformations:
  github_events_transformations:
    engine: arrow
    cache: github_events_cache
```
:::note
🚧 This feature is under development. Interested in becoming an early tester? [Join dlt+ early access](https://info.dlthub.com/waiting-list)
:::

### Profiles

You can use the profiles section to define different environments (example: dev, staging, prod, tests). One package may have multiple profiles which can be specified using dlt+ cli commands. The default profile name is `dev`. It's created automatically alongside the `tests` profile.

```yaml
profiles:
  dev: # Using "dev" profile will write to local filesystem
    destinations:
      delta_lake:
        type: delta
        bucket_url: delta_lake
  prod: # Using "prod" profile will write to s3 bucket
    destinations:
      delta_lake:
        type: delta
        bucket_url: s3://dlt-ci-test-bucket/dlt_example_project/
```

### Project settings and variable substitution

You can override default project settings using the `project` section:
* `project_dir` - the root directory of the project, i.e., the directory where the project Python modules are stored.
* `data_dir` and `local_dir` - [files created by pipelines and destinations](#local-and-temporary-files-data_dir-and-local_dir), separated by the current profile name.
* `name` - the name of the project.
* `default_profile` - the name of the default profile, which can be configured in the project section as seen above.
* `allow_undefined_entities` - by default, dlt+ will create entities like destinations, sources, and datasets ad hoc. This flag disables such behavior.

In the example below:
```yaml
project:
  name: test_project
  data_dir: "{env.DLT_DATA_DIR}/{current_profile}"
  allow_undefined_entities: false
  default_profile: tests
  local_dir: "{data_dir}/local"
```
* We set the project name to `test_project`, overriding the default (which is the name of the parent folder).
* We set `data_dir` to the value of the environment variable `DLT_DATA_DIR` and separate it by the profile name `current_profile`.
* We prevent any undefined entities (`allow_undefined_entities`) from being created (i.e., datasets or destinations).
* We set the default profile name to `tests`.
* We set the `local_dir` to a folder `local` in the `data_dir` we defined above.

As you may guess from the example above, you can use Python-style formatters to substitute variables:
* You can reference environment variables using the `{env.ENV_VARIABLE_NAME}` syntax.
* Any of the project settings can be substituted as well.

### Other settings

`dlt.yml` is a [dlt config provider](../../general-usage/credentials/setup.md), and you can use it in the same way you use `config.toml`.
For example, you can configure the log level:

```yaml
runtime:
  log_level: WARNING
```

or any of the settings we mention in the [performance](../../reference/performance.md) chapter.

## Local and temporary files (`data_dir`)

The dlt+ project has a dedicated location (`data_dir`), where all working files are stored. By default, it is the `_data` folder in the root of the project.
Working files for each profile are stored separately. For example, files for the `dev` profile are stored in `_data/dev`.

Working files include:
* Pipeline working directory (`{data_dir}/pipelines` folder) where load packages, pipeline state, and schemas are stored locally.
* All files created by destinations (`{data_dir}\local`) i.e., local `filesystem` buckets, duckdb databases, iceberg, and delta lakes (if configured for the local filesystem).
* Default locations for ad hoc (i.e., dbt related) Python virtual environments.

:::tip
Use relative paths when configuring destinations that generate local files to ensure they are automatically placed in the profile-separated
`{data_dir}\local` folder. For example:

```yaml
destinations:
  iceberg:
    bucket_url: lake
  my_duckdb:
    type: duckdb
```
The `iceberg` destination will create an iceberg lake in the `_data/dev/local/lake` folder, and `duckdb` will create a database in
`_data/dev/local/my_duckdb.duckdb`.

You can clean up your working files with the `dlt project --profile name clean` command.
:::

## 🧪 Python API to interact with dlt+ project
You can access any dlt+ project entity or function via the Python interface. In the example below:
```yaml
transformations:
  stressed_transformation:
    engine: arrow
    cache: stressed_cache
pipelines:
  bronze_pipe:
    destination: filesystem
    dataset_name: bronze
```

```py
import dlt_plus

entities = dlt_plus.current.entities()
pipeline = entities.create_pipeline("bronze_pipe")
transformation = entities.create_transformation("stressed_transformation")
```
Here, we access the entities manager, which allows you to create sources, destinations, pipelines, and other objects.
You can also obtain the **catalog** with all created datasets via `dlt_plus.current.catalog()`, which makes it easy to
[explore all of your data](data-access.md).

## Config and secrets

As shown above, it is possible to pass additional dlt settings and configurations in the manifest file itself. However, existing dlt config providers are also supported as usual, like:

1. environ provider
2. `.dlt/config.toml` provider, including the global config
3. `.dlt/<profile_name>.secrets.toml`, which is the secrets toml provider but scoped to a particular profile. A per-profile version (`dev.secrets.toml`) is sought instead of the `secrets.toml` file.

:::note
Based on the information about precedence in the [configuration docs](../../general-usage/credentials/setup#available-config-providers), the yaml files provide the lowest precedence of all providers just above the default values for a config value. Settings in the yaml file will therefore be overridden by `toml` and `env` variables if present.
:::

## Project context

The `dlt.yml` marks the root of a project. Projects can also be nested. If you run any dlt project CLI command, dlt will search for the project root in the filesystem tree starting from the current working directory and run all operations on the found project. So if your `dlt.yml` is in the `tutorial` folder, you can run `dlt pipeline my_pipeline run` from this folder or any subfolder, and it will run the pipeline on the `tutorial` project.

## Packaging and distributing the projects

Projects can be distributed as Python packages to share with your organization and enable data access. Instructions on how to build these Python packages are coming soon. Join our [early access](https://info.dlthub.com/waiting-list) program to learn more!

