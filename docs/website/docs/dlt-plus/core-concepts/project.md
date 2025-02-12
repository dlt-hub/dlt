# dlt+ Package

A dlt+ package is a dlt project with an opinionated structure containing a Python manifest file (`dlt.yml`) where you can define dlt entities (sources, destinations, pipelines, transformations, etc.) in a declarative fashion. The package layout is fully compatible with a standard Python package approach and may be distributed via PyPI or as a git repository. It has the following components:

1. A Python manifest of the package (`pyproject.toml`) which specifies dependencies, source files, and package build system.
2. A dlt manifest file (`dlt.yml`) which specifies data platform entities like sources, destination, pipelines, transformations etc.
3. Python modules with source code and tests. We propose a strict layout of the modules (i.e., source code is in the `sources/` folder, etc.)

Package and project manager [uv](https://docs.astral.sh/uv/) is used for packaging.  

## Project structure
  
A dlt+ package has the following general structure:
```text
.
├── dlt_example_package
│    ├── .dlt/                 # folder containg dlt configrations and profile settings
│    │   ├── config.toml
│    │   ├── dev.secrets.toml  # credentials for access profile 'dev'
│    │   └── secrets.toml      
│    ├── sources/              # modules containing the source code for sources 
│    │   └── github.py         # source code for a github source  
│    ├── destinations/         # modules containing the source code for destinations  
│    ├── transformations/      # modules containing the source code for transformations 
│    ├── .gitignore
│    └── dlt.yml               # the main dlt manifest file
└── pyproject.toml             # the python manifest file for the package
```

### The dlt manifest file (dlt.yml)

This file contains the main configuration for the package. It allows you to declare all dlt entities in a yaml format. It contains the following sections:

#### Sources

This section lets you define sources either declaratively or by referencing an implementation from a python module inside `sources/`. In the example below, two sources are declared:  
1. a dlt REST API source whose parameters are passed within the manifest
2. a github source whose source code inside `sources/github.py` is referenced
  
```yaml
sources:
  pokemon:
    type: dlt.sources.rest_api.rest_api
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
    type: dlt_example_package.github.source
```
#### Destinations  
  
The destinations section defines dlt destinations in a similar way to how you would define them in a pure Python dlt project. As with sources, you can also reference custom implementations of destinations from inside the `destinations/` folder.
  
```yaml  
destinations:
    duckdb:
        type: duckdb
```

#### Pipelines 

Pipelines can be used to load data from sources to destinations. The pipeline defined below loads data from the github source to a dataset named "github_events_dataset" inside the duckdb destination.

```yaml
  github_pipeline:
    source: github
    destination: duckdb
    dataset_name: github_events_dataset
```

#### Datasets

The datasets section defines datasets that live on a destination (defined in the destinations section). Datasets must be declared. They are referenced by pipelines when loading the data, and by the cache when doing transformations. Read more about datasets in dlt+ [here](datasets.md).  
  
```yaml
datasets:
    github_events_dataset:
        "on":
            - duckdb
```

#### Caches  

In this section you specify the input table(s) that you want to transform, and the output table(s) that you want to write after performing the transformations. The example below loads the table "events" from the destination dataset "github_events_dataset" into a local cache, then transforms it using the transformations inside the `transformations/` folder, and finally writes two tables back into the dataset "github_events_dataset": the original "events" table, and the transformed "events_aggregated" table. Read more about how local cache is used for transformations [here](cache.md).
  
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

#### Transformations

Here you specify the settings for your transformations. In the code example we define an arrow-based transformation that will operate on the cache "github_events_cache". It will make use of code in the `transformations/` folder. Read more about how transformations are done [here](cache.md).

```yaml
transformations:
  github_events_transformations:
    engine: arrow
    cache: github_events_cache
```

#### Profiles

You can use the profiles section to define different environments (example: dev, staging, prod, tests). One package may have multiple profiles which can be specified using dlt+ CLI commands. The default profile name is `dev`. If you want to use dlt test helpers, you must define a `tests` profile as well.

```yaml
profiles:
    dev: # Using "dev" profile will write to local filesystem
        destinations:
            delta_lake:
                type: filesystem
                bucket_url: ${tmp_dir}delta_lake
    prod: # Using "prod" profle will write to an s3 bucktet
        destinations:
            delta_lake:
                type: filesystem
                bucket_url: s3://dlt-ci-test-bucket/dlt_example_package/

```

#### Misc settings

It is also possible to add additional dlt settings that mirror the `config.toml` settings:

```yaml
runtime:
  log_level: WARNING
```

### Config and secrets

As shown above, it is possible to pass additional dlt settings and configurations in the manifest file itself. But existing dlt config providers are also supported as usual, like:

1. environ provider
2. `.dlt/config.toml` provider, including the global config
3. `.dlt/<profile_name>.secrets.toml`, which is the secrets toml provider but scoped to a particular profile. A per-profile version (`dev.secrets.toml`) is sought instead of the `secrets.toml` file.

### Python interface to use and share the data

dlt packages provide a Python interface to access the data. This is done by exposing a standard Python API which provides connection to the production data through a profile called "access". The access to data can be limited by applying data and schema contracts on top of dlt datasets.

