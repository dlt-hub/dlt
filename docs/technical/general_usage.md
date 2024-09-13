# General usage
marks features that are:

⛔ not implemented, hard to add

☮️ not implemented, easy to add

## importing dlt
Basic `dlt` functionalities are imported with `import dlt`. Those functionalities are:
1. ability to run the pipeline (which means extract->normalize->load for particular source(s) and destination) with `dlt.run`
2. ability to configure the pipeline ie. provide alternative pipeline name, working directory, folders to import/export schema and various flags: `dlt.pipeline`
3. ability to decorate sources (`dlt.source`) and resources (`dlt.resource` , `dlt.transformer`)
4. ability to access secrets `dlt.secrets` and config values `dlt.config`

## importing destinations
We support a few built in destinations which may be imported as follows
```python
import dlt
from dlt.destinations import bigquery
from dlt.destinations import redshift
```

The imported modules may be directly passed to `run` or `pipeline` method. ⛔ They can be also called to provide credentials and other settings explicitly (discouraged) ie. `bigquery(Service.credentials_from_file("service.json"))` will bind the credentials to the module.

Destinations require `extras` to be installed, if that is not the case, an exception with user friendly message will tell how to do that.

## importing sources
We do not have any built in or contributed sources and no methods to distribute them implemented.

☮️ the built in sources can be imported from `dlt.sources` namespaces
⛔ the contributed sources can be added as python code modules via `dlt init <source> <destination>`

## default and explicitly configured pipelines
When the `dlt` is imported a default pipeline is automatically created. That pipeline is configured via configuration providers (ie. `config.toml` or env variables - see [secrets_and_config.md](secrets_and_config.md)). If no configuration is present, default values will be used.

1. the name of the pipeline, the name of default schema (if not overridden by the source extractor function) and the default dataset (in destination) are set to **current module name** with `dlt_` prefix, which in 99% of cases is the name of executing python script. Example: for `pipeline.py` the default names are `dlt_pipeline`.
2. the working directory of the pipeline will be (1) for non-root user with home directory (99% of cases) **~/.dlt/pipelines/pipeline name** (2) for root users (Linux/Mac OS): **/var/dlt/pipelines/pipeline name** (3) for users without home directory: **OS temp dir/dlt/pipelines/pipeline name**`
3. ☮️ the system logging level will be **CRITICAL** (disabled) (**WARNING** in alpha)
4. all other configuration options won't be set or will have default values.
5. ⛔ the user log will be enabled

Pipeline can be explicitly created and configured via `dlt.pipeline()` that returns `Pipeline` object. All parameters are optional. If no parameter is provided then default pipeline is returned. Here's a list of options. All the options are configurable.
1. pipeline_name - default as above
2. working_dir - default as above
3. pipeline_secret - for deterministic hashing - default is random number
4. destination - the imported destination module or module name (we accept strings so they can be configured) - default is None
4. dataset_name - name of the dataset where the data goes (see later the default names)
5. import_schema_path - default is None
6. export_schema_path - default is None
7. dev_mode - if set to True the pipeline working dir will be erased and the dataset name will get the unique suffix (current timestamp). ie the `my_data` becomes `my_data_20221107164856`.

> **Achtung** as per `secrets_and_config.md` the arguments passed to `dlt.pipeline` are configurable and if skipped will be injected by the config providers. **the values provided explicitly in the code have a full precedence over all config providers**

Some pipeline setting may be configured only via config providers ie `config.toml`. A few interesting settings
```toml
# Enables the `run` method of the `Pipeline` object to restore the pipeline state and schemas from the destination
restore_from_destination=true
# Stores all schemas in single dataset. When False, each schema will get a separate dataset with `{dataset_name}_{schema_name}
use_single_dataset=true
```

> It is possible to have several pipelines in a single script if many pipelines are configured via `dlt.pipeline()`.

## the default schema and the default dataset name
`dlt` has following rules when auto-generating schemas and naming the dataset to which the data will be loaded.

**schemas are identified by schema names**

**default schema** is the first schema that is provided or created within the pipeline. First schema comes in the following ways:
1. From the first extracted `@dlt.source` ie. if you `dlt.run(data=spotify(), ...)` and `spotify` source has schema with name `spotify` attached, it will be used as default schema.
2. it will be created from scratch if you extract a `@dlt.resource` or an iterator ie. list (example: `dlt.run(data=["a", "b", "c"], ...)`) and its name is the pipeline name or generator function name if generator is extracted. (I'm trying to be smart with automatic naming)
3. it is explicitly passed with the `schema` parameter to `run` or `extract` methods - this forces all the sources regardless of the form to place their tables in that schema.

The **default schema** comes into play when we extract data as in point (2) - without schema information. in that case the default schema is used to attach tables coming from that data

The pipeline works with multiple schemas. If you extract another source or provide schema explicitly, that schema becomes part of pipeline. Example
```python

p = dlt.pipeline(dataset_name="spotify_data_1")
p.extract(spotify("me"))  # gets schema "spotify" from spotify source, that schema becomes default schema
p.extract(echonest("me").with_resources("mel"))  # get schema "echonest", all tables belonging to resource "mel" will be placed in that schema
p.extract([label1, label2, label3], name="labels")  # will use default schema "spotitfy" for table "labels"
```

> learn more on how to work with schemas both via files and programmatically in [working_with_schemas.md](working_with_schemas.md)

### The dataset name and dataset layout at the destination

`dlt` will load data to a specified dataset in the destination. The dataset in case of bigquery is a native dataset, in case of redshift is a native database schema.

**By default, one dataset can handle multiple schemas**.
The pipeline configuration option `use_single_dataset` controls the dataset layout in the destination. By default it is set to True. In that case only one dataset is created at the destination - by default dataset name which is the same as pipeline name. The dataset name can also be explicitly provided into `dlt.pipeline` `dlt.run` and `Pipeline::load` methods.
All the tables from all the schemas are stored in that dataset. The table names are **not prefixed** with schema names!. If there are any name collisions, tables in the destination will be unions of the fields of all the tables with same name in the schemas.

**Enabling one dataset per schema layout**
If you set `use_single_dataset` to False:

In case **there's only (default) schema** in the pipeline, the data will be loaded into dataset with dataset name. Example: `dlt.run(spotify("me"), dataset_name="spotify_data_1")` will load data into dataset `spotify_data_1`).

In case **there are more schemas in the pipeline**, the data will be loaded into dataset with name `{dataset_name}` for default schema and `{dataset_name}_{schema_name}` for all the other schemas. For the example above:
1. `spotify` tables and `labels` will load into `spotify_data_1`
2. `mel` resource will load into `spotify_data_1_echonest`

The `dev_mode` option: dataset name receives a prefix with the current timestamp: ie the `my_data` becomes `my_data_20221107164856`. This allows a non destructive full refresh. Nothing is being deleted/dropped from the destination.

## pipeline working directory and state
Another fundamental concept is the pipeline working directory. This directory keeps the following information:
1. the extracted data and the load packages with jobs created by normalize
2. the current schemas with all the recent updates
3. the pipeline and source state files.

the working directory of the pipeline may be set by the user and the default is explained in **default and explicitly configured pipelines** above.

### restore the pipeline working directory from the destination

The `restore_from_destination` argument to `dlt.pipeline` let's the user restore the state and the pipeline schemas from the destination. `dlt` will check if the state is stored in the destination, download it and then download all the required schemas. after that data extraction may be restarted.

The state is being stored in the destination together with other data. So only when all pipeline stages are completed the state is available for restoration.

The pipeline cannot be restored if `dev_mode` flag is set.

The other way to trigger full refresh is to drop destination dataset. `dlt` detects that and resets the pipeline local working folder.

## Data sources understood by `dlt`
The `run` and `extract` method understand the following
1. `dlt.source`, `dlt.resource` and lists of them
2. lists, iterators, generators of json-serializable objects (+datetime, decimal, uuid types)
3. ⛔ panda frames, pyarrow tables are processed natively ☮️ are processed as iterators
4. ☮️ `uri` strings representing built is sources (`file.json`, `file.parquet`, `s3://bucket/file.json`)

## running pipelines and `dlt.run` + `@source().run` functions
`dlt.run` + `@source().run` are shortcuts to `Pipeline::run` method on default or last configured (with `dlt.pipeline`) `Pipeline` object. Please refer to [create_pipeline.md](create_pipeline.md) for examples.

The function takes the following parameters
1. data - required - the data to be loaded into destination: a `@dlt.source` or a list of those, a `@dlt.resource` or a list of those, an iterator/generator function or a list of those or iterable (ie. a list) holding something else that iterators.
2. destination
3. dataset name
4. table_name, write_disposition etc. - only when data is: a single resource, an iterator (ie. generator function) or iterable (ie. list)
5. schema - a `Schema` instance to be used instead of schema provided by the source or the default schema
6. credentials - if you want to provide credentials explicitly you pass them here

The `run` function works as follows.
1. if there's any pending data to be normalized or loaded, this is done first.
2. only when successful more data is extracted
3. only when successful newly extracted data is normalized and loaded.

extract / normalize / load are atomic. the `run` is as close to be atomic as possible.

the `run` and `load` return information on loaded packages: to which datasets, list of jobs etc. let me think what should be the content

> `load` is atomic if SQL transformations ie in `dbt` and all the SQL queries take into account only committed `load_ids`. It is certainly possible - we did in for RASA but requires some work... Maybe we implement a fully atomic staging at some point in the loader.

### Naming Conventions and Normalization
The default json normalizer will convert json documents into tables. All the key names (thus all tables and columns) will be normalized according to the configured naming convention.

❗ [more here](working_with_schemas.md)

### Dev mode mode
If `dev_mode` flag is passed to `dlt.pipeline` then
1. the pipeline working dir is fully wiped out (state, schemas, temp files)
2. dataset name receives a prefix with the current timestamp: ie the `my_data` becomes `my_data_20221107164856`.
3. pipeline will not be restored from the destination

This allows a non destructive full refresh. Nothing is being deleted/dropped from the destination. You have a new dataset with a fresh shema

### The LoadInfo return value
The run method returns `LoadInfo` tuple with information what was actually loaded. The exact content is in the works. It currently contains:
1. the destination information
2. the dataset name
3. the list of package ids that got loaded
4. the list of failed jobs per package id

This needs to be extended.

### Run Exceptions
The `run`, `extract`, `normalize` and `load` method raise `PipelineStepFailed` when one of the steps in pipeline failed. Failed jobs do not raise exceptions.


### Retrying
⛔ There's no retry built in. (except the sources and some destinations)

> should we add it? I have a runner in `dlt` that would be easy to modify


## pipeline runtime setup

1. logging - creates logger with the name `dlt` which can be disabled the python way if someone does not like it. (contrary to `dbt` logger which is uncontrollable mess)
2. signals - signals required to gracefully stop pipeline with CTRL-C, in docker, kubernetes, cron are handled. signals are not handled if `dlt` runs as part of `streamlit` app or a notebook.
3. unhandled exceptions - we do not catch unhandled exceptions... but we may do that if run in standalone script.