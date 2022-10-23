## importing dlt
Basic `dlt` functionalities are imported with `import dlt`. Those functionalities are:
1. ability to run the pipeline (which means extract->normalize->load for particular source(s) and destination) with `dlt.run`
2. ability to configure the pipeline ie. provide alternative pipeline name, working directory, folders to import/export schema and various flags: `dlt.pipeline`
3. ability to decorate sources (`dlt.source`) and resources (`dlt.resources`)
4. ability to access secrets `dlt.secrets` and config values `dlt.config`

## importing destinations
We support a few built in destinations which may be imported as follows
```python
import dlt
from dlt.destinations import bigquery
from dlt.destinations import redshift
```

The imported modules may be directly passed to `run` or `pipeline` method. The can be also called to provide credentials and other settings explicitly (discouraged) ie. `bigquery(Service.credentials_from_file("service.json"))` will work.

Destinations require `extras` to be installed, if that is not the case, an exception with user friendly message will tell how to do that.

## importing sources
We do not have any structure for the source repository so IDK. For `create pipeline` workflow the source is in the same script as `run` method.


## default and explicitly configured pipelines
When the `dlt` is imported a default pipeline is automatically created. That pipeline is configured via configuration providers (ie. `config.toml` or env variables). If no configuration is present, default values will be used.

1. the name of the pipeline, the name of default schema (if not overridden by the source extractor function) and the default dataset (in destination) are set to **current module name** which in 99% of cases is the name of executing script
2. the working directory of the pipeline will be **OS temporary folder/pipeline name**
3. the logging level will be **INFO**
4. all other configuration options won't be set or will have default values.

Pipeline can be explicitly created and configured via `dlt.pipeline()` that returns `Pipeline` object. All parameters are optional. If no parameter is provided then default pipeline is returned. Here's a list of options:
1. pipeline_name
2. working_dir
3. pipeline_secret - for deterministic hashing
4. destination - the imported destination module or module name (we accept strings so they can be configured)
5. import_schema_path
6. export_schema_path
7. full_refresh - if set to True all the pipeline working dir and all datasets will be dropped with each run
8. ...any other popular option... give me ideas. maybe `dataset_name`?

> **Achtung** as per `secrets_and_config.md` the options passed in the code have **lower priority** than any config settings. Example: the pipeline name passed to `dlt.pipeline()` will be overwritten if `pipeline_name` is present in `config.toml` or `PIPELINE_NAME` is in config variables.


> It is possible to have several pipelines in a single script if many pipelines are configured via `dlt.pipeline()`. I think we do not want to train people on that so I will skipp the topic.

## pipeline working directory


## the default schema and the default data set

## running pipelines and `dlt.run` + `@source().run` functions
`dlt.run` + `@source().run` are shortcuts to `Pipeline::run` method on default or last configured (with `dlt.pipeline`) `Pipeline` object.

The function takes the following parameters
1. source - required - the data to be loaded into destination: a `@dlt.source` or a list of those, a `@dlt.resource` or a list of those, an iterator/generator function or a list of those or iterable (ie. a list) holding something else that iterators.
2. destination
3. dataset
4. table_name, write_disposition etc. - only when data is: a single resource, an iterator (ie. generator function) or iterable (ie. list)
5. schema - a `Schema` instance to be used instead of schema provided by the source or the default schema

The `run` function works as follows.
1. if there's any pending data to be normalized or loaded, this is done first.
2. only when successful more data is extracted
3. only when successful newly extracted data is normalized and loaded.

extract / normalize / load are atomic. the `run` is as close to be atomic as possible.

> `load` is atomic if SQL transformations ie in `dbt` and all the SQL queries take into account only committed `load_ids`. It is certainly possible - we did in for RASA but requires some work... Maybe we implement a fully atomic staging at some point in the loader.


## the `Pipeline` object

## Examples

Loads data from `taktile_data` source function into bigquery. All the credentials amd credentials are taken from the config and secret providers.

Script was run with `python taktile.py`

```python
from my_taktile_source import taktile_data
from dlt.destinations import bigquery

# I only want logs from the resources present in taktile_data
taktile_data.run(source=taktile_data(1).select("logs"), destination=bigquery)
```

pipeline name is explicitly configured.

## command line interface
I need concept for that. Commands that we need:

1. `dlt init` to initialize new project and create project template for `create pipeline` use case. Should it also install `extras`?
2. `dlt deploy` to create deployment package (probably cron)

I have two existing working commands
1. `dlt schema` to load and parse schema file and convert it into `json` or `yaml`
2. `dlt pipeline` to inspect a pipeline with a specified name/working folder

We may also add:
1. `dlt run` or `dlt schedule` to run a pipeline in a script like cron would.

## pipeline runtime setup

1. logging
2. signals
3. unhandled exceptions