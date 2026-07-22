<h1 align="center">
    <strong>data load tool (dlt) — the open-source Python library that automates all your tedious data loading tasks</strong>
</h1>
<p align="center">
Be it a Google Colab notebook, AWS Lambda function, an Airflow DAG, your local laptop,<br/>or an AI coding agent—<strong>dlt</strong> can be dropped in anywhere.
</p>


<h3 align="center">

🚀 Join our thriving community of likeminded developers and build the future together!

</h3>

<div align="center">
  <a target="_blank" href="https://dlthub.com/community" style="background:none">
    <img src="https://img.shields.io/badge/slack-join-dlt.svg?labelColor=191937&color=6F6FF7&logo=slack" style="width: 260px;"  />
  </a>
</div>
<div align="center">
  <a target="_blank" href="https://pypi.org/project/dlt/" style="background:none">
    <img src="https://img.shields.io/pypi/v/dlt?labelColor=191937&color=6F6FF7">
  </a>
  <a target="_blank" href="https://pypi.org/project/dlt/" style="background:none">
    <img src="https://img.shields.io/pypi/pyversions/dlt?labelColor=191937&color=6F6FF7">
  </a>
  <a target="_blank" href="https://pypi.org/project/dlt/" style="background:none">
    <img src="https://img.shields.io/pypi/dm/dlt?labelColor=191937&color=6F6FF7">
  </a>
</div>

## Installation

dlt supports Python 3.10 through Python 3.14. Note that some optional extras are not yet available for Python 3.14, so support for this version is considered experimental.

```sh
pip install dlt
```

Add the extras you need for your sources and destinations, for example:

```sh
pip install "dlt[duckdb]"        # local DuckDB destination
pip install "dlt[bigquery]"      # or snowflake, postgres, redshift, databricks, athena, ...
pip install "dlt[s3]"            # or gs, az for cloud filesystems
pip install "dlt[sql_database]"  # read from any SQL database
pip install "dlt[hub]"           # data quality, transformations, and AI (see below)
```

Prefer [uv](https://docs.astral.sh/uv/)? `uv add "dlt[duckdb]"`.

## Quick Start

Describe an API declaratively and load it into DuckDB — dlt handles requests, pagination, schema inference, and typing for you:

```python
import dlt
from dlt.sources.rest_api import rest_api_source

# 1. Describe the API declaratively
source = rest_api_source({
    "client": {"base_url": "https://pokeapi.co/api/v2/"},
    "resources": [
        {"name": "pokemon", "endpoint": {"path": "pokemon", "params": {"limit": 1000}}},
    ],
})

# 2. Point a pipeline at any destination
pipeline = dlt.pipeline(
    pipeline_name="pokemon",
    destination="duckdb",
    dataset_name="pokemon_data",
)

# 3. Extract, normalize, and load
print(pipeline.run(source))

# 4. ...and read it straight back as a DataFrame
print(pipeline.dataset().pokemon.df())
```

...or load any Python iterable — a [resource](https://dlthub.com/docs/general-usage/resource) is just a generator, and dlt infers the schema, types the columns, and writes the table:

```python
import dlt

@dlt.resource(table_name="players", primary_key="id", write_disposition="merge")
def players():
    yield {"id": 1, "name": "Magnus", "rating": 2839}
    yield {"id": 2, "name": "Pragg", "rating": 2758}

dlt.pipeline(destination="duckdb", dataset_name="chess").run(players())
```

Try it out in our **[Colab Demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)** or directly on our wasm-based [playground](https://dlthub.com/docs/tutorial/playground) in our docs.

## Why dlt

**dlt** loads data from messy, often unstructured sources into well-structured, typed datasets. It's a **library, not a platform** — you `pip install` it into your existing code and keep your workflow and the other tools you already use. No black boxes: clean Pythonic interfaces, human-readable file formats, schemas you can inspect, no hidden side effects.

dlt and its docs are **built from the ground up for LLMs and coding agents**. Pair the typed, declarative primitives below with [dlthub.com/context](https://dlthub.com/context) and the [LLM-native workflow](https://dlthub.com/docs/dlt-ecosystem/llm-tooling/llm-native-workflow) to go from prompt to working pipeline — across [5000+ sources](https://dlthub.com/workspace) — often in a single shot.

## Extract from any source

**REST APIs** — describe the endpoints declaratively; filter, map, and flatten records right at the source ([docs](https://dlthub.com/docs/tutorial/rest-api)):

```python
from dlt.sources.rest_api import rest_api_source

source = rest_api_source({
    "client": {
        "base_url": "https://api.example.com/v1",
        "paginator": {"type": "cursor", "cursor_path": "next_cursor"},
    },
    "resources": [
        {
            "name": "guests",
            "endpoint": {"path": "events/guests"},
            "processing_steps": [
                {"filter": lambda r: r["approval_status"] == "approved"},
                {"map": lambda r: {**r, "email": r["email"].lower()}},
            ],
        },
    ],
})
```

**SQL databases** — reflect tables and types straight from the database ([docs](https://dlthub.com/docs/tutorial/sql-database)):

```python
from dlt.sources.sql_database import sql_database

source = sql_database("mysql+pymysql://user:pass@host/db")
```

**Files in any bucket** — list, then parse CSV / JSONL / Parquet from local disk, S3, GCS, or Azure ([docs](https://dlthub.com/docs/tutorial/filesystem)):

```python
from dlt.sources.filesystem import filesystem, read_csv_duckdb

source = (
    filesystem(bucket_url="s3://my-bucket/data", file_glob="*.csv")
    | read_csv_duckdb()
).with_name("events")
```

**DataFrames & Arrow** — pandas, Polars, and Arrow tables load directly; Arrow-backed frames move with zero copies:

```python
import dlt
import pandas as pd

df = pd.DataFrame({"event": ["dlt summit", "DuckCon"], "signups": [1240, 860]})
dlt.pipeline(destination="duckdb", dataset_name="events").run(df, table_name="events")
```

See [many more sources](https://dlthub.com/docs/dlt-ecosystem/verified-sources) in the ecosystem.

## Load to 20+ destinations — swap one string

The same resource runs anywhere. Change the `destination` string and dlt takes care of credentials, DDL in the target dialect, staging, and schema drift:

```python
pipeline = dlt.pipeline(
    pipeline_name="luma",
    destination="duckdb",       # → snowflake, bigquery, postgres, redshift, databricks,
    dataset_name="luma_data",   #   athena, clickhouse, motherduck, filesystem (S3/GCS/Azure),
)                               #   iceberg, delta, ... and custom reverse-ETL destinations
pipeline.run(source)
```

dlt handles the parts you'd rather not:

- **Credentials** → `secrets.toml` / env vars, injected automatically
- **DDL** → `CREATE TABLE` in the target's dialect
- **Type mapping** → source types converted to the destination's types
- **Staging** → S3 / GCS for warehouses that need it
- **Schema drift** → `ALTER TABLE` on the fly

Browse all [supported destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/), or build a [custom one](https://dlthub.com/docs/dlt-ecosystem/destinations/destination).

## Declare intent with decorators

Decorators let you declare *what* you want — incremental loading, merge strategies, schema contracts, column hints — instead of hand-rolling it. Every knob can be overridden at runtime ([docs](https://dlthub.com/docs/general-usage/resource)):

```python
import dlt

@dlt.resource(
    primary_key="id",
    write_disposition="merge",                       # upsert on the primary key
    columns={"email": {"x-annotation-pii": True}},   # type and annotate columns
    schema_contract={"columns": "freeze"},           # reject unexpected columns
)
def events(
    updated_at=dlt.sources.incremental("updated_at"),  # load only new/changed rows
):
    yield from fetch_events(since=updated_at.last_value)


@dlt.source
def luma(api_key: str = dlt.secrets.value):
    return events(), guests()   # group one or more resources behind shared config/auth
```

[**Schema contracts**](https://dlthub.com/docs/general-usage/schema-contracts) enforce the shape at the gate, with three modes — `evolve` (accept and adapt the schema), `freeze` (reject the record), and `discard` (drop the offending row/column) — applied independently to `tables`, `columns`, and `data_type`. You also get [schema inference](https://dlthub.com/docs/general-usage/schema), [normalization of nested data](https://dlthub.com/docs/general-usage/schema/#data-normalizer), [incremental loading](https://dlthub.com/docs/general-usage/incremental-loading), and [secrets & config injection](https://dlthub.com/docs/general-usage/credentials) out of the box.

## Read your data back: the Dataset API

A pipeline is durable. Reconnect to one by name with `dlt.attach` and read any table back in the shape that fits your tool ([docs](https://dlthub.com/docs/general-usage/dataset-access/)):

```python
import dlt

pipeline = dlt.attach(pipeline_name="luma", destination="duckdb", dataset_name="luma_data")

dataset = pipeline.dataset()
dataset.tables               # ['events', 'guests', ...]

guests = dataset.guests      # a lazy dlt.Relation
guests.df()                  # pandas DataFrame
guests.arrow()               # pyarrow.Table (zero-copy)
guests.to_ibis()             # ibis expression — lazy, composable
```

## Transform with Ibis — Python in, SQL out

Lift any loaded table into an [Ibis](https://ibis-project.org/) expression, compose group-bys, joins, and window functions in Python, and let dlt compile it to SQL in the destination's dialect. Nothing runs until you ask for the result:

```python
import ibis

guests = pipeline.dataset().guests.to_ibis()

guests_by_event = (
    guests
    .group_by("event_id")
    .aggregate(n_guests=ibis._.api_id.count())
)

guests_by_event.to_pyarrow()   # compiles to SQL and runs on the destination
```

dlt also supports [Python and SQL data access](https://dlthub.com/docs/general-usage/dataset-access/), [transformations](https://dlthub.com/docs/dlt-ecosystem/transformations), [pipeline inspection](https://dlthub.com/docs/general-usage/dashboard), and [visualizing data in Marimo notebooks](https://dlthub.com/docs/general-usage/dataset-access/marimo).

## Documentation

For detailed usage and configuration, please refer to the [official documentation](https://dlthub.com/docs).

## Examples

You can find examples for various use cases in the [examples](docs/examples) folder, or in the [code examples section](https://dlthub.com/docs/examples) of our docs page.

## Adding as dependency

`dlt` follows the semantic versioning with the [`MAJOR.MINOR.PATCH`](https://peps.python.org/pep-0440/#semantic-versioning) pattern.

* `major` means breaking changes and removed deprecations
* `minor` new features, sometimes automatic migrations
* `patch` bug fixes

We suggest that you allow only `patch` level updates automatically using the [Compatible Release Specifier](https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release). For example **dlt~=1.23.0** allows only versions **>=1.23.0** and less than **<1.24.0**

Please also see our [release notes](https://github.com/dlt-hub/dlt/releases) for notable changes between versions.

## Get Involved

The dlt project is quickly growing, and we're excited to have you join our community! Here's how you can get involved:

- **Connect with the Community**: Join other dlt users and contributors on our [Slack](https://dlthub.com/community)
- **Report issues and suggest features**: Please use the [GitHub Issues](https://github.com/dlt-hub/dlt/issues) to report bugs or suggest new features. Before creating a new issue, make sure to search the tracker for possible duplicates and add a comment if you find one.
- **Track progress of our work and our plans**: Please check out our [public Github project](https://github.com/orgs/dlt-hub/projects/9)
- **Improve documentation**: Help us enhance the dlt documentation.

## Contribute code
Please read [CONTRIBUTING](CONTRIBUTING.md) before you make a PR.

- 📣 **New destinations are unlikely to be merged** due to high maintenance cost (but we are happy to improve SQLAlchemy destination to handle more dialects)
- Significant changes require tests and docs and in many cases writing tests will be more laborious than writing code
- Bugfixes and improvements are welcome! You'll get help with writing tests and docs + a decent review.

## Sponsors

<p>
  <a href="https://blacksmith.sh/?utm_source=dlt&utm_medium=readme&utm_campaign=sponsorship" target="_blank">
    <img src=".github/assets/blacksmith-logo.svg" alt="Blacksmith" width="240" />
  </a>
</p>

[Blacksmith](https://blacksmith.sh/?utm_source=dlt&utm_medium=readme&utm_campaign=sponsorship) is a drop-in replacement for GitHub-hosted runners that speed up our CI/CD pipelines by 2x and up to 75% cheaper. We're grateful to Blacksmith for sponsoring us with free CI/CD minutes--which helps us keep builds fast and our costs lower.

## License

`dlt` is released under the [Apache 2.0 License](LICENSE.txt).
