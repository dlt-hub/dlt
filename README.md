![](docs/DLT-Pacman-Big.gif)

<p align="center">

[![PyPI version](https://badge.fury.io/py/python-dlt.svg)](https://pypi.org/project/python-dlt/)
[![LINT Badge](https://github.com/scale-vector/dlt/actions/workflows/lint.yml/badge.svg)](https://github.com/scale-vector/dlt/actions/workflows/lint.yml)
[![TEST COMMON Badge](https://github.com/scale-vector/dlt/actions/workflows/test_common.yml/badge.svg)](https://github.com/scale-vector/dlt/actions/workflows/test_common.yml)
[![TEST REDSHIFT Badge](https://github.com/scale-vector/dlt/actions/workflows/test_loader_redshift.yml/badge.svg)](https://github.com/scale-vector/dlt/actions/workflows/test_loader_redshift.yml)
[![TEST GCP Badge](https://github.com/scale-vector/dlt/actions/workflows/test_loader_gcp.yml/badge.svg)](https://github.com/scale-vector/dlt/actions/workflows/test_loader_gcp.yml)

</p>

# Data Load Tool (DLT)

Data Load Tool (DLT) enables simple, python-native data pipelining for data professionals. It is an open source, scalable data loading framework that does not require any help from DevOps.

Learn more with the **[Quickstart guide](QUICKSTART.md)** and check out **[Example Sources](examples/README.md)** to get started.

## How does it work?

DLT aims to simplify data loading for everyone.


To achieve this, we take into account the progressive steps of data pipelining:

![](docs/DLT_Diagram_1.jpg)
### 1. Data discovery, typing, schema, metadata

When we create a pipeline, we start by grabbing data from the source.

Usually, the source metadata is lacking, so we need to look at the actual data to understand what it is and how to ingest it.

In order to facilitate this, DLT includes several features
* Auto-unpack nested json if desired
* generate an inferred schema with data types and load data as-is for inspection in your warehouse.
* Use an adjusted schema for follow up loads, to better type and filter your data after visual inspection (this also solves dynamic typing of Pandas dfs)

### 2. Safe, scalable loading

When we load data, many things can intrrerupt the process, so we want to make sure we can safely retry without generating artefacts in the data.

Additionally, it's not uncommon to not know the data size in advance, making it a challenge to match data size to loading infrastructure.

With good pipelining design, safe loading becomes a non-issue.

* Idempotency: The data pipeline supports idempotency on load, so no risk of data duplication.
* Atomicity: The data is either loaded, or not. Partial loading occurs in the s3/storage buffer, which is then fully committed to warehouse/catalogue once finished. If something fails, the buffer is not partially-committed further.
* Data-size agnostic: By using generators (like incremental downloading) and online storage as a buffer, it can incrementally process sources of any size without running into worker-machine size limitations.


### 3. Modelling and analysis

* Instantiate a dbt package with the source schema, enabling you to skip the dbt setup part and go right to SQL modelling.


### 4. Data contracts

* If using an explicit schema, you are able to validate the incoming data against it. Particularly useful when ingesting untyped data such as pandas dataframes, json from apis, documents from nosql etc.

### 5. Maintenance & Updates

* Auto schema migration: What do you do when a new field appears, or if it changes type? With auto schema migration you can default to ingest this data, or throw a validation error.

## Why?

Data loading is at the base of the data work pyramid.

The current ecosystem of tools follows an old paradigm where the data pipeline creator is a software engineer, while the data pipeline user is an analyst.

In the current world, the data analyst needs to solve problems end to end, including loading.

Currently there are no simple frameworks to achieve this, but only clunky applications that need engineering and devops expertise to run, install, manage and scale. The reason for this is often an artificial monetisation insert (open source but pay to manage).

Additionally, these existing loaders only load data sources for which somebody developed an extractor, requiring a software developer once again.

DLT aims to bring loading into the hands of analysts with none of the unreasonable redundacy waste of the modern data platform.

Additionally, the source schemas will be compatible across the community, creating the possiblity to share reusable analysis and modelling back to the open source community without creating tool-based vendor locks.


## Why not an OOP framework?

```
import this
```
[The zen of python](!https://peps.python.org/pep-0020/#the-zen-of-python) and the idea of "pythonic" programming ask for simplicity.

Data people operate at the intersection between business, mathematics and functional programming.

Such, the learning curve to write OOP sources in a very complex SDK is too steep for all but the most tech-interested.

In contast, DLT allows you to throw json or other iterable data into the database, with zero learning curve.

## Why not something simpler?

> “Everything should be as simple as it can be, but not simpler” – a scientist’s defense of art and knowledge – of lightness, completeness and accuracy.

DLT aims to make data pipelining as simple as possible by defaulting to simple loading.

At the same time, it can also be configured for high parallelism, incremental loading, custom types, performance hints, keys, filtering fields in/out, schema migration strategies, etc.

It can also test the data or initiate a dbt package.

In short, DLT does not require any specialist knowledge to use

Advanced, commercial-grade use of DLT requires only some configuration.



