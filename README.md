![](docs/DLT-Pacman-Big.gif)

<p align="center">

[![PyPI version](https://badge.fury.io/py/python-dlt.svg)](https://pypi.org/project/python-dlt/)
[![LINT Badge](https://github.com/scale-vector/dlt/actions/workflows/lint.yml/badge.svg)](https://github.com/scale-vector/dlt/actions/workflows/lint.yml)
[![TEST COMMON Badge](https://github.com/scale-vector/dlt/actions/workflows/test_common.yml/badge.svg)](https://github.com/scale-vector/dlt/actions/workflows/test_common.yml)
[![TEST REDSHIFT Badge](https://github.com/scale-vector/dlt/actions/workflows/test_loader_redshift.yml/badge.svg)](https://github.com/scale-vector/dlt/actions/workflows/test_loader_redshift.yml)
[![TEST BIGQUERY Badge](https://github.com/scale-vector/dlt/actions/workflows/test_loader_bigquery.yml/badge.svg)](https://github.com/scale-vector/dlt/actions/workflows/test_loader_bigquery.yml)

</p>

# Data Load Tool (DLT)

## What is DLT?

Data Load Tool (DLT) is an open source python library for building data pipelines.

It's goal is to enable faster building of low-maintenance data pipelines.

It's designed to run anywhere (python 3.8+) and it can fit in your existing workflows or be scheduled independently.

Learn more with the **[Quickstart Guide](QUICKSTART.md)** and check out **[Example Sources](examples/README.md)** to get started.

## Who is DLT for?

DLT is for data professionals who use Python to build pipelines.
- dlt enables non-engineers to build commercial-grade pipelines.
- dlt minimises maintenance by design.
- dlt is a better replacement for most home-baked custom pipelines.
- Community pipeline packages are a good resource for typical needs.

## Why use DLT?

DLT is the first python data-pipelining library, meant to be a pip-installable easy to use pipeline creation and deployment kit.

If you end up creating your own pipeline in python, then create it with DLT and benefit from
- automatic schema inference, deployment, evolution, and data contracts - so you do not have to maintain schemas
- configurable loading - append, replace, or merge.
- configurable normalisation engine - decide how to unpack nested documents or specify date formats
- commercial-grade engineering - dlt implements data engineering best practices such as idempotent, atomic loading.

## How does it work?

DLT aims to simplify data loading for everyone.

To achieve this, we take into account the progressive steps of data pipelining:

![](docs/DLT_Diagram_2.jpg)
### 1. Data extraction

DLT accepts json or json-producing functions as input, including nested, unstructured data.

### 2. Data normalisation

DLT features a configurable normalisation engine - it can recursively unpack nested structures into relational tables, or handle various data type conversions.

### 3. Low effort, Safe & scalable loading

When we load data, many things can intrrerupt the process, so we want to make sure we can safely retry without generating artefacts in the data.

Additionally, it's not uncommon to not know the data size in advance, making it a challenge to match data size to loading infrastructure.

With good pipelining design, safe loading becomes a non-issue.
* Schema evolution - configurable strategy for schema changes: automatic migration or fail&alert.
* Idempotency: The data pipeline supports idempotency on load, so no risk of data duplication.
* Atomicity: The data is either loaded, or not. Partial loading occurs in the s3/storage buffer, which is then fully committed to warehouse/catalogue once finished. If something fails, the buffer is not partially-committed further.
* Data-size agnostic: By using generators (like incremental downloading) and online storage as a buffer, it can incrementally process sources of any size without running into worker-machine size limitations.


## Why?

Data loading is at the base of the data work pyramid.

The current ecosystem of tools follows an old paradigm where the data pipeline creator is a software engineer, while the data pipeline user is an analyst.

In the real world, the data analyst needs to solve problems end to end, including loading.

Currently, there are no simple frameworks to achieve this, but only clunky applications that need engineering and devops expertise to run, install, manage and scale. The reason for this is often an artificial monetisation insert (open source but pay to manage).

DLT aims to simplify pipeline building, making it easier and faster to build low-maintenance pipelines with evolving schemas.

## Why not an OOP framework?

Data people operate at the intersection between business, mathematics and functional programming.

Such, the learning curve to write OOP sources in a very complex SDK/framework is too steep for all but the most tech-interested.

In contrast, DLT allows you to throw json or other iterable data into the database, with zero learning curve.


## Supported data warehouses

Google BigQuery:
```pip3 install python-dlt[gcp]```

Amazon Redshift:
```pip install python-dlt[redshift]```
