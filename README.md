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

The goal of DLT is to make it easier and faster to build low-maintenance data pipelines.

It's designed to run anywhere (Python 3.8+), so it can fit into your existing workflows or be scheduled independently (e.g. using GitHub Actions)

Learn more with the **[Quickstart Guide](QUICKSTART.md)** and check out **[Example Sources](examples/README.md)** to get started.

## Who is DLT for?

DLT is for data professionals who use Python to build pipelines.
- DLT enables anyone who knows a bit of Python to build commercial-grade pipelines
- DLT minimises maintenance requirements by design
- The DLT community shares pipelines with each other, so frequently you'll be able to just use a pipeline created by someone else

## Why use DLT?

DLT takes the Python scripts you already write to the next level with a library you install using pip in order to create highly scalable, easily maintainable pipelines that are straightforward to deploy.

If you end up creating your own pipeline in python, then create it with DLT and benefit from
- Automatic schema maintenance (i.e. schema inference, deployment, evolution, data contracts, etc.)
- configurable loading - append, replace, or merge.
- configurable normalisation engine - decide how to unpack nested documents or specify date formats
- commercial-grade engineering - dlt implements data engineering best practices such as idempotent, atomic loading.

## How does it work?

DLT aims to simplify data loading for everyone.

To achieve this, we take into account the progressive steps of data pipelining:

![](docs/DLT-Diagram_2.png)
### 1. Data extraction

DLT accepts json and json-producing functions as input, including nested, unstructured data.

### 2. Data normalisation

DLT features a configurable normalisation engine - it can recursively unpack nested structures into relational tables, or handle various data type conversions.

### 3. Data loading

When we load data, many things can interrupt the process, so we want to make sure we can safely retry without generating artifacts in the data.

Additionally, it's not uncommon to not know the data size in advance, making it a challenge to match data size to loading infrastructure.

With good pipelining design, safe loading becomes a non-issue.

* Schema evolution - configurable strategy for schema changes: automatic migration or fail&alert.
* Idempotency & Atomicity: Built in best practices of Atomic loading (all or nothing), and configurable Idempotency (do not double load, incurs a table "read" cost)
* Data-size agnostic: By using generators (like incremental downloading) and online storage as a buffer, it can incrementally process sources of any size without running into worker-machine size limitations.


## Why?

Data loading is at the base of the data work pyramid.

The current ecosystem of tools follows an old paradigm where the data pipeline creator is a software engineer, while the data pipeline user is an analyst.

In the real world, the data analyst needs to solve problems end to end, including loading.

Currently, there are no simple frameworks to achieve this, but only clunky applications that need engineering and devops expertise to run, install, manage and scale.

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
