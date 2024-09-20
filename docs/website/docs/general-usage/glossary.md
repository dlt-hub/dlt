---
title: Glossary
description: Glossary of common dlt terms
keywords: [glossary, resource, source, pipeline]
---

# Glossary

## [Source](source)

A location that holds data with a certain structure, organized into one or more resources.

- If endpoints in an API are the resources, then the API is the source.
- If tabs in a spreadsheet are the resources, then the spreadsheet is the source.
- If tables in a database are the resources, then the database is the source.

Within this documentation, **source** also refers to the software component (i.e., a Python function)
that **extracts** data from the source location using one or more resource components.

## [Resource](resource)

A logical grouping of data within a data source, typically holding data of similar structure and
origin.

- If the source is an API, then a resource is an endpoint in that API.
- If the source is a spreadsheet, then a resource is a tab in that spreadsheet.
- If the source is a database, then a resource is a table in that database.

Within this documentation, **resource** also refers to the software component (i.e., a Python function)
that **extracts** the data from the source location.

## [Destination](../dlt-ecosystem/destinations)

The data store where data from the source is loaded (e.g., Google BigQuery).

## [Pipeline](pipeline)

Moves the data from the source to the destination, according to instructions provided in the schema
(i.e., extracting, normalizing, and loading the data).

## [Verified source](../walkthroughs/add-a-verified-source)

A Python module distributed with `dlt init` that allows creating pipelines that extract data from a
particular **Source**. Such a module is intended to be published in order for others to use it to
build pipelines.

A source must be published to become "verified," which means that it has tests, test data,
demonstration scripts, documentation, and the dataset produced was reviewed by a data engineer.

## [Schema](schema)

Describes the structure of normalized data (e.g., unpacked tables, column types, etc.) and provides
instructions on how the data should be processed and loaded (i.e., it tells `dlt` about the content
of the data and how to load it into the destination).

## [Config](credentials/setup#secrets.toml-and-config.toml)

A set of values that are passed to the pipeline at runtime (e.g., to change its behavior locally vs.
in production).

## [Credentials](credentials/complex_types)

A subset of configuration whose elements are kept secret and never shared in plain text.

