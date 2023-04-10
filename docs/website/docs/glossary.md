---
title: Glossary
description: Glossary of common dlt terms
keywords: [glossary, resource, source, pipeline]
---

# Glossary

## [Resource](./concepts/resource.md)

  If the source is an API, then a resource is an endpoint in that API. If the source is a
  spreadsheet, then a resource is a tab in that spreadsheet. If the source is a database,
  then a resource is a table in that database. A source is organized into one or more resources.

## [Source](./concepts/source.md)

  If endpoints in an API are the resources, then the API is the source. If tabs in a spreadsheet
  are the resources, then the source is the spreadsheet. If tables in a database are the resources,
  then the source is the database. A source is organized into one or more resources.

## [Destination](./destinations.md)

  The data store where data from the source is loaded (e.g. Google BigQuery).

## [Pipeline](./concepts/pipeline.md)

  Moves the data from the source to the destination, according to instructions provided
  in the schema (i.e. extracting, normalizing, and loading the data).

## [Schema](./concepts/schema.md)

  Describes the structure of normalized data (e.g. unpacked tables, column types, etc.) and provides instructions on how the data should be processed and loaded (i.e. it tells `dlt` about the content
  of the data and how to load it into the destination).

## [Config](./customization/configuration.md)

  A set of values that are passed to the pipeline at run time (e.g. to change its behavior locally
  vs. in production).

## [Credentials](./customization/credentials.md)

  A subset of configuration whose elements are kept secret and never shared in plain text.