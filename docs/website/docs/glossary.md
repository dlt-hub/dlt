---
sidebar_position: 6
---

# Glossary

## [Resource](./walkthroughs/create-a-pipeline.md)

  If the source is an API, then the resource is an endpoint in that API. If the source is a
  database, then the resource is a table in that database. A source is organized into one 
  or more resources.

## [Source](./walkthroughs/create-a-pipeline.md)

  If endpoints in an API are the resources, then the API is the source. If tables in a database 
  are the resources, then the source is the database. A source is organized into one or more resources.

## [Destination](./walkthroughs/create-a-pipeline.md)

  The data store where data from the source is loaded (e.g. a data warehouse like Google BigQuery).

## [Pipeline](./walkthroughs/create-a-pipeline.md)

  Moves the data from the source to the destination, according to instructions provided 
  in the schema (i.e. extracting, normalizing, and loading the data).

## [Schema](./customization/schema.md)

  Describes the structure of data returned from the source and provides instructions on 
  how the data should be processed and loaded (i.e. it tells `dlt` about the content of the 
  data and how to load it into the destination).

## [Config](./customization/configuration.md)

  A set of values that are passed to the pipeline at run time (e.g. to change its behavior locally
  vs. in production).

## [Credentials](./customization/credentials.md)

  A subset of configuration whose elements are kept secret and never shared in plain text.