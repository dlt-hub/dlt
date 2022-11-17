---
sidebar_position: 4
---

# Architecture

![architecture-diagram](/img/architecture-diagram.png)

## Extract

The Python script requests data from an API or a similar source. Once this data is recieved, 
the script provides the JSON or a function that produces JSON to `dlt` as input, which then 
normalizes that data.

## Normalize

The configurable normalization engine in `dlt` recursively unpacks this nested structure into 
relational tables (i.e. inferring data types, linking tables to create parent-child relationships, 
and more), making it ready to be loaded.

## Load

The data is then loaded into your destination of choice. `dlt` uses configurable, idempotent, atomic 
loads that ensure data safely ends up where you want it, like you want it. For example, you don't need 
to worry about the size of the data you are loading and if the process is interrupted, it is safe to 
retry without creating errors.