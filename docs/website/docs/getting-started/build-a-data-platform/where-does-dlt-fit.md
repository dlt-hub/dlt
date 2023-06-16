---
title: How dlt fits in the data landscape today
description: where should you plug in dlt?
keywords: [build with dlt]
---

# How `dlt` fits in the data landscape today

## Data warehouse: Structure data during load

`dlt` is a data loading tool that transitions unstructured data into structured.

With data warehouses, this structuring is traditionally done manually by data engineers. They usually download the data, look at it, then decide which field should be loaded and how, then convert it etc. Sometimes, the data engineer loads the JSON directly as a string, and then all this structuring is done by the analytics engineer.

This is a labor-intensive process that can be automated. By using `dlt` as your solution for loading data, you can both, **accelerate development and reduce maintenance.**
![ETL image](/img/docs_where_does_dlt_fit_in_ETL_pipeline.png)

With `dlt`, you can serve all loading use-cases:
- Use existing pipelines.
- Build simple pipelines by passing data to the `dlt` loader.
- Build advanced pipelines by using `dlt`’s advanced features like extractors, retries, state, schema management, etc.

## Data lake: Feature engineering made easy

In a data lake, the data is often stored in its raw, unstructured form. As a data scientist, you'd first need to understand and explore this data, which could involve parsing through unstructured text files, handling missing or inconsistent data, normalizing disparate data sources, and more. By structuring your semi-structured dirty data during load, `dlt` makes the contents of your data immediately visible and accelerates the process of discovery.

### Why *structured* data lakes are better than unstructured dumps
- **Easier data exploration and understanding**: No more mystery about how data may differ from file to file.
- **Simplified data prep**: No more type-casting, cleaning, etc.
- **Faster and less labor-intensive structuring process.**
- **Higher quality features**: Leave the garbage at the door: Robust clean data in, robust clean data out.
- **Efficient use of resources**: Scanning and using unstructured data on the fly is expensive.
- **Scalability**: No more schema drift and maintenance events around unknown schemas. With inference and evolution, we can structure unknown data.

## Data lakehouse: Structure data for usage

The data lakehouse offers a way to simultaneously store unstructured data, while also enabling it to be structured during usage.

Not all data should be structured. For example, to be able to extract specific features from raw text or images, they may need to be used as-is. In such cases, data lakehouses help us by offering the best of both worlds, lakes and warehouses.

`dlt` fits well here in doing the conversion from unstructured data to structured.
- It can structure semi-structured data for usage.
- It can structure the data extracted from the unstructured dump.
    - For example, we could ask ChatGPT to extract JSON from text docs, and then load that JSON using `dlt`.

## Data platforms

`dlt` is a library, meaning you can use different parts of it for different purposes without having to run any overhead app.

So, in general, you can do a lot with `dlt` in a data platform:

- You could plug `dlt`’s pipeline into your existing pipeline as a “loader” that will structure your data and maintain schemas.
- You could use `dlt`’s extraction helpers to extract data more efficiently.
- You could use `dlt`’s declarative loading config to materialize your data similarly to dbt.
- You could use `dlt`’s helpers for anonymization or data munging before loading.
- You could plug `dlt` at the end of your compute pipelines that save data back into a database.

Because you can just plug `dlt` into your stack, you can choose how you use it and how much of it you use.

## Case study: Alto

Special shout out to the creator of [Alto](https://github.com/z3z1ma/alto), a data platform powered by `dlt`.

- Initially, `dlt` was used as a sink for Singer taps data, to enable schema evolution.
- Later, `dlt` was also used to create extraction scripts, as it's a better designed technology.
- In the final stage, Alto contributed a few features back to `dlt`, to enable advanced usage in the Alto context. We are very happy to accept these contributions which push forward not just the tech, but also the vision.