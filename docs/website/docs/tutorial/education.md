---
title: Education
description: Learn the fundamentals and advanced concepts of dlt with self-paced courses. Start with pipelines, resources, sources, pagination, schema, and state, then move on to custom sources, complex APIs, destinations, transformations, data contracts, performance optimization, and production deployment.
keywords: [dlt, fundamentals, advanced, tutorial, pipeline, python, data engineering, resources, sources, pagination, authentication, configuration, schema, state, incremental loading, write disposition, custom sources, custom destinations, transformations, data contracts, logging, tracing, performance optimization, deployment, airflow, lambda, github actions, dagster, duckdb]
---

# Welcome to self-paced `dlt` courses!

![simpsons-hello.gif](https://storage.googleapis.com/dlt-blog-images/dlt-fundamentals-course/dlt_fundamentals_img1.gif)

Here you’ll find two self-paced `dlt` courses designed to help you grow from beginner to advanced data engineer.  
- The **Fundamentals Course** introduces you to `dlt`, covering pipelines, resources, sources, configuration, schema, state, and incremental loading.  
- The **Advanced Course** takes you further, teaching you how to build custom sources and destinations, apply transformations, enforce data contracts, optimize performance.

## **`dlt` Fundamentals Course**

### Lesson 1: Quick Start [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_1_Quick_start.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_1_Quick_start.ipynb)

Discover what dlt is, run your first pipeline with toy data, and explore it like a pro using DuckDB, `sql_client`, and dlt datasets!

### Lesson 2: dlt Resources and Sources [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_2_dlt_sources_and_resources_Create_first_dlt_pipeline.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_2_dlt_sources_and_resources_Create_first_dlt_pipeline.ipynb)

Learn to run pipelines with diverse data sources (dataframes, databases, and REST APIs), 
master `dlt.resource`, `dlt.source`, and `dlt.transformer`, and create your first REST API pipeline!

### Lesson 3: Pagination & Authentication & dlt Configuration [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration.ipynb)


Since it is never a good idea to publicly put your API keys into your code, different environments have different methods to set and access these secret keys. `dlt` is no different.
Master pagination and authentication for REST APIs, explore dlt's RESTClient and manage secrets and configs.

### Lesson 4: Using dlt’s pre-built Sources and Destinations [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_4_Using_pre_build_sources_and_destinations.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_4_Using_pre_build_sources_and_destinations.ipynb)

Now that you took a data source and loaded it into a `duckdb` destination, it is time to look into what other possibilities `dlt` offers.
In this notebook we will take a look at pre-built verified sources and destinations and how to use them.

### Lesson 5: Write disposition and incremental loading [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_5_Write_disposition_and_incremental_loading.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_5_Write_disposition_and_incremental_loading.ipynb)


Learn to control data behavior with dlt write dispositions (Append, Replace, Merge), master incremental loading, and efficiently update and deduplicate your datasets.

### Lesson 6: How dlt works [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_6_How_dlt_works.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_6_How_dlt_works.ipynb)

Discover the magic behind `dlt`! Learn its three main steps — Extract, Normalize, Load — along with default behaviors and supported file formats.

### Lesson 7: Inspecting & Adjusting Schema [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_7_Inspecting_&_Adjusting_Schema.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_7_Inspecting_&_Adjusting_Schema.ipynb)



dlt creates and manages the schema automatically, but what if you want to control it yourself? Explore the schema and customize it to your needs easily with dlt!

### Lesson 8: Understanding Pipeline State & Metadata [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_8_Understanding_Pipeline_Metadata_and_State.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_8_Understanding_Pipeline_Metadata_and_State.ipynb)


After having learnt about pipelines and how to move data from one place to another. We now learn about information about the pipeline itself. Or, metadata of a pipeline that can be accessed and edited through dlt.
This notebook explores `dlt` states, what it collected and where this *extra* information is stored. It also expands a bit more on what the load info and trace in `dlt` is capable of.


## **Advanced `dlt` Course**

### **Lesson 1: Custom Sources – REST APIs & RESTClient** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_1_Custom_sources_RestAPI_source_and_RESTClient.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_1_Custom_sources_RestAPI_source_and_RESTClient.ipynb)

Learn how to build flexible REST API connectors from scratch using `@dlt.resource` and the powerful `RESTClient`. 

### **Lesson 2: Custom Sources – SQL Databases** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_2_Custom_sources_SQL_Databases_.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_2_Custom_sources_SQL_Databases_.ipynb)

Connect to any SQL-compatible database, reflect table schemas, write query adapters, and selectively ingest data using `sql_database`.

### **Lesson 3: Custom Sources – Filesystems & Cloud Storage** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_3_Custom_sources_Filesystem_and_cloud_storage.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_3_Custom_sources_Filesystem_and_cloud_storage.ipynb)
Build sources that read from local or remote files (S3, GCS, Azure).

### **Lesson 4: Custom Destinations – Reverse ETL** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_4_Destinations_Reverse_ETL.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_4_Destinations_Reverse_ETL.ipynb)
Use `@dlt.destination` to send data back to APIs like Notion, Slack, or Airtable. Learn batching, retries, and idempotent patterns.

### **Lesson 5: Transforming Data Before & After Load**[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_5_Transform_data_before_and_after_loading.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_5_Transform_data_before_and_after_loading.ipynb)

Learn when and how to apply `add_map`, `add_filter`, `@dlt.transformer`, or even post-load transformations via SQL or Ibis. Control exactly how your data looks.

### **Lesson 6: Write Disposition Strategies & Advanced Tricks** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_6_Write_disposition_strategies_&_Advanced_tricks.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_6_Write_disposition_strategies_&_Advanced_tricks.ipynb)
Understand how to use `replace` and `merge`, and combine them with schema hints and incremental loading. 

### **Lesson 7: Data Contracts** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_7_Data_Contracts.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_7_Data_Contracts.ipynb)
Define expectations on schema, enforce data types and behaviors, and lock down your schema evolution. Ensure reliable downstream use of your data.

### **Lesson 8: Logging & Tracing** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_8_Logging_&_Tracing.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_8_Logging_&_Tracing.ipynb)
Track every step of your pipeline: from extraction to load. Use logs, traces, and metadata to debug and analyze performance.

### **Lesson 9: Performance Optimization** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_9_Performance_optimisation.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/Lesson_9_Performance_optimisation.ipynb)
Handle large datasets, tune buffer sizes, parallelize resource extraction, optimize memory usage, and reduce pipeline runtime.

