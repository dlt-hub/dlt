---
title: dlt Advanced Course
description: Master advanced dlt concepts with this comprehensive course covering custom sources, destinations, transformations, data contracts and performance optimization.
keywords: [dlt, advanced, tutorial, pipeline, python, data engineering, custom sources, custom destinations, transformations, data contracts, logging, tracing, performance optimization]
---

# `dlt` Advanced Course

In this course, you'll go far beyond the basics. You’ll build production-grade data pipelines with custom implementations, advanced patterns, and performance optimizations.

## Lessons

### **Lesson 1: Custom Sources – REST APIs & RESTClient** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_1_custom_sources_restapi_source_and_restclient.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_1_custom_sources_restapi_source_and_restclient.ipynb)

Learn how to build flexible REST API connectors from scratch using `@dlt.resource` and the powerful `RESTClient`. 

### **Lesson 2: Custom Sources – SQL Databases** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_2_custom_sources_sql_databases_.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_2_custom_sources_sql_databases_.ipynb)

Connect to any SQL-compatible database, reflect table schemas, write query adapters, and selectively ingest data using `sql_database`.

### **Lesson 3: Custom Sources – Filesystems & Cloud Storage** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_3_custom_sources_filesystem_and_cloud_storage.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_3_custom_sources_filesystem_and_cloud_storage.ipynb)
Build sources that read from local or remote files (S3, GCS, Azure).

### **Lesson 4: Custom Destinations – Reverse ETL** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_4_destinations_reverse_etl.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_4_destinations_reverse_etl.ipynb)
Use `@dlt.destination` to send data back to APIs like Notion, Slack, or Airtable. Learn batching, retries, and idempotent patterns.

### **Lesson 5: Transforming Data Before & After Load** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_5_transform_data_before_and_after_loading.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_5_transform_data_before_and_after_loading.ipynb)

Learn when and how to apply `add_map`, `add_filter`, `@dlt.transformer`, or even post-load transformations via SQL or Ibis. Control exactly how your data looks.

### **Lesson 6: Write Disposition Strategies & Advanced Tricks** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_6_write_disposition_strategies_and_advanced_tricks.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_6_write_disposition_strategies_and_advanced_tricks.ipynb)
Understand how to use `replace` and `merge`, and combine them with schema hints and incremental loading. 

### **Lesson 7: Data Contracts** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_7_data_contracts.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_7_data_contracts.ipynb)
Define expectations on schema, enforce data types and behaviors, and lock down your schema evolution. Ensure reliable downstream use of your data.

### **Lesson 8: Logging & Tracing** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_8_logging_and_tracing.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_8_logging_and_tracing.ipynb)
Track every step of your pipeline: from extraction to load. Use logs, traces, and metadata to debug and analyze performance.

### **Lesson 9: Performance Optimization** [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_9_performance_optimisation.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/master/docs/education/dlt-advanced-course/lesson_9_performance_optimisation.ipynb)
Handle large datasets, tune buffer sizes, parallelize resource extraction, optimize memory usage, and reduce pipeline runtime.

## Homework & Certification

You’ve finished the dlt Advanced Course — well done! Test your skills with the [**Advanced Certification Homework**](https://dlthub.learnworlds.com/course/dlt-advanced).
