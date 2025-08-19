---
title: dlt Fundamentals Course
description: Master the basics of dlt with this comprehensive course covering pipelines, resources, sources, configuration, schema, state, and incremental loading.
keywords: [dlt, fundamentals, tutorial, pipeline, python, data engineering, resources, sources, pagination, authentication, configuration, schema, state, incremental loading, write disposition]
---

# `dlt` Fundamentals Course

In this course you will learn the fundamentals of `dlt` alongside some of the most important topics in the world of Pythonic data engineering.

## Lessons

### Lesson 1: Quick Start [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_1_Quick_start.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_1_Quick_start.ipynb)

Discover what dlt is, run your first pipeline with toy data, and explore it like a pro using DuckDB, `sql_client`, and dlt datasets!

### Lesson 2: dlt Resources and Sources [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_2_dlt_sources_and_resources_Create_first_dlt_pipeline.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_2_dlt_sources_and_resources_Create_first_dlt_pipeline.ipynb)

Learn to run pipelines with diverse data sources (dataframes, databases, and REST APIs), 
master `dlt.resource`, `dlt.source`, and `dlt.transformer`, and create your first REST API pipeline!

### Lesson 3: Pagination & Authentication & dlt Configuration [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_3_Pagination_%26_Authentication_%26_dlt_Configuration.ipynb)


Since it is never a good idea to publicly put your API keys into your code, different environments have different methods to set and access these secret keys. `dlt` is no different.
Master pagination and authentication for REST APIs, explore dlt's RESTClient and manage secrets and configs.

### Lesson 4: Using dlt's pre-built Sources and Destinations [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_4_Using_pre_build_sources_and_destinations.ipynb) [![GitHub badge](https://img.shields.io/badge/github-view_source-2b3137?logo=github)](https://github.com/dlt-hub/dlt/blob/e3e1b07af61ee0a95053d3ec5769f807a4a86d79/docs/education/dlt-fundamentals-course/Lesson_4_Using_pre_build_sources_and_destinations.ipynb)

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

### Homework & Certification

As a final step, you can complete the [**homework quiz**](https://dlthub.learnworlds.com/course/dlt-fundamentals). Successful completion will earn you a course certification.



