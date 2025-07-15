---
title: REST API Sources with Cursor
description: This doc explains how to build REST API Sources with Cursor
keywords: [cursor, llm, restapi, ai]
---

# REST API Sources with Cursor

## Overview

The purpose of this document is to explain how to build REST API sources with Cursor and dlt. While the focus here is on REST APIs, this approach can be generalized to other source types such as Python imperative/full code dlthub sources. We choose REST APIs becausebeing a declarative source, it is ideal for this process - being inherently self-documenting, exposing endpoints and methods in a way that enables us to easily troubleshoot and refine our AI-assisted development.

With REST API sources being configuration-driven and AI-assisted development based on prompts, users don't necessarily need extensive coding experience. However, it is important to understand how an API is represented and how data should be structured at the destination, such as managing incremental loading configurations. This foundational knowledge ensures that even non-developers can effectively contribute to building and maintaining these data pipelines.

## 1. Problem definition & feature extraction

Building a data pipeline can be separated into two distinct problems, each with their own challenges:

1. **Extraction**: Identifying and gathering key configuration details from various sources.
2. **Pipeline construction**: Using those details to build a robust data ingestion pipeline.

todo: Mention the scaffolds and how they achieve point 1


## 2. Set up Cursor for REST API source generation

### 2.1 Understanding Cursor

Cursor is an AI-powered IDE built on Visual Studio Code that accelerates your workflow by integrating intelligent features into a familiar editor. This enables you to build REST API sources with dlt through prompt-based code generation, clear handling of parameters (such as endpoints, authentication, and pagination), and real-time code validation.

This produces self-documenting, self-maintaining, and easy to troubleshoot sources.

### 2.2 Adding rules and context to cursor

We bundled LLM-native source docs with a set of cursor rules that help build REST API pipelines with dlt. To use them, you should start by initializing a new dlt project:

```bash
pip install dlt[workspace]
```

Init the source documentation and template. You can find the supported sources on our hub TODO LINK.
If your source is not supported, request it here: TODO LINK.
If you don't want to wait, find the source docs online and add them to Cursor, but this results in less focused outputs and increased risk of overlooking critical API details.

```bash
dlt init dlthub:{source_name} duckdb
```
The init command will setup some important files and folders.
- your source documentation scaffold
- cursor rules
- dlt pipeline template files

### 2.3 Model and context selection

We had the best results with models Claude 3.7-sonnet and Gemini 2.5 or higher. Weaker models were not able to comprehend the required context in full and were not able to use tools and follow workflows consistently. Due to the big performance difference we discourage using inferior models.



## 3. Begin coding

Once you are ready, prompt something along the lines:

 ```prompt
    Please generate a REST API Source for {source} API, as specified in @{source}-docs.yaml
    Start with endpoints {endpints you want} and skip incremental loading for now.
    Place the code in {source}_pipeline.py and name the pipeline {source}_pipeline.
    If the file exists, use it as a starting point.
    Do not add or modify any other files.
    Use @dlt rest api as a tutorial.
    After adding the endpoints, allow the user to run the pipeline with python {source}_pipeline.py and await further instructions.
   ```

 If your pipeline runs correctly, you’ll see something like the following:

    ```shell
    Pipeline {source} load step completed in 0.26 seconds
    1 load package(s) were loaded to destination duckdb and into dataset {source}_data
    The duckdb destination used duckdb:/{source}.duckdb location to store data
    Load package 1749667187.541553 is LOADED and contains no failed jobs
    ```

If the pipeline runs, continue to the debugging step. If it does not, pass the error message to the LLM in chat and let it attempt to resolve it until the code runs. If your code does not run after 4-8 prompts, consider restarting the process.

Consider prompting for instructions how to make credentials for the source and add them to the project secrets file.


## 4. Debug your pipeline and data with the Pipeline Dashboard

Now that you have a running pipeline, you need to make sure it’s correct, so you do not introduce silent failures like misconfigured pagination or incremental. By launching the dlt Workspace Pipeline Dashboard, you can see various info about the pipeline to enable you to test it.

Here you can see:
    - Pipeline overview: State, load metrics
    - Data’s schema: tables, columns, types, hints,
    - You can query the data itself

    ```shell
    dlt pipeline {source}_pipeline show --dashboard
    ```

Read more about [accessing your data](https://dlthub.com/docs/general-usage/dataset-access/)


## 5. Next steps: Deploy to production

- [How to prepare production deployment](https://dlthub.com/docs/walkthroughs/share-a-dataset)
- [How to deploy a pipeline](https://dlthub.com/docs/walkthroughs/deploy-a-pipeline)
