---
title: REST API Sources with Cursor
description: This doc explains how to build REST API Sources with Cursor
keywords: [cursor, llm, restapi, ai]
---

## Overview

The purpose of this document is to explain how to build REST API sources with Cursor and dlt. While the focus here is on REST APIs, this approach can be generalized to other source types such as Python imperative/full code dlthub sources. We choose REST APIs because they are ideal for this process, they are inherently self-documenting, exposing endpoints and methods in a way that enables us to easily troubleshoot and refine our vibe coding.

With REST API sources being configuration-driven and vibe coding based on prompts, users don't necessarily need to know how to code. However, it is important to understand how an API is represented and how data should be structured at the destination, such as managing incremental loading configurations. This foundational knowledge ensures that even non-developers can effectively contribute to building and maintaining these data pipelines.

## 1. Problem definition & feature extraction

Building a data pipeline can be separated into two distinct problems, each with their own challenges:

1. **Extraction:** Identifying and gathering key configuration details from various sources.
2. **Pipeline construction:** Using those details to build a robust data ingestion pipeline.

Consider these steps separately, as this will aid you in troubleshooting.

![image](https://storage.googleapis.com/dlt-blog-images/dlt-cursor-restapi.drawio.png)

### 1.1 Extracting features from information sources

The best source of information for building a pipeline, is another working pipeline. This is because many documentations do not contain all the information required to build a top notch pipeline. For example, API docs often do not contain all the information needed for creating an incremental strategy - meaning you can still build a pipeline, but it will not work as efficiently as it could.

So here is the ranking of what sources you could use

1. **Other pipelines**: Legacy pipelines, sources and connectors from other languages or frameworks, or any code that shows how to request, such as API wrappers.
2. **Docs + http responses**. Reading the docs, creating an initial working pipeline and then requesting from the API data so we can infer the rest of the missing info.
3. **Scraped code, llm memory**: When nothing is available but a public API exists, such as APIs that power public websites, we can try inferring how it is called from other websites’ code that calls it.

### 1.2 Understanding the parameters

Since there are only partial naming standards for these parameters, we describe the ones we use here.

- **Top-level client settings:**
    - **client.base_url:** The API’s root URL.
    - **client.auth:** Authentication details (token/credentials, reference to dlthub secrets).
    - **client.headers:** Required custom headers.
    - **client.paginator:** Pagination configuration (type such as "cursor", and associated parameters like `next_url_path`, `offset_param`, `limit_param`).
- **Per-resource settings:**
    - **name:** The resource/table name.
    - **endpoint.path:** The REST API endpoint’s path.
    - **endpoint.method:** HTTP method (defaults to GET if unspecified).
    - **endpoint.params:** Query parameters including defaults and dynamic placeholders.
    - **endpoint.json:** POST payload (with support for placeholders) when applicable.
    - **endpoint.data_selector:** JSONPath or equivalent to extract the actual data.
    - **endpoint.paginator:** Resource-specific pagination settings if differing from client-level.
    - **endpoint.response_actions:** Handlers for varied HTTP status codes or responses.
    - **write_disposition:** Options like append, replace, or merge.
    - **primary_key:** Field(s) used for deduplication.
    - **incremental:** Configuration parameters for incremental loading (`start_param`, `end_param`, `cursor_path`, etc.).
    - **include_from_parent:** Inheriting fields from parent resources.
    - **processing_steps:** Transformations (filters, mappings) applied to the records.
- **Resource relationships:**
    - Define parent-child relationships and how to reference fields using placeholders (for example when requesting details of an entity by ID).

## 2. Setting up Cursor for REST API source generation

### 2.1 Understanding Cursor

Cursor is an AI-powered IDE built on Visual Studio Code that accelerates your workflow by integrating intelligent features into a familiar editor. This enables you to build REST API sources with dlt through prompt-based code generation, clear handling of parameters (such as endpoints, authentication, and pagination), and real-time code validation.

This produces self-documenting, self-maintaining, and easy to troubleshoot sources.

### 2.2 Configuring Cursor with documentation

Assuming you have Cursor installed, you can access settings by clicking the cog setttings icon at the top right corner of the IDE.

Under `Cursor Settings` > `Features` > `Docs`, you will see the docs you have added. You can edit, delete, or add new docs here.

We recommend adding the [dlthub docs from this URL](https://github.com/dlt-hub/dlt/tree/devel/docs), where various code examples are included.

For the task of building REST API sources, we also created this [LLM-friendly documentation that you can add from this url.](https://github.com/dlt-hub/cursor-dlt-example/tree/main/docs)

### 2.3 Adding rules to cursor

Rules are like system prompts. In our experiments, when building a REST API pipeline, you want to keep the original rule of how to do it on during the whole process. For this, you can use global rules:

Global rules can be added by modifying the `Rules for AI` section under `Cursor Settings` > `General` > `Rules for AI`. This is useful if you want to specify rules that should always be included in every project like output language, length of responses etc.

[You can find a rule created for REST API building here.](https://github.com/dlt-hub/cursor-dlt-example/blob/main/.cursor/rules/build-rest-api.mdc)

### 2.4 Integrating local docs

If you have local docs in a folder in your codebase, Cursor will automatically index them unless they are added to `.gitignore` or `.cursorignore` files.

To improve accuracy, make sure any files or docs that could confound the search are ignored.

### 2.5 Documentation augmentation

If existing documentation for a task is regularly ignored by LLMs, consider using reverse prompts outside of cursor to create LLM optimised docs, and then make those available to cursor as local documentation. You could use a RAG or something like DeepResearch for this task. You can then ask cursor to improve the cursor rules with this documentation.

## 3. Running the workflow

Before you start, it might be a good idea to scaffold a dlt pipeline by running dlt init with the REST API source:

`dlt init rest_api duckdb`

Consider prompting for instructions how to make credentials for the source and add them to the project.

1. **Initial prompt:**
    - Make sure your cursor rule is added, dlt REST API docs are added and your source for information is added.
    - Prompt something along the lines:
    
        "Please build a REST API (dlthub) pipeline using the details from the documentation you added in context. Please include all endpoints you find and try to capture the incremental logic you can find. Use the build REST API cursor rule. Build it in a python file called “my_pipeline.py" 
    
    - If it builds what looks like a sensible REST API source, go on with inspection. If not and if the LLM ended up writing random code, just start over.
2. **Inspect the generated code:**
    - Look into the endpoints that were added. Often the LLM will stop after adding a bunch, so you might need to prompt “Did you add all the endpoints? Please do”.
    - Look into incremental loading configs, does it align with documentation, is it sensible?
3. **Run and test the code:**
    - Execute the code in a controlled test environment.
    - If it succeeds, double check the outputs and make double sure your incremental and pagination are correct - both of them could cause silent failures, where only some pages are loaded, or pagination never finishes (some APIs re-start from page 0 once they finish pages), or the wrong incremental strategy removes records based on the wrong key, or the wrong data is extracted.  Writing some tests is probably a good idea at this point.
4. **Error handling and recovery:**
    - If an error occurs, share the error message with Cursor’s LLM agent chat.
    - Let the LLM attempt to recover the error and repeat this process until the issue is resolved.
5. **Iterative debugging:**
    - If errors persist after several attempts, review the error details manually.
        - Extraction issue: Identify if any configuration details are missing from the feature extraction, and try to manually locate the missing information and provide it in the chat along with the error message.
        - Info in responses: Currently the REST API does not return full information about the API calls on failure, to prevent accidental leakage of sensitive information. We are considering adding a dev mode for enabling full responses in a future version. If you want to pass the full responses, consider building the pipeline in pure python first  to expose the API responses (you can prompt for it).
        - Implementation incorrect: If the extracted details seem correct but the REST API source still isn’t implemented properly, ask the LLM to re-check the REST API documentation and locate the missing information.

## 4. Best practices and vibe coding tips

Below, find some tips that might help when vice coding REST API or python pipelines.

### 4.1 General best practices

- **Clarity over cleverness**
    - Use explicit names for variables, configs, and pipeline steps.
        
        `source_config = {...}` is better than `cfg = {...}`.
        
    - Version your code and config files. Use Git branches per integration.
    - Include comments only when the code isn’t self-explanatory, avoid noise.
        
        Good: `# Required by API to avoid pagination bug`.
        
        Bad: `# Set the page size`.
        
- **Use checklists, no, really.**
    
    Before publishing any new pipeline or source, confirm:
    
    - [ ]  All `client` configs (auth keys, tokens, endpoints) are present and correct.
    - [ ]  `resource` settings match the expected schema and entity structure.
    - [ ]  Incremental fields (`updated_at`, `id`, etc.) are configured correctly.
    - [ ]  Destination settings (dataset names, schema names, write disposition) are reviewed.
- **Break work into small, testable chunks**
    - Don’t process all endpoints at once. Implement one `resource` at a time, test it, and then layer more.
    - Ingestion pipelines should run in under 10 minutes locally, if not, split the logic.
    - Use fixtures or mocks when testing APIs with rate limits or unstable responses.

### 4.2 LLM-Specific tips & common pitfalls

- **Prompt design tips**
    - Give context: include 1-2 lines about the task, data structure, and output format.
        
        Example: *"You are generating Python code to parse a paginated REST API. Output one function per endpoint."*
    
    - Be explicit: always define structure expectations.
        
        Example: *"Return a list of dictionaries with keys `id`, `name`, `timestamp`."*
        
    - Avoid ambiguity: never say “optimize” or “improve” unless you define what “better” means.
- **Validate LLM output like you would PRs**
    - Run the code it generates.
    - Compare schema definitions, type hints, and transformations with the actual API/data.
    - If your docs say a field is optional, the prompt should include that.
- **Pitfalls to avoid**
    - Session sprawl: Keep LLM sessions under 15-20 interactions. Split into sub-tasks if needed.
    - Misinterpreting placeholders: For dynamic variables like `{{timestamp}}` or `{{page}}`, wrap them in extra explanation, e.g.,
        
        *"Use `{{timestamp}}` as an ISO 8601 string from the last run time."*
        
    - **Model drift**: GPT-4, Claude, and other models don’t behave the same. Outputs vary in subtle ways. Always test prompts across models if switching.
- **When changing models or seeing weird behavior:**
    - Reduce the scope: instead of asking “write the full source,” say “write the pagination logic.”
    - Switch from creative mode (broad prompts) to constrained mode (e.g., give a template and ask it to fill it in).
    - Run diffs between old and new LLM outputs to detect accidental regressions, especially if you fine-tuned prompts.

## Closing words

This domain is new, and the document on this page will evolve as we will be better able to provide better instructions. We are actively working on pushing the envelope. For advanced usage, see our [MCP server docs.](../../dlt-ecosystem/llm-tooling/mcp-server.md)
