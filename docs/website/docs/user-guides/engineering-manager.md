---
title: Engineering Manager
description: A guide to using dlt for Engineering Managers
keywords: [engineering manager, EM, data platform engineer]
---

# Engineering Manager

## Why are data platform engineers needed?

Today, the data platform engineer role is critical to organizations that wish to build and maintain big data products used by other data professionals.

These platform engineers are responsible for building and maintaining the data platforms that enable organizations to collect, store, and analyze data at scale. Their work is essential to the success of data-driven initiatives.

Historically, this role would be fulfilled by a cross functional data manager or technical manager. Nowadays as requirements, data, technologies, and teams grow more complex, data products often have dedicated platform engineers.

## What does a data platform engineer do?

A data platform engineer ensures that the rest of the data professionals are enabled to deliver on the goals of the business. They often have to come up with a plan to bring a company higher on the data maturity scale. While their job title sounds like an engineer, they have to solve just as much for the people element. Their responsibilities often include:
- Understand the goal, budget, technology, staffing constraints, etc. and create a plan to reach the goals
- Choose and and govern the development paradigm, which roles do what and how to ensure neither bottlenecks nor chaos are created
- Design and manage the architecture and infrastructure the team will use
- Manage data security and access paradigms
- Educate the team on how to use the platform and ensure that the team is able to deliver their goals on the platform

## Why do data platform engineers use `dlt`?

`dlt` is the most extensive data loading option that scales with your team and usage for free. From simple to complex, `dlt` offers different levels of abstraction that enable and empower your data analysts, data scientists, data engineers,analytics engineers, etc. to build and use pipelines.

### 1. Single use case driven pipelines

Some use cases require specific bits of external data (e.g. attribution might use weather data). These pipelines are usually built by data analysts and data scientists, since they support a single use case usually and have less business impact if they fail. For example, this [colab demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing) shows grabbing some data from the chess.com API.

### 2. Core business pipelines

These are usually engineered to ensure high service service-level agreement (SLA) for all core business cases. The core pipelines are generally built by data engineers. For example, a pipeline like [Google Sheets](https://dlthub.com/docs/pipelines/google_sheets) that copies ranges, named ranges or entire documents dynamically.

### 3. Customized pipelines

 Heavily customized pipelines that give fine grain control over everything about the data processing and loading. These are usually engineered to solve specific obstacles to data usage, such as performance, cost of loading, or things about the data, such as anonymization or managing schemas. Depending if the problem being solved is an engineering or business logic problem, it may be solved by either a data engineer or a business facing analytics engineer. For example, a pipeline like [Zendesk](../verified-sources/zendesk) with automatic field renames and optionally pivoted tables for easy analysis.

### `dlt` massively reduces pipeline maintenance in your organization

With dlt data pipeline maintenance stops being an issue for data engineering, many issues can be directly solved by Python stakeholders. `dlt` was built as a productivity tool, so it automates time consuming tasks. It has been designed to address the three major causes of pipeline maintenance and many more minor issues by empowering stakeholders to solve them:
1. When a source produces new data
- Before `dlt`: We do not find out until the stakeholder finds our dashboard and requests the data. The request goes to the producer, who tells the data engineer what to load, who changes the loading pipeline and notifies the downsteam analytics engineer to pick up the data and include it in reporting. Time to value: Days to Never
- With `dlt`: New field is added and the data alerts channel gets a notification of new fields. The analytics engineer sees the alert and quickly checks with the stakeholders or the producers if they wish this to be reported on. Since the data is already loaded, the analyst can adjust the SQL and deploy changes same day
2. When a source has a new endpoint
- Before `dlt`: Data pipeline engineer needs to add code to extract normalize and load the new endpoint. Complexity: Need to read and edit existing code or do extract normalize and load from scratch.
- With `dlt`: We encourage dynamic resource generation, so a new endpoint can probably be added to the list of endpoints, and if the api is self consistent the data will be loaded. Alternatively, the developer will need to figure out how to request the new data and pass it to `dlt`. Complexity: Add 1 word to a list or do extraction from scratch.
3. When the business is using a new service
- Before `dlt`: Data engineer builds a pipeline. They usually build extraction from scratch, normalization based on what is desired by the business and then finally doing typical loading. Somewhere, data typing happens, either before ingestion, or everything is ingested as string and typed after. After building, the bugs and gotchas we are weeded out, and finally after a few weeks our pipeline runs securely. All the code will need to be maintained
- With `dlt`: We only write minimal code, extraction code. We declare how we load, the typing, normalisation is automated. We only maintain extraction code. This can now be maintained by most people- hey only need to understand extraction and not a whole bunch of glue code.

### More open source, less vendor lock-in

`dlt` was made to solve your problems around data loading without introducing redundancy, complexity, or vendor monetzation hooks. It is meant to play nice with the rest of the stack. It is customizable, extensible, and does not require you to pay for another SaaS product.

### Better governance with less manual work by people

#### Example: Giving autonomy and governance to the producer with schema change notifications

When a stakeholder has to request something from another person, the pain of asking becomes an obstacle, and in larger orgs the request chain might prove to be a dead end. For this reason, there have been attempts to solve such problems through governing paradigm shifts (e.g data mesh).

The automation of data normalisation by `dlt` at ingestion supports shortcutting the human element-the producer can easily adjust their source schema and the data will be automatically made available downstream in the analytical system, where they or an analyst can use it.

By using the “notify on schema change” functionality offered by `dlt`, this can act as a data contract test to notify both the consumer and the producer of any changes. Read more about [alerting schema change](./running-in-production/running#inspect-save-and-alert-on-schema-changes). You can use the schema generated from the data or you can define it, read more about [schema](../general-usage/schema)
