---
title: Engineering Manager
description: A guide to using dlt for Engineering Managers
keywords: [engineering manager, EM, data platform engineer]
---

# Data Stack Architect / Chief data officer / Data platform engineer

What does a senior architect or engineer consider when choosing a tech stack?

- Company Goals and Strategy
- Cost and Return on Investment (ROI)
- Staffing and Skills
- Employee Happiness and Productivity
- Maintainability and Long-term Support
- Integration with Existing Systems
- Scalability and Performance
- Data Security and Compliance
- Vendor Reliability and Ecosystem


# What makes dlt a must-have for your data stack or platform?

For starters, dlt is the first data pipeline solution that is built for your data team's ROI. Our vision is to add value, not gatekeep it.

By being a library built to enable free usage, we are uniquely positioned to run in existing stacks without replacing them. This enables us to disrupt and revolutionise the industry in ways that only open source communities can.


## dlt massively reduces pipeline maintenance, increases efficiency and ROI.

- Reduce engineering effort as much as 5x via a paradigm shift. Structure data automatically to not do it manually. [Read about structured data lake](https://dlthub.com/docs/blog/next-generation-data-platform), and [how to do schema evolution](https://dlthub.com/docs/general-usage/schema-evolution).
- Better Collaboration and Communication: Structured data promotes better collaboration and communication among team members. Since everyone operates on a shared understanding of the data structure, it becomes easier to discuss and align on data-related topics. Queries, reports, and analysis can be easily shared and understood by others, enhancing collaboration and teamwork.
- Faster time to build pipelines: After extracting data, if you pass it to dlt, you are done. If not, it needs to be structured. Because structuring is hard, we curate it. Curation involves at least the producer, and consumer, but often also an analyst and the engineer, and is a long, friction-full process.
- Usage focus improves ROI.To use data, we need to understand what it is. Structured data already contains a technical description, accelerating usage.
- Lower cost: Reading structured data is cheaper and faster because we can specify which parts of a document we want to read.
- Removing friction: By alerting schema changes to the producer and stakeholder, and by automating structuring, we can keep the data engineer out of curation and remove the bottlneck. [notify maintenance events](../running-in-production/running#inspect-save-and-alert-on-schema-changes)
- Improving quality. No more garbage in, garbage out. Because dlt structures data and alerts schema changes, we can have better governance.

## dlt makes your team happy.

- Spend more time using data, less time loading it. When you build a dlt pipeline, you only build the extraction part, automating the tedious structuring and loading.
- Data meshing to reduce friction: By structuring data before loading, the engineer is no longer involved in curation. This makes both the engineer and the others happy.
- Better governance with end to end pipelining via dbt: [run dbt packges on the fly](../using-loaded-data/transforming-the-data#transforming-the-data-using-dbt). [lineage out of the box](../using-loaded-data/understanding-the-tables).
- Zero learning curve: Declarative loading, simple functional programming: By using dlt's declarative, standard approach to loading data, there is no complicated code to maintain, and the analysts can thus maintain the code.
- Autonomy and Self service: Customising pipelines is easy, whether you want to plug an anonymiser, rename things, or curate what you load. [Anonymisers, renamers](../customizations/customizing-pipelines/pseudonymizing_columns),
- Easy discovery and governance: By tracking metadata like data lineage, describing data with schemas, and alerting changes, we stay on top of the data.
- Simplified access: Querying structured data can be done by anyone with their tools of choice.

## dlt is a library that you can run in unprecedented places

Before dlt existed, all loading tools were built either
- as Saas (5tran, Stitch, etc),
- as installed apps with their own orchestrator: Pentaho, Talend, Airbyte
- or as abandonware framework meant to be unrunnable without help (Singer was released without orchestration, not for general public)

dlt is the first python library in this space, which means you can just run it wherever the rest of your python stuff runs, without adding complexity.
- You can run dlt in airflow - this is the first ingestion tool that does this.
- You can run dlt in small spaces like cloud functions or git actions - so you could easily set up webhooks, etc.
- You can run dlt in your notebook and load data to duckdb.
- You can run dlt on large machines, it will attempt to make the best use of the resources available to it.
- You can run dlt locally just like you run any python scripts. Link to running.

The implications:
- Empowering Data Teams and Collaboration: You can discover or prototype in notebooks, run in cloud functions, and deploy to production, the same scalable, robust code. No more friction between roles. [Colab demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing#scrollTo=A3NRS0y38alk)
- Rapid Data Exploration and Prototyping: By running in colab with duckdb, you can explore semi structured data much faster by structuring it with dlt and analysing it in sql.  [schema inference](../general-usage/schema#data-normalizer),[exploring the loaded data](../using-loaded-data/understanding-the-tables#show-tables-and-data-in-the-destination).
- No vendor limits: dlt is forever free, with no vendor strings. We do not create value by creating a pain for you and solving it. We create value by supporting you beyond.
- Dlt removes complexity: You can use dlt in your existing stack, no overheads, no race conditions, full observability. Other tools add complexity.
- dlt can be leveraged by AI - because it's a library with low complexity to use, large language models can produce dlt code for your pipelines
- Ease of adoption: If you are running python, you can adopt dlt. dlt is orchestrator and destination agnostic.



