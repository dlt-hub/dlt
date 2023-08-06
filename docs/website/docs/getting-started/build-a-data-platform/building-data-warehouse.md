---
title: Build a data warehouse
description: How to build a data warehouse, with example dlt
keywords: [data warehouse]
---

# What is a data warehouse?

A data warehouse is a collection of your business’ data organized by process. It enables you to
easily measure and describe the performance of your business, take informed decisions, and improve
the processes you measure.

# How do you build a data warehouse?

A data warehouse is built in three steps:

1. **Ingestion**: Consolidate the business data you want to track into a single location where it
   can be used.
1. **Transformation**: Arrange and curate the data to represent the business processes.
1. **Governance**: Train users and document data. Since the data in a warehouse is highly business
   specific, users need data docs and tool training to understand how to use the data.

## Step 1: Ingestion

There are three elements to ingestion: a destination to load your data into, an orchestrator to run
your pipeline, and your pipeline.

### Choosing a destination

There are many cloud data platforms you could use. They all store data, so the question really is,
which one best fits your use case?

Usually you would want to choose a destination with SQL support. The reason for this is that the
business access tools are built for SQL databases. So, if you want the business user to be able to
self-service, then you will need a destination that can support the tooling.

So, let’s look at the use cases for major technologies:

- **Postgres**: This is good for small data warehouses. It’s a transactional database which is not
  particularly fast at aggregations, but is very fast at transactional queries. It also has a very
  clear cost structure - you pay for the infrastructure.
- **Massive Parallel Processing (MPP) databases**:
  - **BigQuery**: If you do not know what to choose, choose Bigquery. Under the hood is a data lake
    with a clear payment model - you pay for read volume of data, regardless of what you do with it.
    Pay as you go: there's a generous free tier, beyond which there are multiple cost-reducing
    mechanisms that may kick in at a larger scale making pricing hard to predict. **BigQuery is the
    most suitable for the use-case: “I want a solution that just works, with no maintenance, and no
    understanding of databases.”**
  - **Snowflake**: Snowflake is fast and requires minimal maintenance. Its extra functionalities
    make it particularly friendly for people who come from enterprise solutions. It's easier to
    manage than Redshift, but offers less flexibility than BigQuery. **It caters to people who spend
    most of their time working with SQL.**
  - **Redshift**: The first MPP database among the ones we discuss. Launched in 2012, it follows an
    old paradigm, where the data engineer needs to manage how data is distributed between nodes -
    failure to do so degrades performance 100x. **The use case for Redshift is heavy read and
    processing**, because you rent compute resources that are always running, so you might as well
    utilise them. We do not recommend starting new projects on Redshift unless you understand the
    type of maintenance you will need to perform.
  - **Trino**: The open source alternative. Trino does not store data, but only queries it. It’s
    cheaper than the rest, if you have the internal resources to run it. It also has some features
    that only open source software would have, such as federated database access that allows you to
    integrate with other storage solutions and query the data directly. Trino originates out of
    Facebook’s Presto.
  - **Azure Synapse Analytics**: This is suitable for professionals in the Microsoft ecosystem who
    do not want to use an independent vendor or open source.

### Choosing an orchestrator

There are
[a couple of considerations](../../dlt-ecosystem/deployments/orchestrators/choosing-an-orchestrator)
to make when choosing an orchestrator. If you choose Airflow as an orchestrator, then to set it up
with your destination, say BigQuery, you would do the following:

1. Create a GCP account.
1. Start up an Airflow instance. We suggest using some CI/CD for simple deployment.

### Adding a pipeline

Depending on which data sources you use, you might have three options:

1. SaaS pipelines:
   - Upside: You don't have to handle maintenance.
   - Downside: The cost and lack of flexibility.
1. Open-source pipelines:
   - Upside: Flexibility and community support.
   - Downside: These tend to be frameworks and require additional applications.
1. DIY pipelines:
   - Upside: Can be used with any source and are fully customizable.
   - Downside: They would need to be built and maintained by you.

#### **What's a typical solution?**

A typical solution would be to have multiple solutions. The upside is that you solve each problem
with the appropriate tool. The downside is that now you need to manage and maintain multiple tools,
adding complexity, increasing recovery times, etc.

#### **What's a good solution?**

A good solution would be to have a single way to do things that plugs into existing workflows.
Inevitably, you will end up having some custom sources and scripts, and an orchestrator to run them,
so it makes sense that the pipelines should be runnable by this orchestrator too, and not add more
complexity.

**This is one of the main reasons we created `dlt`:**

- For common sources, `dlt`’s community-built and maintained pipelines can deploy to your airflow
  with only two CLI commands (`dlt init source`, `dlt deploy source airflow`). Maintenance is partly
  automated, and open sourced, while customisations are easy.
- For simple sources, you can have an easy python function to just take any data and load it in a
  robust way.
- For complex sources, you can have some helpers for common problems. `dlt`’s extractors and helpers
  make it easy to scalable and quickly build custom extraction logic.

## Step 2: Transformation

Once data is loaded to a SQL database, you would want to use SQL statements to clean it, apply
business rules, and arrange it in a way that self-service tools can query this data easily by means
of “pivot tables”, which essentially enable business users to run aggregations over metrics grouped
by dimensions.

A typical data model to support such queries is the star schema, which brings many benefits to
implement, such as lowering maintenance and creating a single source of truth. But this may vary
based on your problem.

Read more about the star schema [here](https://en.wikipedia.org/wiki/Star_schema).

### **dbt**

Since you will be running SQL queries, it makes sense to use a tool to help you manage them. One
popular such tool is dbt.

`dlt` supports dbt uniquely. `dlt` can run dbt packages on the fly: you just need to provide the Git
URL or local path of the package. This enables you to have end-to-end pipelines that start with
loading and end with transform. `dlt` can create a venv on the fly for dbt, enabling running it even
in environments where dbt would not be possible to run directly. Additionally, credentials
accessible to `dlt` are passed to dbt, simplifying your setup.

For example, you could develop with dbt cloud, save to GitHub, and run your dbt GitHub package from
your airflow instance via `dlt`.

Read more about using dbt with `dlt` [here](../../dlt-ecosystem/transformations/dbt.md).

## Step 3: Governance

### Using your data

For an organization to use the data effectively, it is important to choose tooling that can satisfy
three data access paradigms:

1. **Dashboards**: These are graphical interfaces that display key data trends and metrics
   simultaneously. Dashboards are typically used for monitoring key performance indicators (KPIs)
   and are developed using tools like Tableau, Power BI, or Looker.
1. **ROLAP (Relational Online Analytical Processing) and self-service BI tools**: These tools allow
   users to generate their own reports and analyses. They provide an interface for creating pivot
   tables, diving deep into the data, and exploring it in an interactive manner. Microsoft Excel's
   PivotTable feature, Power BI, and Tableau are examples of such tools.
1. **Querying**: This is a more technical approach, where users write SQL or similar queries to
   directly access and analyse data stored in databases or data warehouses. SQL Workbench or DBeaver
   are tools often used for this.

Because Metabase is the open source tool that enables all the access paradigms to the analyst as an
end user, and does all of them very well, we strongly recommend it. Other open source tools worth
mentioning are Apache Superset and Redash. If you use BigQuery, Google Sheets is an interesting new
mention, and a favorite of business users.

### Governing your data

Data governance ensures that data, regardless of how it's accessed or visualised, is accurate,
reliable, and secure. It ensures:

1. **Access Control**: Data governance policies dictate who can access what data, safeguarding
   sensitive information from unauthorized usage.
1. **Data Quality**: Governance includes processes for maintaining data accuracy and consistency,
   which is crucial for the reliability of any data visualization or derived insights. We suggest:
   1. [Creating implicit data contracts](../../running-in-production/alerting)
   1. [Validating your data](https://greatexpectations.io/)
   1. [Adding tests to your workflow](https://docs.getdbt.com/docs/build/tests)
1. **Compliance**: Data governance also helps maintain compliance with regulations on data usage and
   privacy, like GDPR or CCPA. Make sure you follow the respective documentation. If you want to
   anonymize before ingestion,
   [check out the `dlt` docs](../../general-usage/customising-pipelines/pseudonymizing_columns#pseudonymizing-or-anonymizing-columns-by-replacing-the-special-characters).
1. **Data Literacy**: Data governance helps ensure users have a clear understanding of the data they
   are working with, its context, limitations, and appropriate usage, preventing misinterpretation
   or misuse of data. If you are using Metabase with dbt, dbt-metabase is a nice solution. If not,
   figure out how to best provide your user with a data dictionary and training.

### With governance follows democracy. Without it, only anarchy

The concept of data democratization ties all of this together. Data democratization means making
data accessible to everyone, not just data professionals or top management. In this context, data
governance policies ensure that while data is democratized, it is also used responsibly, with
respect to privacy, security, and quality considerations.

Data visualization tools, especially dashboards and self-service BI tools, play a key role in data
democratization. They allow non-technical users to access, analyze, and make decisions based on data
without needing advanced technical skills. At the same time, data governance ensures this
democratization doesn't compromise data integrity, security, or compliance.

So make sure you document and enforce your governance, unless you want to create data anarchy.

# Finally, a recipe for beginners

1. Orchestration and storage: Choose BigQuery and Cloud Composer
   - GCP offers “normie” software, meaning normal people can use it. So if you are feeling normal
     and not yet specialized, choose GCP.
   - On GCP, you have a very nice [managed Airflow called “Cloud Composer”](../../dlt-ecosystem/deployments/orchestrators/airflow-deployment.md).
     The advantage in using it is that you only pay for the servers, so it saves you doing your own setup.
     It also comes with CI/CD support.
   - On [BigQuery](../../dlt-ecosystem/destinations/bigquery.md), you can have the best of both worlds: data lake and data warehouse, all in one
     tech and at a similar price-point as anything else.
   - Native integration with Google Sheets gives BigQuery a boost in Google-Sheets-heavy
     organizations.
1. Ingestion: Use `dlt`. The schema inference and evolution will save you most of the pain, and the
   declarative interface will make it fast to develop with. Having community support means you will
   benefit from a community of people with the same problem. Run `dlt` in Airflow or under your
   chosen orchestrator.
1. Transformation: Choose dbt. By using `dlt` pipelines, which output universal schemas, you can
   develop database-agnostic dbt packages. This way you can maintain them in the community,
   regardless of which tech stacks they use.
1. Data access: Use the right paradigm for the right person. Metabase solves 90% of cases for
   non-technical access - the remaining are using data in sheets, which is available natively on GCP, and well-supported in the Microsoft stack.
