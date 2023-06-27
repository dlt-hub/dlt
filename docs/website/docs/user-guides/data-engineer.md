---
title: ETL Engineer
description: A guide to using dlt for Data Engineers
keywords: [data engineer, de, pipeline builder, ETL engineer]
---

# ETL Engineer

## Use Case #1: Remove a big chunk of tedious work and most ETL maintenance

With `dlt`, you can automate the transition from unstructured (nested, untyped) to structured
(tabular, typed) data.

`dlt` is a library that you can use almost anywhere, so you are not forced to use some prescribed
way of doing things. The simplest usage of dlt would be to use it as a unstructured to structured
data transition tool in your existing code.

A `dlt` pipeline is made of a source, which contains resources, and a connection to the destination,
which we call pipeline. So in the simplest use case, you could pass your unstructured data to the
`pipeline` and it will automatically be migrated to structured at the destination. See how to do
that here [pipeline docs](../general-usage/pipeline).

The big advantage of using dlt for this is that it automates what is otherwise an error prone and
tedious operation for the data engineer. What’s more, `dlt` can migrate schemas, removing not only
development work but also most of the maintenance work as well. Read more about [how to monitor
schema evolution](../running-in-production/running#inspect-save-and-alert-on-schema-changes).

## Use Case #2: Empower everyone on the team with a library

`dlt` is meant to be accessible to each person on the data team, enabling you to standardize how
your entire team loads data. This reduces knowledge requirements to get work done and enables
collaborative working between engineers and analysts.

- Analysts can use ready-built sources or pass their unstructured data to `dlt`, which will create a
  sturdy pipeline (e.g. [get an existing source](../walkthroughs/add-a-verified-source),
  [build a pipeline](../walkthroughs/create-a-pipeline)).
- Python-first users can heavily customize how dlt sources produce data, as dlt supports selecting,
  filtering, renaming, anonymizing, and just about any custom operation (e.g.
  [rename columns example](../general-usage/customising-pipelines/renaming_columns.md)).
- Junior data engineers can configure dlt to do what they want-change the loading modes, add
  performance hints, etc. (e.g. [adjust a schema](../walkthroughs/adjust-a-schema)).
- Senior data engineers can dig even deeper into customization options and change schemas,
  normalizers, the way pipelines run such as parallelism, etc.

## Use Case #3: Enhance your productivity with `dlt`

`dlt` is meant to support natural workflows that occur in the data team. Offering native support for
local development and testing, you can use `dlt` with a local duckdb destination and use
`dlt pipeline show` to generate a web client to display and query the data. Once you are happy with
it, you can simply switch to your production destination and you pipeline will run on production
from the first try (e.g. [show data](../dlt-ecosystem/visualizations/exploring-the-data.md),
[colab duckdb example](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)).

With dbt runner, you can aditionally develop and later run transformations. If you use cross-db
compatibility for dbt, your code can even be developed locally such as on duckdb or reused by others
if you choose to reshare your code (e.g.
[run dbt from local or repository](../dlt-ecosystem/transformations/transforming-the-data)).

`dlt` supports Airflow and other workflow managers. It’s meant to plug into your data stack without
causing more overheads (e.g.
[how to set up on airflow](../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md)).

`dlt` also allows easy customization and maintenance of the code-`dlt` sources are pythonic and
simple, so no object oriented programming required. Your junior data scientist can fix and customize
the pipelines too (e.g
[strapi example](https://github.com/dlt-hub/verified-sources/blob/master/sources/strapi/__init__.py)).

## Use Case #4: Solve problems with a community

No more reinventing the flat tyre in the form of yet another custom pipeline.

- Community support. If you choose to publish your pipelines to the community, they will be reused,
  improved, upgraded, tests added, etc.
- Community distribution support. A verified source hosted in the public `verified-sources` repo can
  be distributed via our command line interface. The command also handles versioning, merging,
  upgrading, etc.

Read more about our contribution process and contribute sources or request them in
[verified sources repository](https://github.com/dlt-hub/verified-sources).

## Inheriting a running `dlt` pipeline

If you are taking over a `dlt` pipeline that someone else built or set up, you are in luck! What's
important to know:

- The pipeline is largely self-maintaining. The only thing you must ensure is that the source keeps
  producing data-from there dlt handles it.
- You can notify schema evolution events (e.g.
  [alert schema change](../running-in-production/running#inspect-save-and-alert-on-schema-changes)).
- You can use the data by transforming it with dbt (e.g.
  [run dbt from local or repository](../dlt-ecosystem/transformations/transforming-the-data)).
- If the sources used are public, you can update your pipelines to the latest version from the
  online repo with the `dlt init`command. If you are the first to fix, you can contribute the fix
  back so it will be included in the future versions (e.g.
  [init a pipeline](../reference/command-line-interface#dlt-init),
  [contribute a source](https://github.com/dlt-hub/verified-sources)).
