---
title: Where can dlt run?
description: dlt can run on almost any python environment and hardware
keywords: [dlt, running, environment]
---

# Where can dlt run?

## Made for your environments: Airflow, GCP Cloud Functions, AWS Lambda, GitHub Actions

Scaling data pipelines presents unique challenges:

- How much RAM we need to process the data - are we reading gigs in memory, or are we limiting
  memory usage?
- How much compute resource we need to process the data - should we parallelize to leverage more
  threads or machines?

And because from pipeline to pipeline data volumes may vary 10000x, then the answer is you will not
know upfront how to scale.

So you either prepare for scaling, or prepare for maintenance. `dlt` is already prepared for scaling.

- It can run in [Cloud Functions](running-in-cloud-functions.md) - you could use it to receive events and load them, or to process
  events from a buffer.
- It can run on Airflow - where most data pipelines run nowadays. We support
  [deployment to Airflow](orchestrators/airflow-deployment.md) here.
- It can run on Git Actions - we even support the
  [Github Actions deployment command](orchestrators/github-actions.md) here.

### Memory and compute management

While you can pass any data to `dlt`, `dlt` prefers sources as generators, because this enables `dlt` to
manage how much data it loads into memory at a time.

By managing the maximum size of the memory buffer for extraction, `dlt` can process large data sources
chunk by chunk and not run out of ram.

After the data is downloaded, `dlt` may spawn multiple parallel processes to normalise the data - they
will also do it chunk by chunk, to avoid filling the ram.

This way, with `dlt`:

- Large data can be processed on tiny machines. No more crashing Airflow workers or pods.
- When normalising data, we use the available compute resources optimally.
- By enabling running on micro workers, you are enabled to split your job before running it with
  `dlt`, to achieve more parallelism.

### Python env, dependencies

You can run `dlt` directly on your python interpreter, or you could create a venv to isolate its
dependencies. Read more about [installing dlt](../../reference/installation.mdx) here.

`dlt` is designed with minimal dependency footprint. Once you install `dlt`, you may also need to
install the destination package. The instructions to do so will appear when you init a pipeline with
a different destination. For example, initing a BigQuery pipeline will create a requirements file
with `dlt[bigquery]` in it, and you will be asked to pip install it.
