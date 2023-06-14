---
title: Where can dlt run?
description: dlt can run on almost any python environment and hardware
keywords: [dlt, running, environment]
---

# Where can dlt run?

## Made for your environments: Airflow, GCP Cloud Functions, AWS Lambda, GitHub Actions

Scaling data pipelines presents unique challenges:
- How much RAM we need to process the data - are we reading gigs in memory, or are we limiting memory usage?
- How much compute resource we need to process the data - should we parallelize to leverage more threads or machines?

And because from pipeline to pipeline data volumes may vary 10000x, then the answer is you will not know upfront how to scale.

So you either prepare for scaling, or prepare for maintenance. Dlt is already prepared for scaling.
- It can run in cloud functions - you could use it to receive events and load them, or to process events from a buffer.
- It can run on airflow - where most data pipelines run nowadays. We support [deployment to airfow](/docs/reference/command-line-interface#github-action) here.
- It can run on git actions - we even support the [github actions deployment command](/docs/reference/command-line-interface#github-action) here.

### Memory and compute management

While you can pass any data to dlt, dlt prefers sources as generators, because this enables dlt to manage how much data it loads into memory at a time.

By managing the maximum size of the memory buffer for extraction, dlt can process large data sources chunk by chunk and not run out of ram,

After the data is downloaded, dlt may spawn multiple parallel processes to normalise the data - they will also do it chunk by chunk, to avoid filling the ram.

This way, with dlt:
- Large data can be processed on tiny machines. No more crashing airflow workers or pods.
- when normalising data, we use the available compute resources optimally.
- By enabling running on micro workers, you are enabled to split your job before running it with dlt, to achieve more parallelism.


### python env, dependencies

You can run dlt directly on your python interpreter, or you could create a venv to isolate its dependencies.
Read more about [installing dlt](/docs/reference/installation.mdx) here.

dlt is designed with minimal dependency footprint. Once you install dlt, you may also need to install the destination package.
The instructions to do so will appear when you init a pipeline with a different destination.
For example, initing a biguqery pipeline will create a requirements file with `dlt[bigquery]` in it, and you will be asked to pip install it.

