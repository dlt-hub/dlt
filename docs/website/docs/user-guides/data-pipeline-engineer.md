# So you’re a data engineer and you are considering dlt?

Indeed look no further.

If you are using one of dlt’s supported destinations, you can quickly go from extracting to loading data by just passing your unstructured data to dlt.

### First use case, remove a big chunk of tedious work and most of the ETL maintenance by automating the transition from unstructured (nested, untyped) to structured (tabular, typed) data

Dlt is a library that you can use almost anywhere, so you are not forced to use the dlt prescribed way of doing things. The simplest usage of dlt would be to use it as a unstructured to structured data transition tool.

A dlt pipeline is made of a source, which contains resources, and a connection to the destination, which we call pipeline.

So in the simplest use case, you could pass your unstructured data to the `pipeline` and it will automatically be migrated to structured at the destination.

See how to do that here [pipeline docs](../general-usage/pipeline)

The big advantage of using dlt for this is that it automates what is otherwise an error prone and tedious operation for the data engineer. What’s more, dlt can migrate schemas, removing now only development work but most of the maintenance work as well.

### Second use case, A library to empower everyone on the team.

Dlt is meant to cater to the entire data team, enabling you to standardize how your entire team loads data. This reduces knowledge requirements to get work done and enables collaborative working between engineers and analysts.

- Analysts can use ready-build sources or pass their unstructured data to dlt and dlt will do a good job of creating a sturdy pipeline.  docs: [get an existing pipeline](../walkthroughs/add-a-pipeline), [build a pipeline](../walkthroughs/create-a-pipeline)
- Python first users can heavily customise how dlt sources produce data, as dlt supports selecting, filtering, renaming, anonymising and just about any custom operation you could think of. docs: [build a pipeline](../customizations/customizing-pipelines/renaming_columns)
- Junior data engineers can configure dlt to do what they want - change the loading modes, add performance hints. docs:  [build a pipeline](../walkthroughs/adjust-a-schema)
- Senior data engineers can dig even deeper into customisation options and change schemas, normalisers, or the way pipelines run such as parallelism etc.

### Third use case, dlt is a productivity enhancing tool for yourself

Dlt is meant to support natural workflows that occur in the data team

- dlt offers native support for local development and testing - you can use a local duckdb destination, and use `dlt pipeline show` to generate a web client to display and query the data. Once you are happy, you can simply switch to your production destination and you pipeline will run on production from the first try :)
  - docs:  [show data](../using-loaded-data/exploring-the-data)
  - example: [colab duckdb](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)
- with dbt runner, you can aditionally develop and later run transformations. If you use cross-db compatibility for dbt, your code can even be developed locally such as on duckdb or reused by others if you choose to reshare your code. [run dbt from local or repository](../using-loaded-data/transforming-the-data)
- dlt supports Airflow and other workflow managers, it’s meant to plug into your data stack without causing more overheads. [example how to set up on airflow](../running-in-production/orchestrators/airflow-gcp-cloud-composer)
- dlt allows easy customisation and maintenance of the code - dlt sources are pythonic and simple, no object oriented programming required. Your junior data scientist can fix and customise the pipelines too. [Simple example](https://github.com/dlt-hub/pipelines/blob/master/pipelines/strapi/strapi.py)

### Fourth use case, dlthub community supports solving problems “for good”. No more reinventing the flat tyre in the form of yet another custom pipeline.

- Community support - If you choose to publish your pipelines to the community, they will be reused, improved, upgraded, tests added etc.
- Community distribution support - A pipeline hosted in the public repos (ours or you could fork your own) can be distributed via our command line interface. The command also handles versioning, merging, upgrading.

Read more about our contribution process here, contribute pipelines or open issues for requests [here ](https://github.com/dlt-hub/pipelines)

# Or are you a data engineer who has to take over a dlt pipeline?

In that case, you are in luck! Know the following:

- this pipeline is largely self maintaining. The only thing you must ensure is that the source keeps producing data - from there dlt handles it.
- You can notify schema evolution events, see docs here: [alert schema change](../running-in-production/running#inspect-save-and-alert-on-schema-changes)
- you can use the data by transforming it with dbt, see this example here: [run dbt from local or repository](../using-loaded-data/transforming-the-data)
- If the sources used are public, you can update your pipelines to the latest version from the online repo with the `dlt init`command. If you are the first to fix, you can contribute the fix back so it will be included in the future versions.
  - docs: [init a pipeline](../reference/command-line-interface#dlt-init)
  - docs: [contribute a pipeline](https://github.com/dlt-hub/pipelines)