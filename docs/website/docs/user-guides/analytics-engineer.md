### Use case #1: Develop dbt packages on top of existing dlt pipelines

Dlt automatically transitions unstructured data to structured. This means that the loaded data structure is a consequence of the initial unstructured data, except clean, typed and normalised.

However, this data usually needs to be rearranged to bring it to a structure that other analysts or business users can answer questions with. For example, consolidating this data or into tables that represent the business process and entities makes it easier for all downstream consumers.

There is little benefit to separate a pipeline between tools, so dlt supports a dbt runner which allows you to create a virtual environment, install dbt on it, pass it credentials and run a dbt package from a local or online location. To learn more about how to do that, see our docs page TODO

[run dbt from local or repository](./using-loaded-data/transforming-the-data)

### Use case #2: Clean type or customise how data is produced or loaded

dlt allows you to customise how data is produced, enabling you to filter, modify, rename, or differently customise the data that arrives at your destination. By doing it in python, you can make changes either before dlt normalises the data, or after, enabling you to choose very granularly what to do.

Read here about possible customisations in our docs, for example: [pseudonymizing_columns](./customizations/customizing-pipelines/pseudonymizing_columns)

### Use case #3: Develop your own pipelines in a similar declarative way, so you can use engineering that was already done for you

dlt was designed from the start for the open source data tool users. It enables people who have never before built a pipeline to go from raw data in python to structured data in a database in minutes.

It’s meant to be a true open source productivity tool without unnecessary complexities designed to capture your credit card.

It features a declarative way to configure loading modes, and handles all of the engineering for you. Dlt was made with you in mind, while the engineering behind it allows you to leverage the best loader that comes complete with support for schema migrations, data typing, performance hint declaration, schema management, etc.

What’s more, you can develop a pipeline that includes a dbt package and thus use it end to end to deliver analytics. And thanks to our duckdb support, you can easily develop locally without needing a connection to your production warehouse.

Read more about it here

- build a pipeline [run dbt from local or repository](./using-loaded-data/transforming-the-data)
- incremental loading [incremental loading](./general-usage/incremental-loading)
- Advanced: performance hints and schema [adjust a schema](./walkthroughs/adjust-a-schema)
- dbt duckdb example [colab duckdb](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)

### Use case #4: Contribute dbt packages back

We do not like reinventing the flat tyre. Data modelling is an art that takes deep understanding of the data in order to do well.

We encourage you to contribute back the following types of packages, so other people may also use them. Or perhaps, you will use them later in your next project or job.

- Packages that transition the pipeline data to 3rd normal form. These packages support simpler usage of the data, and creation of Inmon architecture data warehouse.
- Packages that transition the pipeline data to dimensional model. These packages support pivot-table style usage by business user via query builders.

How to contribute packages?

- You can fork our pipelines repo and do a PR from your fork. You can store the dbt package in the pipeline folder.
- You can link your repo into the pipeline readme and the docs.
- If you are unsure how to do it, give us a shout in slack in the technical help channel.
Links: [pipelines repo](https://github.com/dlt-hub/pipelines), [join slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g)