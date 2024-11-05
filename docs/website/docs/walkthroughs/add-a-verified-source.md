---
title: Add a verified source
description: How to create a pipeline from a verified source
keywords: [how to, add a verified source]
---

# Add a verified source

Follow the steps below to create a [pipeline](../general-usage/glossary.md#pipeline) from a
[verified source](../general-usage/glossary.md#verified-source) contributed by `dlt` users.

Please make sure you have [installed `dlt`](../reference/installation.md) before following the
steps below.

## 1. Initialize project

Create a new empty directory for your `dlt` project by running:

```sh
mkdir various_pipelines
cd various_pipelines
```

List available sources to see their names and descriptions:

```sh
dlt init --list-sources
```

Now pick one of the source names, for example, `pipedrive` and a destination, i.e., `bigquery`:

```sh
dlt init pipedrive bigquery
```

The command will create your pipeline project by copying over the `pipedrive` folder and creating a
`.dlt` folder:

```text
├── .dlt
│   ├── config.toml
│   └── secrets.toml
├── pipedrive
│   └── helpers
│   └── __init__.py
│   └── settings.py
│   └── typing.py
├── .gitignore
├── pipedrive_pipeline.py
└── requirements.txt
```

After running the command, read the command output for the instructions on how to install the
dependencies:

```text
Verified source pipedrive was added to your project!
* See the usage examples and code snippets to copy from pipedrive_pipeline.py
* Add credentials for bigquery and other secrets in .dlt/secrets.toml
* Add the required dependencies to pyproject.toml:
  dlt[bigquery]>=0.3.1
  If the dlt dependency is already added, make sure you install the extra for bigquery to it
  If you are using poetry you may issue the following command:
  poetry add dlt -E bigquery

* Read https://dlthub.com/docs/walkthroughs/create-a-pipeline for more information
```

So make sure you install the requirements with `pip install -r requirements.txt`. When deploying to
an online orchestrator, you can install the requirements to it from requirements.txt in the ways
supported by the orchestrator.

Finally, run the pipeline, fill the secrets.toml with your credentials or place your credentials in
the supported locations.

## 2. Adding credentials

For adding them locally or on your orchestrator, please see the following guide
[credentials](add_credentials).

## 3. Customize or write a pipeline script

Once you have initialized the pipeline, you will have a sample file `pipedrive_pipeline.py`.

This is the developer's suggested way to use the pipeline, so you can use it as a starting point -
in our case, we can choose to run a method that loads all data, or we can choose which endpoints
should load.

You can also use this file as a suggestion and write your own instead.

## 4. Hack a verified source

You can modify an existing verified source in place.

- If that modification is generally useful for anyone using this source, consider contributing it
  back via a PR. This way, we can ensure it is tested and maintained.
- If that modification is not a generally shared case, then you are responsible for maintaining it.
  We suggest making any of your own customizations modular if possible, so you can keep pulling the
  updated source from the community repo in the event of source maintenance.

## 5. Add more sources to your project

```sh
dlt init chess duckdb
```

To add another verified source, just run the `dlt init` command at the same location as the first
pipeline:

- The shared files will be updated (secrets, config).
- A new folder will be created for the new source.
- Do not forget to install the requirements for the second source!

## 6. Update the verified source with the newest version

To update the verified source you have to the newest online version just do the same init command in
the parent folder:

```sh
dlt init pipedrive bigquery
```

## 7. Advanced: Using dlt init with branches, local folders, or git repos

To find out more info about this command, use --help:

```sh
dlt init --help
```

To deploy from a branch of the `verified-sources` repo, you can use the following:

```sh
dlt init source destination --branch <branch_name>
```

To deploy from another repo, you could fork the verified-sources repo and then provide the new repo URL as below, replacing `dlt-hub` with your fork name:

```sh
dlt init pipedrive bigquery --location "https://github.com/dlt-hub/verified-sources"
```

