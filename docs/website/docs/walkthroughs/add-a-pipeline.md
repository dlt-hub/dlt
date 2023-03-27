---
sidebar_position: 2
---

# Add a pipeline

Follow the steps below to add a [pipeline](../glossary.md#pipeline) pipeline contributed by the other `dlt` users.

Please make sure you have [installed `dlt`](../installation.mdx) before following the steps below.

## 1. Initialize project

Create a new empty directory for your `dlt` project by running
```shell
mkdir various_pipelines
cd various_pipelines
```

List available pipelines to see their names and descriptions
```
dlt init --list-pipelines
```

Now pick one of the pipeline names, for example `pipedrive` and a destination ie. `bigquery`.
```
dlt init pipedrive bigquery
```

The command will copy over the pipedrive folder and create a dlt folder, in the case of pipedrive it looks like

```
├── .dlt
│   ├── config.toml
│   └── secrets.toml
├── pipedrive
│   └── __init__.py
├── .gitignore
├── pipedrive_pipeline.py
└── requirements.txt
```

After initing the pipeline, read the command line interface for the command you need to run to install the dependencies.

```
Pipeline pipedrive was added to your project!
* See the usage examples and code snippets to copy from pipedrive_pipeline.py
* Add credentials for bigquery and other secrets in .dlt/secrets.toml
* requirements.txt was created. Install it with:
pip3 install -r requirements.txt
```
So make sure you install the requirements with `pip3 install -r requirements.txt`.
When deploying to an online orchestrator, you can install the requirements to it from requirements.txt in the ways supported by the orchestrator.

Finally, run the pipeline, fill the secrets.toml with your credentials or place your credentials in the supported locations.

## 2. Adding credentials

For adding them locally or on your orchestrator, please see the following guide [credentials](../customization/credentials)

For getting the destination credentials, please see the [guide](../destinations.md).

## 3. Customize or write a pipeline script


Once you inited the pipeline, you will have a sample file  `pipedrive_pipeline.py`.

This is the developer's suggested way to use the pipeline, so you can use it as a starting point - in our case, we can choose to run a method that loads all data, or we can choose which endpoints should load.

You can also use this file as a suggestion and write your own instead.

## 4. Hack a pipeline

You can modify an existing pipeline.
* If that modification is generally useful for anyone using this source, consider contributing it back via a PR. This way, we can ensure it is tested and maintained.
* If that modification is not a generally shared case, then you are responsible for maintaining it. We suggest making any of your own customisations modular is possible, so you can keep pulling the updated pipeline from the community repo in the event of source maintenance.


## 5. Add more pipelines to your project
```
dlt init chess duckdb
```
To add another pipeline, just run the dlt init command at the same location as the first pipeline
- the shared files will be updated (secrets, config)
- a new folder will be created for the new pipeline
- do not forget to install the requirements for the second pipeline!


## 6. Update the pipeline with the newest version
To update the pipeline you have to the newest online version just do the same init command in the parent folder.
```
dlt init pipedrive bigquery
```

## 7. Advanced: Using dlt init with branches, local folders or git repos.
To find out more info about this command, use --help.

```
dlt init --help
```


To deploy from a branch of the pipelines repo, you can use the following:

```
dlt init source destination --branch branchname
```

To deploy from another repo, you could fork the pipelines repo and then provide the new repo url as below, replacing `dlt-hub` with your fork name

```
dlt init pipedrive bigquery --location "https://github.com/dlt-hub/pipelines/pipelines"
```

## 8. Further reading
