---
title: Asana
description: dlt verified source for Asana API
keywords: [asana api, verified source, asana]
---
import Header from './_source-info-header.md';

# Asana

<Header/>

[Asana](https://asana.com) is a widely used web-based project management and collaboration tool that helps teams stay
organized, focused, and productive. With Asana, team members can easily create, assign, and track
tasks, set deadlines, and communicate with each other in real-time.

When you use Asana, you can create various resources like "projects", "tasks", "users",
"workspaces", and others mentioned below to help you manage your work effectively.

Resources that can be loaded using this verified source are:

| Name       | Description                                                                                           |
| ---------- | ----------------------------------------------------------------------------------------------------- |
| workspaces | People, materials, or assets required to complete a task or project successfully                      |
| projects   | Collections of tasks and related information                                                          |
| sections   | Used to organize tasks within a project into smaller groups or categories                             |
| tags       | Labels that can be attached to tasks, projects, or conversations to help categorize and organize them |
| stories    | Updates or comments that team members can add to a task or project                                    |
| teams      | Groups of individuals who work together to complete projects and tasks                                |
| users      | Individuals who have access to the Asana platform                                                     |

To get a complete list of sub-endpoints that can be loaded, see
[asana_dlt/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/asana_dlt/settings.py)

## Setup guide

### Grab credentials

1. To start, head to [the Asana developer console](https://app.asana.com/-/developer_console).
1. Next, click the "Create new token" button in the "Personal Access Token" section.
1. Give your access token a name that is meaningful to you, and take a moment to read and agree to
   the API terms and conditions.
1. After that, click "Create token" and you're all set!
1. Now, copy your Access token safely as it is only displayed once.
1. This token will be used to configure `.dlt/secrets.toml`, so keep it secure and don't share it
   with anyone.

> Note: The Asana UI, which is described here, might change.
The full guide is available at [this link.](https://developers.asana.com/docs/authentication)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init asana_dlt duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/asana_dlt_pipeline.py)
   with Asana as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md) as
   the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source guide](../../walkthroughs/add-a-verified-source).

### Add credentials

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can
   securely store your access tokens and other sensitive information. It's important to handle this
   file with care and keep it safe. Here's what the file looks like:
   ```toml
   [sources.asana_dlt]
   access_token = "access_token"
   ```
1. Replace the value of `access_token` with the one that [you copied above](asana#grab-credentials).
   This will ensure that your data pipeline can access your Asana resources securely.
1. Finally, follow the instructions in the
   [destination documentation](../../dlt-ecosystem/destinations) to add credentials for your chosen
   destination. This will ensure that your data is properly routed to its final destination.

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```sh
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python asana_dlt_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `asana`, you may also use any
   custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Default endpoints

You can write your own pipelines to load data to a destination using this verified source. However,
it is important to note the complete list of the default endpoints given in
[asana_dlt/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/asana_dlt/settings.py)

### Source `asana_source`

This is a `dlt.source` function, which returns a list of DltResource objects: "workspaces",
"projects", "sections","tags","tasks","stories", "teams", and "users".

```py
@dlt.source
def asana_source(access_token: str = dlt.secrets.value) -> Any:
    return [
      workspaces, projects, sections, tags, tasks, stories, teams, users,
    ]
```

`access_token`: Token required to authenticate the Asana API. This token is defined in the
`.dlt/secret.toml` file.

### Resource `workspaces`

This is a `dlt.resource` function, which returns collections of tasks and related information.

```py
@dlt.resource(write_disposition="replace")
def workspaces(
    access_token: str = dlt.secrets.value,
    fields: Iterable[str] = WORKSPACE_FIELDS
) -> Iterable[TDataItem]:
    yield from get_client(access_token).workspaces.find_all(opt_fields=",".join(fields))
```

`access_token`: Token required to authenticate the Asana API. This token is defined in the
`.dlt/secret.toml` file.

`fields`: A list of workspace fields to be fetched from `asana_dlt/settings.py`. For example, "gid",
"name", "is_organization", etc.

To fetch all workspaces from Asana, the function uses the
`get_client(access_token).workspaces.find_all` method. The `opt_fields` parameter is set to a string
of all fields joined by commas. A generator is used with the `yield from` statement to provide each
workspace from the iterator obtained. This enables the workspaces to be consumed one by one.

### Resource-transformer `projects`

In addition to these source and resource functions, there are seven transformer functions for various endpoints like "projects", "sections", "tags", "tasks", "stories", "teams", and "users". The transformer functions transform or process data from one or more resources.

The transformer function `projects` processes data from the `workspaces` resource. It fetches and returns a list of projects for a given workspace from Asana.

```py
@dlt.transformer(
    data_from=workspaces,
    write_disposition="replace",
)
@dlt.defer
def projects(
    workspace: TDataItem,
    access_token: str = dlt.secrets.value,
    fields: Iterable[str] = PROJECT_FIELDS,
) -> Iterable[TDataItem]:
    ...
```

`workspace`: The data item from the 'workspaces' resource.

`access_token`: Token required to authenticate the Asana API. This token is defined in the `.dlt/secret.toml` file.

`fields`: A list of workspace fields to be fetched from `asana_dlt/settings.py`. For example, "name", "members", "completed", etc.

It uses the `@dlt.defer` decorator to enable parallel run in a thread pool.

### Resource-transformer `tasks`

This [incremental](../../general-usage/incremental-loading.md) resource-transformer fetches all tasks for a given project from Asana.

```py
@dlt.transformer(data_from=projects, write_disposition="merge", primary_key="gid")
def tasks(
    project_array: t.List[TDataItem],
    access_token: str = dlt.secrets.value,
    modified_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        "modified_at", initial_value=DEFAULT_START_DATE
    ),
    fields: Iterable[str] = TASK_FIELDS,
) -> Iterable[TDataItem]:
    ...
```

`workspace`: The data item from the 'projects' resource.

`access_token`: Token required to authenticate the Asana API. This token is defined in the `.dlt/secret.toml` file.

`modified_at`: The date from which to fetch modified tasks.

`fields`: A list of workspace fields to be fetched from `asana_dlt/settings.py`. For example, "name", "assignee", "completed", etc.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

To create your data pipeline using single loading for the "workspaces" and "projects" endpoints, follow these steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="asana_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="asana_dataset"  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our [documentation](../../general-usage/pipeline).

1. To load the data from all the fields, you can utilize the `asana_source` method as follows:

   ```py
   load_data = asana_source()
   ```

1. Use the method `pipeline.run()` to execute the pipeline.

   ```py
   load_info = pipeline.run(load_data)
   # print the information on data that was loaded
   print(load_info)
   ```

1. To use the method `pipeline.run()` to load custom endpoints "workspaces" and "projects", the above script may be modified as:

   ```py
   load_info = pipeline.run(load_data.with_resources("workspaces", "projects"))
   # print the information on data that was loaded
   print(load_info)
   ```
<!--@@@DLT_TUBA asana_dlt-->

