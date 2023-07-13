---
title: Asana
description: dlt verified source for Asana API
keywords: [asana api, asana verified source, asana]
---

# Asana

:::info
Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our slack community](https://dlthub-community.slack.com/join/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g) or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::


Asana is a widely used web-based project management and collaboration tool that helps teams stay organized, focused, and productive. With Asana, team members can easily create, assign, and track tasks, set deadlines, and communicate with each other in real-time.

When you use Asana, you can create various resources like "projects", "tasks", "users", "workspaces", and others mentioned below to help you manage your work effectively. You'll be happy that you can easily load these endpoints via the dlt pipeline.

Resources that can be loaded using this verified source are:

| S.No. | Name | Description |
| --- | --- | --- |
| 1 | workspaces | people, materials, or assets required to complete a task or project successfully |
| 2 | projects | collections of tasks and related information |
| 3 | sections | used to organize tasks within a project into smaller groups or categories |
| 4 | tags | labels that can be attached to tasks, projects, or conversations to help categorize and organize them. |
| 5 | stories | updates or comments that team members can add to a task or project |
| 6 | teams | groups of individuals who work together to complete projects and tasks |
| 7 | users | individuals who have access to the Asana platform |

To see a complete list of all the sub-endpoints supported by this verified source. To get complete list of sub-endpoints that can be loaded, see `asana_dlt/settings.py`.

## Grab Asana credentials

1. To start, head to the Asana developer console by clicking this link: **https://app.asana.com/-/developer_console**.
2. Next, click the "Create new token" button in the "Personal Access Token" section.
3. Give your access token a name that is meaningful to you, and take a moment to read and agree to the API terms and conditions.
4. After that, click "Create token" and you're all set!
5. Now, copy your Access token safely as it is only displayed once.
6. This token will be used to configure secrets.toml, so keep it secure and don't share it with anyone.

## Initialize the Asana verified source and pipeline example

To get started with your data pipeline, follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:
    ```bash
    dlt init asana_dlt duckdb
    ```
    >This command will initialize your pipeline with Asana as the source and duckdb as the destination. If you'd like to use a different destination, simply replace **duckdb** with the name of your preferred destination. You can find supported destinations and their configuration options in our [documentation.](../../dlt-ecosystem/destinations/)
3. After running this command, a new directory will be created with the necessary files and configuration settings to get started. From here, you can begin configuring your pipeline to suit your specific needs.
    ```shell
        working_directory
        ├── .dlt
        │   ├── .pipelines
        │   ├── config.toml
        │   └── secrets.toml
        ├── asana_dlt
        │   └── __init__.py
        │   └── helpers.py
        │   └── settings.py
        ├── .gitignore
        ├── requirements.txt
        └── asana_dlt_pipeline.py
    ```

## Add credentials

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.
    Here's what the file looks like:
    ```toml
    [sources.asana_dlt]
    access_token = "access_token" # please set me up!
    ```
2. Replace the value of `access_token` with the one that [you copied above](../../dlt-ecosystem/verified-sources/asana#grab-asana-credentials). This will ensure that your data pipeline can access your Asana resources securely.
3. Finally, follow the instructions in [Destinations](../../dlt-ecosystem/destinations/) to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:
    ```bash
    pip install -r requirements.txt
    ```
2. You're now ready to run the pipeline! To get started, run the following command:
    ```bash
    python3 asana_dlt_pipeline.py
    ```    
3. Once the pipeline has finished running, you can verify that everything loaded correctly by using the following command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```
    (For example, the pipeline_name for the above pipeline example is `asana`, you may also use any custom name instead)

## Customization

You can write your own methods to load data to the destination using this verified source. However, it is important to note the complete list of the following fields “workspaces”, “projects”, “sections”, “tags”, “stories”, “teams”, and “users” given in `asana_dlt/settings.py.`

### Source and resource methods

`dlt` works on the principle of [sources](../../general-usage/source) and [resources](../../general-usage/resource) that for this verified source are found in the `__init__.py` file within the *asana* directory. The `asana_dlt_pipeline.py` has following default methods:

1. **Source <u>asana_source:</u>**
    ```python
    @dlt.source
    def asana_source(
        access_token: str = dlt.secrets.value,
    ) -> Any:  
      return [ workspaces, projects, sections, tags, tasks, stories, teams, users,
     ]
    ```
    `access_token`: token required to authenticate the Asana API. This token is defined in the *dlt.secret.toml* file.

    The function returns a list of resources: "workspaces", "projects", "sections", "tags", "tasks", "stories", "teams", and "users". These resources represent different endpoints of the Asana API that the data load tool (dlt) can fetch data.

2. **Resource <u>workspaces:</u>**

    ```python
    @dlt.resource(write_disposition="replace")
    def workspaces(
        access_token: str = dlt.secrets.value, fields: Iterable[str] = WORKSPACE_FIELDS
    ) -> Iterable[TDataItem]:
    
       yield from get_client(access_token).workspaces.find_all(opt_fields=",".join(fields))
    ```
    `access_token`: A token required to authenticate the Asana API. This token is defined in the *dlt.secret.toml* file.
    
    `fields`: A list of workspace fields to be fetched from `asana/settings.py`. For example, "gid", "name", "resource_type”, etc. 

    To fetch all workspaces from Asana, the function uses the `get_client(access_token).workspaces.find_all` method. The `opt_fields` parameter is set to a string of all fields joined by commas. A generator is used with the "yield from" statement to provide each workspace from the iterator obtained. This enables the workspaces to be consumed one by one.

3. **Transformer <u>projects:</u>**

    In addition to these source and resource functions, there are seven transformer functions. For various endpoints like “projects”, “sections”, “tags”, “tasks”, “stories”, “teams” and “users”.  Below is an example of one of the transformer functions(projects):

    ```toml
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
 
    return list(
        get_client(access_token).projects.find_all(
            workspace=workspace["gid"],
            timeout=REQUEST_TIMEOUT,
            opt_fields=",".join(fields),
        )
    )
    ```

    `workspace`: The workspace data from the resources.

    `access_token`: The access token to authenticate the Asana API client, provided in the secrets file.

    The transformer functions transform or process data from one or more resources. In this case, the transformer function `projects` process data from the `workspaces` resource. It fetches and returns a list of projects for a given workspace from Asana. The `@dlt.defer` decorator is used to defer the execution of the function until the data from the `workspaces` resource is available.

### Create Your Data Loading Pipeline

If you wish to create your own pipelines you can leverage source and resource methods in verified sources as discussed above.

To create your data pipeline using single loading for “workspaces” and “projects” endpoints follow these steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:
    ```python
    pipeline = dlt.pipeline(
        pipeline_name="asana_pipeline",  # Use a custom name if desired
        destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="asana_dataset"  # Use a custom name if desired
    )
    ```
    >To read more about pipeline configuration, please refer to our [documentation here](../../general-usage/pipeline).

2. To load the data from all the fields, you can utilize the “asana_source” method as follows:
    ```python
    load_data = asana_source()
    ```
3. Use the method `pipeline.run()` to execute the pipeline.
    ```python
    load_info = pipeline.run(load_data) 
    #print the information on data that was loaded 
    print(load_info)
    ```
4. To use the method `pipeline.run()` to load custom endpoints “workspaces” and “projects”, the above script may be modified as:
    ```python
    load_info = pipeline.run(load_data.with_resources("workspaces", "projects")) 
    #print the information on data that was loaded 
    print(load_info)
    ```
    > Note: In the above run, the "workspaces*" and “projects” endpoints are loaded using the 'replace' write disposition. To read more about write disposition please read our official [documentation](../../general-usage/incremental-loading#choosing-a-write-disposition).
    
    Thats it! Enjoy running your Asana `DLT` pipeline!