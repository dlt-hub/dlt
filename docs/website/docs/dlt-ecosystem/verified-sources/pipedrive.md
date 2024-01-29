---
title: Pipedrive
description: dlt verified source for Pipedrive API
keywords: [pipedrive api, pipedrive verified source, pipedrive]
---

# Pipedrive

:::info Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our Slack community](https://dlthub.com/community)
or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::

[Pipedrive](https://developers.pipedrive.com/docs/api/v1) is a cloud-based sales Customer
Relationship Management (CRM) tool designed to help businesses manage leads and deals, track
communication, and automate sales processes.

This Pipedrive `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive_pipeline.py)
loads data using “Pipedrive API” to the destination of your choice.

Sources and resources that can be loaded using this verified source are:

| Name         | Description                                                                                |
|--------------|--------------------------------------------------------------------------------------------|
| activity     | Refers to scheduled events or tasks associated with deals, contacts, or organizations      |
| organization | Company or entity with which you have potential or existing business dealings              |
| person       | Individual contact or lead with whom sales deals can be associated                         |
| product      | Goods or services that a company sells, which can be associated with deals                 |
| deal         | Potential sale or transaction that you can track through various stages                    |
| pipeline     | Visual representation of your sales process, displaying the stages and deals in each stage |
| stage        | Specific step in a sales process where a deal resides based on its progress                |
| user         | Individual with a unique login credential who can access and use the platform              |

## Setup Guide

### Grab API token

1. Set up a Pipedrive account.
1. In Pipedrive, go to your name (in the top right corner).
1. Select company settings.
1. Go to personal preferences.
1. Select the API tab.
1. Copy your API token (to be used in the dlt configuration).

> Note: The Pipedrive UI, which is described here, might change.
The full guide is available at [this link.](https://pipedrive.readme.io/docs/how-to-find-the-api-token)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```bash
   dlt init pipedrive duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive_pipeline.py)
   with Pipedrive as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe.

   Here's what the file looks like:

   ```toml
   [sources.pipedrive.credentials]
   # Note: Do not share this file and do not push it to GitHub!
   pipedrive_api_key = "PIPEDRIVE_API_TOKEN" # please set me up !
   ```

1. Replace `PIPEDRIVE_API_TOKEN` with the API token you [copied above](#grab-api-token).

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```bash
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```bash
   python pipedrive_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```bash
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `pipedrive`, you may also use
   any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Default endpoints

You can write your own pipelines to load data to a destination using this verified source. However,
it is important to note the complete list of the default endpoints given in
[pipedrive/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive/settings.py)

In the file "settings.py" you'll find "ENTITY_MAPPINGS" and "RECENTS_ENTITIES".

- `ENTITY_MAPPING`: is a list detailing entities in Pipedrive. Each tuple contains the entity's
  name, its associated custom fields, and any additional configuration. Provides dynamic access to
  Pipedrive entities and custom fields.

- `RECENTS_ENTITIES`: is a dictionary that provides a mapping between singular entity names and
  their plural forms.

In summary, both "ENTITY_MAPPINGS" and "RECENTS_ENTITIES" standardize interactions with the
Pipedrive API.

### Source `pipedrive_source`

This function returns a list of resources including activities, deals, custom_fields_mapping and
other resources data from Pipedrive API.

```python
@dlt.source(name="pipedrive")
def pipedrive_source(
    pipedrive_api_key: str = dlt.secrets.value,
    since_timestamp: Optional[Union[pendulum.DateTime, str]] = dlt.config.value,
) -> Iterator[DltResource]:
```

`pipedrive_api_key`: Authentication token for Pipedrive, configured in ".dlt/secrets.toml".

`since_timestamp`: Starting timestamp for incremental loading. By default, complete history is loaded
 on the first run. And new data in subsequent runs.

> Note: Incremental loading can be enabled or disabled depending on user prefrences.

### Resource `iterator RECENTS_ENTITIES`

This code generates resources for each entity in
[RECENTS_ENTITIES](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive/settings.py),
stores them in endpoints_resources, and then loads data from each endpoint to the destination.

```python
endpoints_resources = {}
for entity, resource_name in RECENTS_ENTITIES.items():
    endpoints_resources[resource_name] = dlt.resource(
        get_recent_items_incremental,
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
    )(entity, **resource_kwargs)

    #yields endpoint_resources.values
```

`entity and resource_name`: Key-value pairs from RECENTS_ENTITIES.

`get_recent_items_incremental`: Function given to dlt.resource to generate data.

`name`: Sets the resource's name.

`primary_key`: Designates "id" as the resource's primary key.

`write_disposition`: New data merges with existing data in the destination.

### Transformer `deals_participants`

This function gets the participants of deals from the Pipedrive API and yields the result.

```python
def pipedrive_source(args):
  # Rest of function
   yield endpoints_resources["deals"] |  dlt.transformer(
        name="deals_participants",
        write_disposition="merge",
        primary_key="id"
    )(_get_deals_participants)(pipedrive_api_key)
```

`name`: Names the transformer as "deals_participants".

`write_disposition`: Sets the transformer to merge new data with existing data in the destination.

Similar to the transformer function "deals_participants" is another transformer function named
"deals_flow" that gets the flow of deals from the Pipedrive API, and then yields the result for
further processing or loading.

### Resource `create_state`

This function preserves the mapping of custom fields across different pipeline runs. It is used to
create and store a mapping of custom fields for different entities in the source state.

```python
@dlt.resource(selected=False)
def create_state(pipedrive_api_key: str) -> Iterator[Dict[str, Any]]:
    def _get_pages_for_rename(
        entity: str, fields_entity: str, pipedrive_api_key: str
    ) -> Dict[str, Any]:
```

It processes each entity in ENTITY_MAPPINGS, updating the custom fields mapping if a related fields
entity exists. This updated state is then saved for future pipeline runs.

### Other functions

Similar to the above functions, there are the following:

`custom_fields_mapping`: Transformer function that parses and yields custom fields' mapping in order
to be stored in destination by dlt.

`leads`: Resource function that incrementally loads Pipedrive leads by update_time.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```python
   pipeline = dlt.pipeline(
       pipeline_name="pipedrive",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="pipedrive_data"  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our
   [documentation](../../general-usage/pipeline).

1. To print source info:

   ```python
   pipedrive_data = pipedrive_source()
   #print source info
   print(pipedrive_data)
   # list resource names
   print(pipedrive_data.resources.keys())
   # print `persons` resource info
   print(pipedrive_data.resources["persons"])
   ```

1. To load all the data in Pipedrive:

   ```python
   load_data = pipedrive_source() # calls the source function
   load_info = pipeline.run(load_info) #runs the pipeline with selected source configuration
   print(load_info)
   ```

1. To load data from selected resources:

   ```python
   #To load custom fields, include custom_fields_mapping for hash to name mapping.
   load_data = pipedrive_source().with_resources("products", "deals", "deals_participants", "custom_fields_mapping")
   load_info = pipeline.run(load_data) #runs the pipeline loading selected data
   print(load_info)
   ```

1. To load data from a start date:

   ```python
   # Configure a source for 'activities' starting from the specified date.
   # The 'custom_fields_mapping' is incorporated to convert custom field hashes into their respective names.
   activities_source = pipedrive_source(
       since_timestamp="2023-03-01 00:00:00Z"
   ).with_resources("activities", "custom_fields_mapping")

   # Run the pipeline with both sources
   load_info = pipeline.run(activities_source)
   print(load_info)
   ```
