---
title: Hubspot
description: dlt verified source for Hubspot API
keywords: [hubspot api, hubspot verified source, hubspot]
---

# HubSpot

:::info Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our Slack community](https://join.slack.com/t/dlthub-community/shared_invite/zt-1n5193dbq-rCBmJ6p~ckpSFK4hCF2dYA)
or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::

HubSpot is a customer relationship management (CRM) software and inbound marketing platform that
helps businesses to attract visitors, engage customers, and close leads.

This Hubspot `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/hubspot_pipeline.py)
loads data using “Hubspot API” to the destination of your choice.

| Name                       | Description                                                            |
| -------------------------- | ---------------------------------------------------------------------- |
| contacts                   | visitors, potential customers, leads                                   |
| companies                  | information about organizations                                        |
| deals                      | deal records, deal tracking                                            |
| tickets                    | request for help from customers or users                               |
| products                   | pricing information of a product                                       |
| quotes                     | price proposals that salespeople can create and send to their contacts |
| hubspot_events_for_objects | web analytics events for a given object type and object ids            |

To get details about endpoints that can be loaded, see
[hubspot/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/hubspot/settings.py)

## Setup Guide

### Grab credentials

> Note: As of November 30, 2022, HubSpot API Keys are being deprecated and are no longer supported.
Instead, we recommend to authenticate using a private app access token or OAuth access token.

Create a private app and get an authentication token before running the
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/hubspot_pipeline.py).
Follow these steps:

1. In HubSpot, click the ⚙️ icon to access settings.

1. Under "Account Setup" in the left sidebar, choose "Integrations" > "Private Apps".

1. Select “Create a private app”.

1. In the “Basic Info” tab, provide a name and description.

1. In the “Scopes” tab, grant:

   - Read scopes for CMS, CRM, and Settings.
   - Permissions for:
    ```
    business-intelligence, actions, crm.export, e-commerce, oauth, tickets
    ```

1. Click "Create app" > "Continue Creating".

1. Click "Show token" and store it for ".dlt/secrets.toml".


> Note: The Hubspot UI, which is described here, might change.
The full guide is available at [this link.](https://knowledge.hubspot.com/integrations/how-do-i-get-my-hubspot-api-key)


### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```bash
   dlt init hubspot duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/hubspot_pipeline.py)
   with Hubspot as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credentials

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can
   securely store your access tokens and other sensitive information. It's important to handle this
   file with care and keep it safe. Here's what the file looks like:

   ```toml
   # put your secret values and credentials here
   # do not share this file and do not push it to github
   [sources.hubspot]
    api_key = "api_key" # please set me up!
   ```

1. Replace the access_token value with the [previously copied one](hubspot.md#grab-credentials) to
   ensure secure access to your Hubspot resources.

1. Enter credentials for your chosen destination as per the [docs](../destinations/).

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```bash
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```bash
   python hubspot_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```bash
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `hubspot_pipeline`, you may
   also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Default endpoints

You can write your own pipelines to load data to a destination using this verified source. However,
it is important to note the complete list of the default endpoints given in
[hubspot/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/hubspot/settings.py)

### Source `hubspot`

This function returns a list of resources to load companies, contacts, deals, tickets, products, and
web analytics events data into the destination.

```python
@dlt.source(name="hubspot")
def hubspot(
    api_key: str = dlt.secrets.value,
    include_history: bool = False,
) -> Sequence[DltResource]:
```

`api_key`: The key used to authenticate with the HubSpot API. Configured in "secrets.toml".

`include_history`: This parameter, when set to "True", loads the history of property changes for the
specified entities.

### Resource `companies`

This resource function fetches data from the "companies" endpoint and loads it to
the destination, replacing any existing data.

```python
@dlt.resource(name="companies", write_disposition="replace")
def companies(
   api_key: str = api_key,
   include_history: bool = include_history,
   props: Sequence[str] = DEFAULT_COMPANY_PROPS,
   include_custom_props: bool = True,
) -> Iterator[TDataItems]:
   """Hubspot companies resource"""
   yield from crm_objects(
      "company",
      api_key,
      include_history=include_history,
      props=props,
      include_custom_props=include_custom_props,
   )
```

This resource function takes the same arguments, `api_key` and `include_history` as the "husbpot"
source described [above](hubspot.md#source-hubspot), but also supports two additional.
`include_custom_props` - indicates if all the properties of CRM objects, except Hubspot driven
(prefixed with `hs_`), are to be extracted. `props` - the list of properties to extract
in addition to the custom properties. Similar to this, resource functions "contacts",
"deals", "tickets", "products", and "quotes" retrieve data from the Hubspot API.

### Resource `hubspot_events_for_objects`

This function loads web analytics events for specific objects from Hubspot API into the destination.

```python
@dlt.resource
def hubspot_events_for_objects(
     object_type: THubspotObjectType,
     object_ids: List[str],
     api_key: str = dlt.secrets.value,
     start_date: pendulum.DateTime = STARTDATE,
) -> DltResource:
```

`object_type`: One of the Hubspot object types as defined in
[hubspot/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/hubspot/settings.py).

`object_ids`: List of object ids to track events.

`api_key`: The key used to authenticate with the HubSpot API. Configured in "secrets.toml".

`start_date`: The initial date time from which start getting events, default to "01-01-2000",
configured in
[hubspot/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/hubspot/settings.py).

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```python
   pipeline = dlt.pipeline(
       pipeline_name="hubspot",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="hubspot_data"  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our
   [documentation](../../general-usage/pipeline).

1. To load all the data from contacts, companies, deals, products, tickets, and quotes into the destination.

   ```python
   load_data = hubspot()
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

1. To load data from contacts and companies, with time history using "with_resources" method.

   ```python
   load_data = hubspot(include_history=True).with_resources("companies","contacts")
   load_info = pipeline.run(load_data)
   print(load_info)
   ```
    1. `include_history` loads property change history and entities as separate tables. By default set as False.

1. By default, all the custom properties of a CRM object are extracted. If you want only particular fields,
    set the flag `include_custom_props=False` and add a list of properties with the `props` arg.

   ```python
   load_data = hubspot()
   load_data.contacts.bind(props=["date_of_birth", "degree"], include_custom_props=False)
   load_info = pipeline.run(load_data.with_resources("contacts"))
   ```

1. If you want to read all the custom properties of CRM objects and some additional (e.g. Hubspot driven) properties.

   ```python
   load_data = hubspot()
   load_data.contacts.bind(props=["hs_content_membership_email", "hs_content_membership_email_confirmed"])
   load_info = pipeline.run(load_data.with_resources("contacts"))
   ```


1. To load the web analytics events of a given object type.

   ```python
   resource = hubspot_events_for_objects("company", ["7086461639", "7086464459"])
   # Here, object type : company, and object ids : 7086461639 and 7086464459
   load_info = pipeline.run([resource])
   print(load_info)
   ```
    1. This function uses "object_type" and "object_id" as arguments.

    1. This function loads data incrementally and tracks the `occurred_at.last_value` parameter from
    the previous pipeline run. Refer to our official documentation for more information on [incremental loading](../../general-usage/incremental-loading.md).
