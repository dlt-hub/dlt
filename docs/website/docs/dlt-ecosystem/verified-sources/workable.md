---
title: Workable
description: dlt pipeline for Workable API
keywords: [workable api, workable pipeline, workable]
---
import Header from './_source-info-header.md';

# Workable

<Header/>

[Workable](https://www.workable.com/) is an online platform for posting jobs and managing the hiring process. With Workable,
employers can create job listings, receive applications, track candidates, collaborate with team members, schedule interviews, and manage the overall hiring workflow.

This Workable `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/workable_pipeline.py)
loads data using the “Workable API” to the destination of your choice.

### Default endpoints

This verified source loads data from the following default endpoints:

| Name              | Description                                                                           |
| ----------------- | ------------------------------------------------------------------------------------- |
| members           | Individuals who have access to your Workable account                                  |
| recruiters        | Individuals who are responsible for managing the hiring and recruitment processes     |
| stages            | Represent the different steps or phases in the hiring process for a job position      |
| requisitions      | Formal requests made by an organization to fill a specific job opening or position     |
| jobs              | Individual job postings or job listings created by employers or recruiters            |
| custom_attributes | Additional fields or data points that you can define and assign to candidates or jobs |
| events            | Specific occurrences or actions related to the hiring and recruitment process         |
| candidates        | Individuals who have applied for job positions within an organization                 |

### Dependent endpoints

Besides the main endpoints, for the "candidate" and "jobs" endpoints, the following are their dependent endpoints:

| Name                              | Dependent endpoints                                                                                |
| --------------------------------- | -------------------------------------------------------------------------------------------------- |
| candidates/:id/activities         | Retrieve activities or events related to the candidate's interaction with the hiring process.      |
| candidates/:id/offer              | A specific candidate's offer information                                                           |
| jobs/:shortcode/activities        | Activities associated with a particular job posting identified by its shortcode                    |
| jobs/:shortcode/application_form  | Application form details for a specified job                                                       |
| jobs/:shortcode/questions         | Retrieve the interview questions associated with a specific job posting                            |
| jobs/:shortcode/stages            | Retrieve information about the hiring stages associated with a particular job                      |
| jobs/:shortcode/custom_attributes | Retrieve custom attributes associated with a particular job posting                                |
| jobs/:shortcode/members           | Retrieve information about the members associated with a particular job within the Workable system |
| jobs/:shortcode/recruiters        | Retrieve the list of recruiters associated with a particular job.                                  |

## Setup guide



### Grab API credentials

1. Log into Workable.
1. Click the top right user icon and select "Settings".
1. Under "RECRUITING", select "Integrations" on the left.
1. Find "ACCESS TOKEN" and generate a new token.
1. Safely copy the new token for pipeline configuration.

> Note: The Workable UI, which is described here, might change.
The full guide is available at [this link.](https://help.workable.com/hc/en-us/articles/115015785428-How-do-I-generate-an-API-key-access-token-Pro-)


### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init workable duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/workable_pipeline.py)
   with Workable as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source.](../../walkthroughs/add-a-verified-source).

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```toml
   # put your secret values and credentials here. do not share this file and do not push it to github
   [sources.workable]
   access_token = "access_token" # Your Workable token copied above
   ```

1. Replace the value of "access_token" with the one that
   [you copied above](workable.md#grab-api-credentials). This will ensure that your data pipeline
   example can access your Workable resources securely.

1. Next, you need to configure ".dlt/config.toml", which looks like:

   ```toml
   [sources.workable]
   subdomain = "subdomain" # please set me up!
   ```

1. Replace the subdomain with the value from the address bar. For example, if your URL is
   "https://my-company.workable.com/", use "my-company".

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```sh
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python workable_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `workable`, you may also use
   any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

Note the default definitions of DEFAULT_ENDPOINTS and DEFAULT_DETAILS in "workable/settings.py".

```py
DEFAULT_ENDPOINTS = ("members", "recruiters", "stages", "requisitions", "jobs", "custom_attributes","events")

DEFAULT_DETAILS = {
    "candidates": ("activities", "offer"),
    "jobs": ("activities", "application_form", "questions", "stages", "custom_attributes", "members", "recruiters" ),
}
```

### Source `workable_source`

This function loads data from the default and "candidates" endpoints.
Most endpoints in the workable, verified source lack the 'updated_at' key,
necessitating data loading in 'replace' mode. However, the 'candidates'
endpoints allow incremental 'merge' mode loading.

This source returns a sequence of dltResources that correspond to the endpoints.

```py
@dlt.source(name="workable")
def workable_source(
    access_token: str = dlt.secrets.value,
    subdomain: str = dlt.config.value,
    start_date: Optional[DateTime] = None,
    load_details: bool = False,
) -> Iterable[DltResource]:
   ...
```

`access_token`: Authenticate the Workable API using the token specified in ".dlt/secrets.toml".

`subdomain`: Your Workable account name, specified in ".dlt/config.toml".

`start_date`: Optional. Sets a data retrieval start date; defaults to January 1, 2000.

`load_details`: A boolean parameter. Set to true to load dependent endpoints with main ones ("jobs"
& "candidates").

### Resource `candidate_resource`

This function is used to retrieve "candidates" endpoints.

```py
@dlt.resource(name="candidates", write_disposition="merge", primary_key="id")
def candidates_resource(
    updated_at: Optional[Any] = dlt.sources.incremental(
        "updated_at", initial_value=workable.start_date_iso
    )
) -> Iterable[TDataItem]:
   ...
```

`updated_at`: Uses the dlt.sources.incremental method. Defaults to the function's start_date or Jan
1, 2000 if undefined.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

To create your data pipeline using single loading and [incremental data loading](../../general-usage/incremental-loading) (only for the **Candidates** endpoint), follow these steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
        pipeline_name="workable",  # Use a custom name if desired
        destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="workable_data"  # Use a custom name if desired
   )
   ```

1. To load all data:

   ```py
   load_data = workable_source()
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

   > Note: In the run, the "candidates" endpoint loads incrementally via 'merge' mode using 'updated_by'. All other endpoints load in 'replace' mode.

1. To load data from a specific date, including dependent endpoints:

   ```py
   load_data = workable_source(start_date=datetime(2022, 1, 1), load_details=True)
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

   > For instance, the above loads data from January 1, 2022, with corresponding details.

   > Note: Set the "load_details" parameter to True to load dependent endpoints. Otherwise, use False.

1. To load custom endpoints “candidates” and “members”:

   ```py
   load_info = pipeline.run(load_data.with_resources("candidates", "members"))
   # print the information on data that was loaded
   print(load_info)
   ```

   > Note: "candidates" loads incrementally in merge mode, while "members" uses replace mode.

1. To load data from the “jobs” endpoint and its dependent endpoints like "activities" and "application_form":

   ```py
   load_data = workable_source(start_date=datetime(2022, 2, 1), load_details=True)
   # Set the load_details as True to load all the dependent endpoints.
   load_info = pipeline.run(load_data.with_resources("jobs","jobs_activities","jobs_application_form"))
   print(load_info)
   ```
   > Note: "load_details" parameter is set to True.

1. To use incremental loading for the candidates endpoint, maintain the same pipeline and destination dataset names. The pipeline name helps retrieve the [state](../../general-usage/state) of the last run, essential for incremental data loading. Changing these names might trigger a [“dev_mode”](../../general-usage/pipeline#do-experiments-with-dev-mode), disrupting metadata tracking for [incremental data loading](../../general-usage/incremental-loading).

<!--@@@DLT_TUBA workable-->

