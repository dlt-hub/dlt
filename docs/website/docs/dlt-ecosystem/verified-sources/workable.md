# Workable

Workable is an online platform for posting jobs and managing the hiring process. With Workable, employers can create job listings, receive applications, track candidates, collaborate with team members, schedule interviews, and manage the overall hiring workflow.

With the Workable `dlt` verified source and pipeline example, you have the flexibility to load various endpoints such as "members", "recruiters", etc to the destination of your choice. Additionally, you can load details about "candidates" and "jobs".

### Default endpoints

This verified source loads data from the following default endpoints:

| Endpoint | Description |
| --- | --- |
| members | individuals who have access to your Workable account |
| recruiters | individuals who are responsible for managing the hiring and recruitment processes  |
| stages | represent the different steps or phases in the hiring process for a job position |
| requisitions | formal request made by an organization to fill a specific job opening or position |
| jobs | individual job postings or job listings created by employers or recruiters |
| custom_attributes | additional fields or data points that you can define and assign to candidates or jobs |
| events | specific occurrences or actions related to the hiring and recruitment process |
| candidates | individuals who have applied for job positions within an organization |

### Dependent endpoints

In addition to the *default endpoints* "candidate" and "jobs," there are specific endpoints that depend on them and are categorized as ***dependent endpoints***. Here is a list of these endpoints:

| Endpoint | Dependent endpoints |  |
| --- | --- | --- |
| candidates/:id/activities | retrieve activities or events related to the candidate's interaction with the hiring process. |  |
| candidates/:id/offer | a specific candidate's offer information |  |
| jobs/:shortcode/activities | activities associated with a particular job posting identified by its shortcode |  |
| jobs/:shortcode/application_form | application form details for a specified job |  |
| jobs/:shortcode/questions | retrieve the interview questions associated with a specific job posting |  |
| jobs/:shortcode/stages | retrieve information about the hiring stages associated with a particular job |  |
| jobs/:shortcode/custom_attributes | retrieve custom attributes associated with a particular job posting |  |
| jobs/:shortcode/members | retrieve information about the members associated with a particular job within the Workable system |  |
| jobs/:shortcode/recruiters | retrieve the list of recruiters associated with a particular job. |  |

## Grab API credentials

1. Sign in to your Workable account.
2. Click on the user menu icon located in the top right corner and select "Settings".
3. In the left-hand menu, navigate to the "Integrations" section under "RECRUITING".
4. On the integrations page, locate the "ACCESS TOKEN" section and click on the option to generate a new token.
5. Safely copy the newly created token as it will be used to configure the *dlt.secrets.toml* file.

## Initialize the Workable verified source and pipeline example

To get started with this verified source follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:
    ```properties
    dlt init workable duckdb
    ```
    This command will initialize your verified source with Workable and creates pipeline example with duckdb as the destination. If you'd like to use a different destination, simply replace **duckdb** with the name of your preferred destination. You can find supported destinations and their configuration options in our [documentation](../destinations/).

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started.
    ```shell
    workable
    ├── .dlt
    │   ├── config.toml
    │   └── secrets.toml
    ├── workable
    │   └── __init__.py
    │   └── settings.py
    │   └── workable_client.py
    ├── .gitignore
    ├── requirements.txt
    └── workable_pipeline.py
    ```

## Add credentials

1. Inside the **`.dlt`** folder, you'll find a file called **`secrets.toml`**, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

    Here's what the file looks like:

    ```toml
    # put your secret values and credentials here. do not share this file and do not push it to github
    [sources.workable]
    access_token = "access_token" # Your Workable token copied above
    ```

2. Replace the value of **`access_token`** with the one that [you copied above](workable.md#grab-api-credentials). This will ensure that your data pipeline example can access your Workable resources securely.
3. Next, follow the instructions in [Destinations](../destinations/) to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.
4. Inside the **`.dlt`** folder, you'll find a file called **`config.toml`**, where you can securely store your pipeline configuration details.

    Here's what the *config.toml* looks like:

    ```toml
    [sources.workable]
    subdomain = "subdomain" # please set me up!
    ```

5. Replace the value of the subdomain with the one in the address bar, for example, if your URL is **https://my-company.workable.com/**, your subdomain would be "**my-company**".

## Run the pipeline example

1. Install the necessary dependencies by running the following command:
    ```properties
    pip install -r requirements.txt
    ```

2. Now the pipeline can be run by using the command:
    ```properties
    python3 workable_pipeline.py
    ```

3. To make sure that everything is loaded as expected, use the command:
    ```properties
    dlt pipeline <pipeline_name> show
    ```
    (For example, the pipeline_name for the above pipeline example is `workable`, you may also use any custom name instead)

## Customization

To load data to the destination using this verified source, you have the option to write your own methods. However, it is important to note how the `DEFAULT_ENDPOINTS` and `DEFAULT_DETAILS` are defined by default (see `workable/settings.py`).

```python
DEFAULT_ENDPOINTS = ("members", "recruiters", "stages", "requisitions", "jobs", "custom_attributes","events")

DEFAULT_DETAILS = {
    "candidates": ("activities", "offer"),
    "jobs": ("activities", "application_form", "questions", "stages", "custom_attributes", "members", "recruiters" ),
}
```

### Source and resource methods

`dlt` works on the principle of [sources](https://dlthub.com/docs/general-usage/source) and [resources](https://dlthub.com/docs/general-usage/resource) that for this verified source are found in the `__init__.py` file within the *workable* directory. The `workable_pipeline.py` has two default methods:

#### 1. Source *workable_source*:
```python
@dlt.source(name="workable")
def workable_source(
    access_token: str = dlt.secrets.value,
    subdomain: str = dlt.config.value,
    start_date: Optional[DateTime] = None,
    load_details: bool = False,
) -> Iterable[DltResource]:
```

- **`access_token`**: A token required to authenticate the Workable API. This token is defined in the *dlt.secret.toml* file.
- **`subdomain`**: The name of your Workable account. It should be specified in the *dlt.config.toml* file.
- **`start_date`**: An optional parameter that allows you to specify a start date for retrieving data. If not provided, the default value is set to January 1, 2000.
- **`load_details`**: A boolean parameter that should be set to true if you want to load the dependent endpoints along with the main endpoints (”jobs” and “candidates”).

The above method loads data from all the default endpoints and “candidates” endpoints to the destination.
For the workable verified source, it's important to note that the majority of endpoints do not include the ‘*updated_at’* key. As a result, we often need to load data in '*replace*' mode, where the existing data is completely replaced. However, there is an exception for the '*candidates*' endpoints as they can be loaded incrementally, in ‘*merge’* mode.

#### a) Resource *candidate_resource*:

```python
@dlt.resource(name="candidates", write_disposition="merge", primary_key="id")
def candidates_resource(
    updated_at: Optional[Any] = dlt.sources.incremental(
        "updated_at", initial_value=workable.start_date_iso
    )
) -> Iterable[TDataItem]:
```

- **`updated_at`**: utilizes the *dlt.sources.incremental* method. It defaults to start_date defined in the function and if not defined defaults to Jan 1, 2000.

Please note that this method is specifically defined for the **Candidates** endpoint.

### **Create Your Data Loading Pipeline**

If you wish to create your own pipelines you can leverage source and resource methods in verified sources as discussed above.

To create your data pipeline using single loading and [incremental data loading](https://dlthub.com/docs/general-usage/incremental-loading) (for the **Candidates** endpoint), follow these steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset. To read more about pipeline configuration, please refer to our [documentation here](https://dlthub.com/docs/general-usage/pipeline).

    ```python
    pipeline = dlt.pipeline(
        pipeline_name="workable_pipeline",  # Use a custom name if desired
        destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="workable_dataset"  # Use a custom name if desired
    )
    ```

2. To load data starting from a specific date and include dependent endpoints, you can utilize the 'start_date' parameter and set it to the desired date. For example, to load data from January 1, 2022, and include all corresponding details for dependent endpoints:

    ```python
    load_data = workable_source(start_date=datetime(2022, 1, 1), load_details=True)
    ```

    > Note: The `load_details` parameter controls whether the [dependent endpoints](workable.md#dependent-endpoints) are loaded. If you want to load the dependent endpoints, set this parameter to **`True`** in your pipeline run. If not, **`False`**.

3. Use the method `pipeline.run()` to execute the pipeline.

    ```python
    load_info = pipeline.run(load_data)
    # print the information on data that was loaded
    print(load_info)
    ```

4. To use the method `pipeline.run()` to load custom endpoints “candidates” and “*members*”, the above script may be modified as:

    ```python
    load_info = pipeline.run(load_data.with_resources("candidates", "members")
    # print the information on data that was loaded
    print(load_info)
    ```

    > Note: Please note that in the above run, the "*candidates*" endpoint is loaded incrementally using the 'merge' mode, utilizing the '*updated_by*' key. During the initial run, the '*updated_by*' value will be set to the specified start date (if provided). However, for subsequent runs, the pipeline will only merge data for the "*candidates*" endpoint starting from the '*updated_by*.*last_value*', which represents the last '*updated_by*' value from the previous pipeline run. It's important to note that for all other endpoints, data is loaded in 'replace' mode.


5. For using incremental loading for candidates endpoint, it's important to keep the pipeline name and destination dataset name unchanged. The pipeline name is crucial for retrieving the [state](https://dlthub.com/docs/general-usage/state) of the last pipeline run, which includes the end date needed for loading data incrementally. Modifying these names can lead to [“full_refresh”](https://dlthub.com/docs/general-usage/pipeline#do-experiments-with-full-refresh) which will disrupt the tracking of relevant metadata(state) for [incremental data loading](https://dlthub.com/docs/general-usage/incremental-loading).
6. If you want to load data from the “jobs” endpoint and also load the endpoints dependent on like activities", "application_form". You can do that as follows:

    ```python
    load_data = workable_source(start_date=datetime(2022, 2, 1), load_details=True)
    #please set the load_details as True to load all the dependent endpoints.
    load_info = pipeline.run(load_data.with_resources("jobs","jobs_activities","jobs_application_form"))
    # print the information on data that was loaded
    print(load_info)
    ```

> Note: In the pipeline run mentioned above, we are loading two [dependent endpoints](workable.md#dependent-endpoints). “jobs_activities” and “jobs_application_form”. To ensure that these endpoints are loaded properly, the **`load_details`** parameter is set as **`True`**.

That’s it! Enjoy running your Workable DLT pipeline!
