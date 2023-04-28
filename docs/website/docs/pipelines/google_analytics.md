# Google Analytics

[Google Analytics](https://marketingplatform.google.com/about/analytics/#?modal_active=none) is a web analytics service that tracks and provides data related to user engagement with your website or application.

## Google Analytics API authentication

Before creating the pipeline, we need to first get the necessary API credentials:

1. Sign in to [[console.cloud.google.com](http://console.cloud.google.com/)].
2. [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create#creating) if you don't already have one.
3. Enable Google Analytics API:
    1. In the left panel under *APIs & Services*, choose *Enabled APIs & services*.
    2. Click on *+ ENABLE APIS AND SERVICES* and find and select Google Analytics API.
    3. Click on *ENABLE*.
4. Generate credentials:
    1. In the left panel under *IAM & Admin*, select *Service Accounts*.
    2. In the service account table click on the three dots under the column "Actions" for the service account that you wish to use.
    3. Select *Manage Keys*.
    4. Under *ADD KEY* choose to *Create a new key*, and for the key type JSON select *CREATE*.
    5. This downloads a .json which contains the credentials that we will be using later.

## Share the Google Analytics Property with the API:

1. To allow the API to access Google Analytics, sign into your google analytics account.
2. Select the website for which you want to share the property.
3. Click on the "Admin" tab in the lower-left corner.
4. Under the "Account" column, click on "Account Access Management."
5. Click on the blue-coloured “+” icon in the top right corner.
6. Select “Add users”, and add the *client_email* with at least viewer privileges. You will find this *client_email* in the JSON that you downloaded above.
7. Finally, click on the “Add” button in the top right corner.

## Initialize the pipeline

We can now create the pipeline.

Initialize a `dlt` project with the following command:

`dlt init google_analytics bigquery`

Here, we chose BigQuery as the destination. To choose a different destination, replace `bigquery` with your choice of destination.

Running this command will create a directory with the following structure:

```sql
directory
├── .dlt
│   ├── .pipelines
│   ├── config.toml
│   └── secrets.toml
└── google_analytics
    ├── helpers
    │   ├── __.init.py__
    │   ├── credentials.py
    │   └── data_processing.py
    ├── __init__.py
		├── setup_script_gcp_oauth.py
└── google_analytics_pipelines.py
└── requirements.txt
```

## Add credentials

1. Open `.dlt/secrets.toml`
2. From the .json that you downloaded earlier, copy `project_id`, `private_key`, and `client_email` under `[sources.google_spreadsheet.credentials]`

```python
[sources.google_analytics.credentials]
project_id = "set me up" # GCP Source project ID!
private_key = "set me up" # Unique private key !(Must be copied fully including BEGIN and END PRIVATE KEY)
client_email = "set me up" # Email for source service account
location = "set me up" #Project Location For ex. “US”
```

3. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/destinations#google-bigquery)

## Add Property_ID and queries

1. `property_id` is a unique number that identifies a particular property. As below “GA4- Google Merch Shop” is the name of the property and “213025502” is the `property_id`
    
    <img src="docs_images/GA4_Property_ID.png" alt="Admin Centre" width = "50%" />
    
2. Note the `property_id`of the property you want to get the data from google analytics.
3. You can set the metrics you want to get data for by defining them in `queries`
4. Below is an example, `config.toml` of setting the `property_id` and `queries`

```bash
[sources.google_analytics]
property_id = "299059933" 
queries = [
    {"resource_name"= "sample_analytics_data1", "dimensions"= ["browser", "city"], "metrics"= ["totalUsers", "transactions"]},
    {"resource_name"= "sample_analytics_data2", "dimensions"= ["browser", "city", "dateHour"], "metrics"= ["totalUsers"]}
]
```

5. `property_id` and `queries` need to be set in config.toml. 

## **Run the pipeline**

1. Install the requirements by using the following command

`pip install -r requirements.txt`

2. In the  main method of `google_analytics_pipeline.py`, run the following function `simple_load_config()` as follows:

```python
if __name__ == "__main__":
    start_time = time.time()
    simple_load_config()       # This function runs !
    end_time = time.time()
    print(f"Time taken: {end_time-start_time}")
```

3. Run the pipeline by using the following command

`python3 google_analytics_pipelines.py`

4. To make sure that everything is loaded as expected, use the command:

`dlt pipeline <pipeline_name> show` (For example, the pipeline_name for the above pipeline is `dlt_google_analytics_pipeline`, you may also use any custom name instead)

## Customize **the pipeline**

This pipeline has some predefined methods that you can use; or you can also define your own methods to run the pipeline. The predefined methods are:
<details>
<summary>simple_load() method</summary>
1. If you don’t want to define the `property_id` and `queries` in the `config.toml` you can define them in the `google_analytics_pipeline.py` as defined below:
    
 ```python
    queries = [
        {"resource_name": "sample_analytics_data1", "dimensions": ["browser", "city"], "metrics": ["totalUsers", "transactions"]},
        {"resource_name": "sample_analytics_data2", "dimensions": ["browser", "city", "dateHour"], "metrics": ["totalUsers"]}
    ] # Define the queries as these are defined
    
    def simple_load():
        """
        Just loads the data normally. Incremental loading for this pipeline is on, the last load time is saved in dlt_state and the next load of the pipeline will have the last load as a starting date.
        :returns: Load info on the pipeline that has been run
        """
        # FULL PIPELINE RUN
        pipeline = dlt.pipeline(pipeline_name="dlt_google_analytics_pipeline", destination='bigquery', full_refresh=False, dataset_name="sample_analytics_data")
        # Google Analytics source function - taking data from queries defined locally instead of config
        # TODO: pass your google analytics property id'
        data_analytics = google_analytics(property_id="12345678", queries=queries) # Here property_id is defined
        info = pipeline.run(data=data_analytics)
        print(info)
        return info
 ```
    
2. In the main method of `google_analytics_pipeline.py`, run the following function `simple_load().`
3. And Run the pipeline as above.
</details>
<details>
<summary>chose_date_first_load() method </summary>
1. In this method you can choose the starting date for which you want to load the data.
2. This method will take `property_id` and `queries` from `config.toml`
3. You can define the start date from which you want to load the data for your website/app as follows:
    
    ```python
    def chose_date_first_load(start_date: str = "2000-01-01"):
    ```
    
4. The `start_date` for the above method is “2000-01-01”, you can set this as required.
5. In the main method of `google_analytics_pipeline.py`, run the following function `chose_date_first_load()`
6. And Run the pipeline as above.
</details>
`The incremental loading for these pipelines is on which means the last load time is saved in dlt_state and the next load of the pipeline will have the last load as a starting date.`
