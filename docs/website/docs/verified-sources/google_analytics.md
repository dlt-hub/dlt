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

1. Open `.dlt/secrets.toml`.
2. From the .json that you downloaded earlier, copy `project_id`, `private_key`, and `client_email` under `[sources.google_spreadsheet.credentials]`.

    ```python
    [sources.google_analytics.credentials]
    project_id = "set me up" # GCP Source project ID!
    private_key = "set me up" # Unique private key !(Must be copied fully including BEGIN and END PRIVATE KEY)
    client_email = "set me up" # Email for source service account
    location = "set me up" #Project Location For ex. “US”
    ```
3. Alternatively, if you're using service account credentials, replace the the fields and values with those present in the credentials .json that you generated above.
4. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/destinations#google-bigquery).

## Pass property_id and request parameters

1. `property_id` is a unique number that identifies a particular property. You will need to explicity pass it to get data from the property that you're interested in. For example, if the property that you want to get data from is “GA4- Google Merch Shop” then you will need to pass its propery id 213025502.

    <img src="https://raw.githubusercontent.com/dlt-hub/dlt/devel/docs/website/docs/pipelines/docs_images/GA4_Property_ID.png" alt="Admin Centre" width = "50%" />

2. You can also specify the parameters of the API requests such as dimensions and metrics to get your desired data.
3. An example of how you can pass all of this in `dlt` is to simply insert it in the `.dlt/config.toml` file as below:

    ```bash
    [sources.google_analytics]
    property_id = "299059933" 
    queries = [
        {"resource_name"= "sample_analytics_data1", "dimensions"= ["browser", "city"], "metrics"= ["totalUsers", "transactions"]},
        {"resource_name"= "sample_analytics_data2", "dimensions"= ["browser", "city", "dateHour"], "metrics"= ["totalUsers"]}
    ]
    ```
    In this example, we pass our request parameters inside a list called `queries`. The data from each request will be loaded onto a table having the table name specified by the parameter `resource_name`.
4. If you're adding the property_id and queries to `.dlt/config.toml`, then you will need to modify the script `google_analytics_pipeline.py`.
5. In the main method of the script `google_analytics_pipeline.py`, replace the function `simple_load()` with the function `simple_load_config()`. This function will automatically read the property_id and queries from `.dlt/config.toml`.
    ```python
    if __name__ == "__main__":
        start_time = time.time()
        simple_load_config()       # Insert the function here
        end_time = time.time()
        print(f"Time taken: {end_time-start_time}")
    ```

## Run the pipeline

1. Install the requirements by using the following command

    `pip install -r requirements.txt`

2. Run the pipeline by using the following command

    `python3 google_analytics_pipelines.py`

3. Make sure that everything is loaded as expected, by using the command:

    `dlt pipeline <pipeline_name> show`   

    For example, the pipeline_name for the above pipeline is `dlt_google_analytics_pipeline`  
    To change this, you can replace it in  
    `dlt.pipeline(pipeline_name="dlt_google_analytics_pipeline", ... )`

## Customize the pipeline

This pipeline has some predefined methods that you can use; or you can also define your own methods to run the pipeline. The predefined methods are:  

- **Incremental Loading**: The incremental loading for these pipelines is on which means the last load time is saved in dlt_state and the next load of the pipeline will have the last load as a starting date.

- **simple_load()**
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
                data_analytics = google_analytics(property_id="12345678", queries=queries) # Pass the property_id and queries here
                info = pipeline.run(data=data_analytics)
                print(info)
                return info
        ```
        
    2. Include the function `simple_load()` in the main method in `google_analytics_pipeline.py`
    3. Run the pipeline as above.
- **chose_date_first_load()**

    1. With this method, you can choose the starting date from when you want to load the data.
    2. This method will take `property_id` and `queries` from `config.toml`
    3. By default, the start date is "2000-01-01"
    4. To specify a different start date, pass it as a string in yyyy-mm-dd format when calling the function in the main method:  
    `chose_date_first_load(start_date="yyyy-mm-dd")`


