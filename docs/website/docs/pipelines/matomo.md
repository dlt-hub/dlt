# Matomo
Matomo is a free and open-source web analytics platform that allows website owners and businesses to get detailed insights into their website and application performance.Matomo provides a comprehensive range of features including visitor maps, site search analytics, real-time visitor tracking, and custom reports, among others. 

With the DLT pipeline, you can effortlessly extract and seamlessly load data from Matomo to your preferred destination. This incredible process covers a wide range of options, including loading visitor data, live events, and reports, which can yield you with insights into your website's performance.

## Grab Matomo credentials

### Grab API access token

1. Firstly, log in to your Matomo account.
2. Once you're logged in, click on the Administration icon (which looks like a gear) in the top right corner of the webpage.
3. From there, go to the "Personal" section, and then click on "Security" in the left side menu bar.
4. In the "Security" section, find the "Auth Tokens" option and click on "Create a New Token."
5. You'll be prompted to enter your account password for verification purposes.
6. After entering your password, provide a meaningful description for the token you're creating in the "Description" field.
7. Click on "Create New Token."
8. Your Access token will be created and displayed on the screen.
9. Make sure to keep this token safe as it will be used to configure secrets.toml in the DLT pipeline.

### Grab `URL` and `site_id`

1. The URL refers to the web address you see in the browser's address bar when you open your Matomo account. 

For instance, if your company's name is "mycompany", then the URL will be something like "**[https://mycompany.matomo.cloud/](https://mycompany.matomo.cloud/)**".

2. Please make sure to note down this URL as it will be needed for configuring the **`config.toml`**.
3. The **`site_id`** is a unique identifier assigned to each site that you monitor using your Matomo account.
4. You can easily locate the **`site_id`** in the URL link in the address bar, for example, as "idSite=2".
5. Alternatively, you can also find the **`site_id`** by navigating to the Administration page > Measureables > Manage, and then locating the **`ID`** number for your site.

## Initialize the pipeline[](https://dlthub.com/docs/pipelines/github#initialize-the-pipeline)

To get started with your data pipeline, follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:

```bash

dlt init matomo bigquery
```

This command will initialize your pipeline with Matomo as the source and BigQuery as the destination. If you'd like to use a different destination, simply replace **`bigquery`** with the name of your preferred destination. You can find supported destinations and their configuration options in our [documentation](https://dlthub.com/docs/destinations/duckdb) 

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started. From here, you can begin configuring your pipeline to suit your specific needs.

```bash
matomo_pipeline
├── .dlt
│   ├── .pipelines
│   ├── config.toml
│   └── secrets.toml
├── matomo
│   └── __pycache__
│   └── helpers
│   └── __init__.py
├── .gitignore
├── matomo_pipeline.py
└── requirements.txt
```

## **Add credential**

1. Inside the **`.dlt`** folder, you'll find a file called **`secrets.toml`**, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

Here's what the file looks like:

```

[sources.matomo]
api_token= "access_token" # please set me up!

[destination.bigquery.credentials]
project_id = "project_id" # GCP project ID
private_key = "private_key" # Unique private key (including `BEGIN and END PRIVATE KEY`)
client_email = "client_email" # Service account email
location = "US" # Project location (e.g. “US”)
```

2. Replace the value of **`api_token`** with the one that [you copied above](matomo.md#grab-api-access-token). This will ensure that your data pipeline can access your Matomo resources securely.
3. Next, follow the instructions in **[Destinations](https://dlthub.com/docs/destinations/duckdb)** to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.
4. Inside the **`.dlt`** folder, you'll find a file called **`config.toml`**, where you can securely store your pipeline configuration details. 

Here's what the config.toml looks like:

```python
[sources.matomo]
url = "Please set me up !" # please set me up!
queries = ["a", "b", "c"] # please set me up!
site_id = 0 # please set me up!
live_events_site_id = 0 # please set me up!
```

5. Replace the value of `url` and `site_id`  with the one that [you copied above](matomo.md#grab-url-,-site_id-and-live-event-site-id). This will ensure that your data pipeline can access required Matomo resources .
6. In order to track live events for a website, the **`live_event_site_id`** parameter must be set to the same value as the **`site_id`** parameter for that website.

## **Specify source methods**

1. You can select which data you would load, be it from reports, live events or custom reports. 
2. The `matomo_pipeline.py` has four source methods, which are all preconfigured for your use:
    
    
    | S.No | Method | Description |
    | --- | --- | --- |
    | a | run_reports() | Loads all the reports available in Matomo. |
    | b | run_live_events() | Loads visitors and visits data, specifically for the current day (can be customized). |
    | c | run_full_load() | Loads data from both reports and live events. |
    | d | run_custom_reports() | Loads data from custom reports. |
3. To specify the method you want to use in the main function, for example if you want to use run_full_load(), your main method should look like:

```python
if __name__ == "__main__":
    run_full_load()

```

## Run the pipeline[](https://dlthub.com/docs/pipelines/strapi#run-the-pipeline)

1. Install the necessary dependencies by running the following command:

`pip install -r requirements.txt`

2. Now the pipeline can be run by using the command:

`python3 matomo_pipeline.py`

3. To make sure that everything is loaded as expected, use the command:

`dlt pipeline <pipeline_name> show` (For example, the pipeline_name for the above pipeline is `Matomo`, you may also use any custom name instead)

## Customizations[](https://dlthub.com/docs/pipelines/strapi#run-the-pipeline)

If you want to load custom reports created in Matomo into your pipeline, you can use the **`run_custom_reports()`** method provided in the pipeline.

Before using this method, make sure you have created a custom report in Matomo by following the instructions provided in the **[documentation here](https://matomo.org/guide/reporting-tools/custom-reports/)**.

The **`run_custom_reports()`** method looks like this:

```python
pythonCopy code
def run_custom_reports():
    """
    Defines some custom reports you can use and shows how to use for different custom reports
    :return:
    """

    queries = [
        {"resource_name": "custom_report_name",
         "methods": ["CustomReports.getCustomReport"],
         "date": "2020-01-01",
         "period": "day",
         "extra_params": {"idCustomReport": 1}},
        {"resource_name": "custom_report_name2",
         "methods": ["CustomReports.getCustomReport"],
         "date": "2020-01-01",
         "period": "day",
         "extra_params": {"idCustomReport": 2}},
    ]
    site_id = 1
    pipeline_reports = dlt.pipeline(dataset_name="matomo_custom_reports_custom", full_refresh=False, destination='bigquery', pipeline_name="matomo")
    data = matomo_reports(queries=queries, site_id=site_id)
    info = pipeline_reports.run(data)
    print(info)

```

To use this method, you need to make the following changes to the queries:

1. Replace the **`custom_report_name`** with the name of the custom report that you created in Matomo.
2. Next, you need to enter the date for which you want to load the custom report in the format of yyyy-mm-dd under **`date`**.
3. Moving on, the **`idCustomReport`** can be found in the custom report URL. For example, if you click on "Create New Report" or "Edit Report", then in the URL in Address bar. You will find a value corresponding to **`idCustomReport=3`**, then `3` is the value used for `idCustomReport`
4. Lastly, remember to only use the appropriate number of queries for which you want the results to be loaded. Here's an example for you:

```python
pythonCopy code
queries = [
        {"resource_name": "my_report",
         "methods": ["CustomReports.getCustomReport"],
         "date": "2023-01-01",
         "period": "day",
         "extra_params": {"idCustomReport": 3}},

    ]
```

You can modify the custom reports and queries as per your preferences. Finally, when you're ready to run the pipeline, make sure to use the **`run_custom_reports()`** method in the main function. 

That's it! Enjoy running your Matomo DLT pipeline!