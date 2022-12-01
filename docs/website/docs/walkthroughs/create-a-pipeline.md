---
sidebar_position: 1
---

# Create a pipeline

Follow the steps below to create a [pipeline](./glossary.md/#pipeline) from the Twitter API to 
Google BigQuery from scratch. The same steps can be repeated for any source and destination of your 
choice--use `dlt init <source> <destination>` and then build the pipeline for that API instead.

Please make sure you have [installed `dlt`](./installation.mdx) before getting started here.

## 1. Initialize project

Create a new empty directory for your `dlt` project by running
```
mkdir twitter-bigquery
```

Start a `dlt` project with a pipeline template that loads data to Google BigQuery by running
```
dlt init twitter bigquery
```

Install the dependencies necessary for Google BigQuery:
```
pip install -r requirements.txt
```

## 2. Add Google BigQuery credentials

You can follow the steps in [Set up Google BigQuery](../getting-started.md#2-set-up-google-bigquery)
to create the service account credentials you'll need for BigQuery and add them to `.dlt/secrets.toml`.

## 3. Add Twitter API credentials

You will need to [sign up for the Twitter API](https://developer.twitter.com/en/docs/platform-overview)
and create a project to grab your Bearer Token for the Twitter API.

Copy the value of the bearer token into `.dlt/secrets.toml`
```
[sources]

twitter_bearer_token = '<bearer token value>'

[destination.bigquery.credentials]

# ...
```

Run the `twitter.py` pipeline script to test that authentication headers look fine
```
python3 twitter.py
```

Your bearer token should be printed out to stdout.

## 4. Request data from Twitter API search endpoint

Uncomment `print(data)` in the `twitter.py` pipeline script main:
```
if __name__=='__main__':
    # print credentials by running the resource
    data = list(twitter_resource())

    # print responses from resource
    print(data)

    # ...
```

Replace the `twitter_resource` function definition in the `twitter.py` pipeline script with a call to the 
[Twitter API search endpoint](https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-recent)
```
@dlt.resource(write_disposition="append")
def twitter_resource(api_url=dlt.config.value, twitter_bearer_token=dlt.secrets.value):
    
    headers = _headers(twitter_bearer_token)

    search_term = 'data engineer'
    
    params = {
                'query': search_term,
                'max_results': 100,
                'tweet.fields': 'id,text,author_id,created_at',
             }

    url = "https://api.twitter.com/2/tweets/search/recent"

    response = requests.get(url, headers=headers, params=params)
    
    response.raise_for_status()

    yield response.json()
```

Run the `twitter.py` pipeline script to test that the API call works
```
python3 twitter.py
```

This should print out the tweets that you requested.

## 5. Load the data

Uncomment the `pipeline` and `load_info` defintions as well as `print(load_info)` 
in the `twitter.py` pipeline script main:
```
if __name__=='__main__':
    # print credentials by running the resource
    # data = list(twitter_resource())

    # print responses from resource
    # print(data)

    # run pipeline
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name="twitter", destination="bigquery", dataset_name="twitter_data")
    # run the pipeline with your parameters
    load_info = pipeline.run(twitter(dlt.config.value, dlt.secrets.value, last_id=819273998))

    # pretty print the information on data that was loaded
    print(load_info)
```

Run the `twitter.py` pipeline script to load data to BigQuery
```
python3 twitter.py
```

Go to the [Google BigQuery](https://console.cloud.google.com/bigquery) console and view the tables 
that have been loaded.

## 7. Next steps

Now that you have a working pipeline, you have options for what to learn next:
- Add a function to this pipeline to handle the `next_token` pagination from the Twitter API
- [Deploy this pipeline](./walkthroughs/deploy-a-pipeline), so that the data is automatically 
loaded on a schedule
- Transform the [loaded data](./using-loaded-data/transforming-the-data) with dbt or in Pandas DataFrames
- Set up a [pipeline in production](./running-in-production/scheduling) with scheduling, 
monitoring, and alerting
- Try loading data to a [different destination](./destinations) like Amazon Redshift or Postgres