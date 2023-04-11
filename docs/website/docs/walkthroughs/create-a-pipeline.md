# Create a pipeline

Follow the steps below to create a [pipeline](../general-usage/glossary.md#pipeline) from the Twitter API to
Google BigQuery from scratch. The same steps can be repeated for any source and destination of your
choice--use `dlt init <source> <destination>` and then build the pipeline for that API instead.

Please make sure you have [installed `dlt`](../installation.mdx) before following the steps below.

## 1. Initialize project

Create a new empty directory for your `dlt` project by running
```
mkdir twitter_bigquery
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

Follow steps 3-7 under [Google BigQuery](../destinations/bigquery.md) to create the
service account credentials you'll need for BigQuery and add them to `.dlt/secrets.toml`.

## 3. Add Twitter API credentials

You will need to [sign up for the Twitter API](https://developer.twitter.com/en/docs/platform-overview)
and create a project to grab your Bearer Token for the Twitter API.

Copy the value of the bearer token into `.dlt/secrets.toml`
```
[sources]

api_secret_key = '<bearer token value>'

[destination.bigquery.credentials]

# ...
```

The secret name must correspond to the argument name in the source (i.e. `api_secret_key=dlt.secrets.value`
in `def twitter_source(api_secret_key=dlt.secrets.value):`).

Run the `twitter.py` pipeline script to test that authentication headers look fine
```
python3 twitter.py
```

Your bearer token should be printed out to stdout along with some test data.

## 4. Request data from Twitter API search endpoint

Replace the `twitter_resource` function definition in the `twitter.py` pipeline script with a call to the
[Twitter API search endpoint](https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-recent)
```
@dlt.resource(write_disposition="append")
def twitter_resource(api_secret_key=dlt.secrets.value):

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

Remove the `exit()` call from the `main` function in `twitter.py`, so that running the
`python3 twitter.py` command will now also run the pipeline:
```
if __name__=='__main__':

    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='twitter', destination='bigquery', dataset_name='twitter_data')

    # print credentials by running the resource
    data = list(twitter_resource())

    # print the data yielded from resource
    print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(twitter_source())

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
- Try loading data to a different destination like [Google BigQuery](../destinations/bigquery.md), [Amazon Redshift](../destinations/redshift.md), or [Postgres](../destinations/postgres.md)