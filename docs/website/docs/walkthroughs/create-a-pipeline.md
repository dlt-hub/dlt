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
and create a

```
[credentials]
bearer_token = '**********'
```



- Uncomment out the first `@resource` function call
- Add credentials
- Check by printing them out in `@resource` function (via `_headers` helper method)

## 4. Grab data from Twitter API search endpoint

- Make the API call
- print the response
- ensure it is the data you want

## 5. 

## 6. Load the data

You can run the pipeline by running
```
python3 twitter.py
```

## Step 6

Add more resources (following same steps as before)