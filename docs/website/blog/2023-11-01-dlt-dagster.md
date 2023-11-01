---
slug: dlt-dagster
title: "Orchestrating unstructured data pipeline with dagster and dlt."
image: https://d1ice69yfovmhk.cloudfront.net/images/dlt-dagster_overview.jpg
authors:
    name: Zaeem Athar
    title: Junior Data Engineer
    url: https://github.com/zem360
    image_url: https://images.ctfassets.net/c4lg2g5jju60/5tZn4cCBIesUYid17g226X/a044d2d471ebd466db32f7868d5c0cc8/Zaeem.jpg?w=400&h=400&q=50&fm=webp
tags: [Dagster, dlt, Asset Factory, Unstructured Data]
---
:::info
TL;DR: In this blog post, we'll build data piplines using dlt and orchestrate them using Dagster.
:::

`dlt` is an open-source Python library that allows you to declaratively load messy data sources into well-structured tables or datasets, through automatic schema inference and evolution. It simplifies building data pipelines by providing functionality to support the entire extract and load process.

It does so in a scalable way, enabling you to run it on both micro workers or in highly parallelized setups. dlt also offers robustness on extraction by providing state management for incremental extraction, drop-in requests replacement with retries, and many other helpers for common and uncommon extraction cases.

To start with `dlt`, you can install it using pip: `pip install dlt`. Afterward, import `dlt` in your Python script and start building your data pipeline. There's no need to start any backends or containers. 

## **Project Overview:**

In this example, we will ingest GitHub issue data from a repository and store the data in BigQuery. We will use `dlt` to create a data pipeline and orchestrate it using dagster. 

Initially, we will start by creating a simple data pipeline using `dlt`.  We will then orchestrate the pipeline using dagster. Finally, we will add more features to this pipeline by using the dlt schema evolution and dagster asset metadata to educate the users about their data pipeline.

The project code is available on [GitHub](https://github.com/dlt-hub/dlt-dagster-demo/tree/main).

![Project Overview](https://d1ice69yfovmhk.cloudfront.net/images/dlt-dagster_overview.jpg)

As we will be ingesting data into BigQuery we first need to create service account credentials for BigQuery. You can find more info on setting up a service account in the `dlt` [docs](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery).

Once we have the credentials we are ready to begin. Let’s first install dagster and `dlt`. The below commands should install both.

```python
pip install dlt
pip install dagster dagster-webserver
```

## **Simple `dlt` Pipeline:**

As a first step, we will create the GitHub issues pipeline using dlt. 

```bash
dlt init github_issues bigquery
```

This will generate a template for us to create a new pipeline. Under `.dlt/secrets.toml` add the service account credentials for BigQuery. Then in the `github_issues.py` delete the generated code and add the following:

```python
@dlt.resource(write_disposition="append")
def github_issues_resource(api_secret_key=dlt.secrets.value):
    owner = 'dlt-hub'
    repo = 'dlt'
    url = f"https://api.github.com/repos/{owner}/{repo}/issues"
    headers = {"Accept": "application/vnd.github.raw+json"}

    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # raise exception if invalid response
        issues = response.json()
        yield issues

        if 'link' in response.headers:
            if 'rel="next"' not in response.headers['link']:
                break

            url = response.links['next']['url']  # fetch next page of stargazers
        else:
            break
        time.sleep(2)  # sleep for 2 seconds to respect rate limits

if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='github_issues', destination='bigquery', dataset_name='github_issues_data'
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(github_issues_resource())

    #print the information on data that was loaded
    print(load_info)
```

The above code creates a simple github_issues pipeline that gets the issues data from the defined repository and loads it into BigQuery. The `dlt.resources` yields the data while the `dlt.pipeline` normalizes the nested data and loads it into the defined destination. To read more about the technical details refer to the `dlt` [docs](https://dlthub.com/docs/intro).

To run the pipeline execute the below commands:

```bash
pip install -r requirements.txt
python github_issues.py
```

We now have a running pipeline and are ready to orchestrate it using dagster.

## **Orchestrating using dagster:**

We will need to adjust our pipeline a bit to orchestrate it using dagster. 

### Step 1: Create a dagster project

- Create a new directory for your Dagster project and scaffold the basic structure:

```bash
mkdir dagster_github_issues
cd dagster_github_issues
dagster project scaffold --name github-issues
```

This will generate the default files for dagster that we will use as a starting point for our data pipeline. 

### Step 2: Set up the directory Structure

- Inside the `github-issues/github_issues` directory create the following folders: `assets`, `resources`, and `dlt`.

```bash
.
├── README.md
├── github_issues
│   ├── __init__.py
│   ├── assets
│   │   ├── __init__.py
│   ├── dlt
│   │   ├── __init__.py
│   └── resources
│       ├── __init__.py
├── github_issues_tests
│   ├── __init__.py
│   └── test_assets.py
├── pyproject.toml
├── setup.cfg
└── setup.py
```

### Step 3: Add dlt Resources and environment variables

- Copy the previously created **`github_issues_resource`** code into **`dlt/__init__.py`** under the **`dlt`** folder. Remove the **`dlt.secrets.value`** parameter, as we'll pass the credentials through a **`.env`** file.
- Create a `.env` file in the root directory. This is the directory where the `pyproject.toml` file exits. Copy the credentials into the `.env` and follow the correct naming convention. For more info on setting up the `.env` file have a look at the [docs](https://dlthub.com/docs/general-usage/credentials#reading-credentials-from-environment-variables).

### Step 4: Add configurable resources and define the asset

- Define a **`DltPipeline`** class in **`resources/__init__.py`** as a Dagster configurable resource. This class allows you to reuse pipeline code inside an asset.

```python
from dagster import ConfigurableResource 
import dlt

class DltPipeline(ConfigurableResource):
    def create_pipeline(self, pipeline_name ,dataset_name, destination, dlt_resource, table_name):

        # configure the pipeline with your destination details
        pipeline = dlt.pipeline(
        pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name
        )
        # run the pipeline with your parameters
        load_info = pipeline.run(dlt_resource, table_name=table_name)

        return load_info
```

- Define the asset, **`issues_pipeline`**, in **`assets/__init__.py`**. This asset uses the configurable resource to create a Dlt pipeline and ingests data into BigQuery.

```python
from dagster import asset, get_dagster_logger
from ..resources import DltPipeline
from ..dlt import github_issues_resource

@asset
def issues_pipeline(pipeline: DltPipeline):
    pipeline_name= 'github_issues'
    dataset= 'dagster_github_issues'
    destination= 'bigquery'
    table= 'github_issues'

    logger = get_dagster_logger()
    results = pipeline.create_pipeline(pipeline_name, dataset, destination, github_issues_resource, table)
    logger.info(results)
```

The defined asset (**issues_pipeline**) takes as input the configurable resource (**DltPipeline**). In the asset, we use the configurable resource to create a dlt pipeline by using an instance of the configurable resource (**DltPipeline**) to call the `create_pipeline`  function. The `dlt.resource` (**github_issues_resource**) is passed to the `create_pipeline` function. The `create_pipeline` function normalizes the data and ingests it into BigQuery.

### Step 5: Handle Schema Evolution

`dlt` provides the feature of schema evolution that monitors changes in the defined table schema. Suppose GitHub adds a new column or changes a datatype of a column this small change can break pipelines and transformations. The schema evolution feature works amazingly well with dagster. 

- Add the schema evolution code to the asset to make our pipelines more resilient to changes.

```python
from dagster import AssetExecutionContext
@asset
def issues_pipeline(context: AssetExecutionContext, pipeline: DltPipeline):
...
md_content=""
    for package in result.load_packages:
        for table_name, table in package.schema_update.items():
            for column_name, column in table["columns"].items():
                md_content= f"\tTable updated: {table_name}: Column changed: {column_name}: {column['data_type']}"

    # Attach the Markdown content as metadata to the asset
    context.add_output_metadata(metadata={"Updates": MetadataValue.md(md_content)})
```

### Step 6: Define Definitions

- In the `__init.py__` under the **github_issues** folder add the definitions:

```python
all_assets = load_assets_from_modules([assets])
simple_pipeline = define_asset_job(name="simple_pipeline", selection= ['issues_pipeline'])

defs = Definitions(
    assets=all_assets,
    jobs=[simple_pipeline],
    resources={
        "pipeline": DltPipeline(),
    }
)
```

### Step 7: Run the Web Server and Materialize the asset

- In the root directory (**github-issues)** run the `dagster dev` command to run the web server and materialize the asset.

![GitHub Asset](https://d1ice69yfovmhk.cloudfront.net/images/dlt-dagster_asset.png)

### Step 8: View the populated Metadata and data in BigQuery

Once the asset has been successfully materialized go to the Assets tab from the top and select the **Issues_pipeline**. In the Metadata you can see the Tables, Columns, and Data Types that have been updated. In this case, the changes are related to internal `dlt` tables. 

Any subsequent changes in the GitHub issues schema can be tracked from the metadata. You can set up [Slack notifications](https://dlthub.com/docs/running-in-production/running#using-slack-to-send-messages) to be alerted to schema changes.

![Meatadata loaded in Asset](https://d1ice69yfovmhk.cloudfront.net/images/dlt-dagster_metadata.png)

Let's finally have a look in BigQuery to view the ingested data.

![Data Loaded in Bigquery](https://d1ice69yfovmhk.cloudfront.net/images/dlt-dagster_bigquery_data.png)

The **github_issues** is the parent table that contains the data from the root level of the JSON returned by the GitHub API. The subsequent table **github_issues_assignees** is a child table that was nested in the original JSON. `dlt` normalizes nested data by populating them in separate tables and creates relationships between the tables. To learn more about how dlt created these relationships refer to the [docs](https://dlthub.com/docs/general-usage/destination-tables#child-and-parent-tables).

## **Orchestrating verified `dlt` source using dagster:**

`dlt` provides a list of verified sources that can be initialized to fast-track the pipeline-building process. You can find a list of sources provided in the `dlt` [docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/).

One of the main strengths of `dlt` lies in its ability to extract, normalize, and ingest unstructured and semi-structured data from various sources. One of the most commonly used verified source is MongoDB. Let’s quickly look at how we can orchestrate MongoDB source using dagster.

### Step 1: Setting up a dagster project:

- Start by creating a new Dagster project scaffold:

```python
dagster project scaffold --name mongodb-dlt
```

- Follow the steps mentioned earlier and create an **`assets`** directory under **`mongodb-dlt/mongodb_dlt`**.
- Initialize a **`dlt`** MongoDB pipeline in the same directory:

```python
dlt init mongodb bigquery
```

After running the command your directory structure should be as follows:

```python
.
├── README.md
├── mongodb_dlt
│   ├── __init__.py
│   ├── assets
│   │   ├── __init__.py
│   │   └── assets.py
│   ├── mongodb
│   │   ├── README.md
│   │   ├── __init__.py
│   │   └── helpers.py
│   ├── mongodb_pipeline.py
│   └── requirements.txt
├── mongodb_dlt_tests
│   ├── __init__.py
│   └── test_assets.py
├── pyproject.toml
├── setup.cfg
├── setup.py
```

### Step 2: Configuring MongoDB Atlas and Credentials

For this example, we are using MongoDB Atlas. Set up the account for MongoDB Atlas and use the test [Movie Flix Dataset](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix/). You can find detailed information on setting up the credentials in the MongoDB verified sources [documentation](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongodb).

Next, create a `.env` file and add the BigQuery and MongoDB credentials to the file. The `.env` file should reside in the root directory.

### Step 3: Defining an Asset Factory

The structure of data in MongoDB is such that under each database you will find multiple collections. When writing a data pipeline it is important to separate the data loading for each collection. 

Dagster provides the feature of `**@multi_asset`** declaration that will allow us to convert each collection under a database into a separate asset. This will make our pipeline easy to debug in case of failure and the collections independent of each other.

In the **`mongodb_pipeline.py`** file, locate the `**load_select_collection_hint_db**` function. We will use this function to create the asset factory. 

 In the **`__init__.py`** file under the **assets** directory, define the **`dlt_asset_factory`**:

```python
url = os.getenv('SOURCES__MONGODB__CONNECTION__URL')

database_collection ={
    "sample_mflix":[
        "comments",
        "embedded_movies"
    ],
}

def dlt_asset_factory(collection_list):
    multi_assets = []
    jobs = []

    for db, collection_name in database_collection.items():
        @multi_asset(
            name=db,
            group_name=db,
            outs={
            stream: AssetOut(key_prefix=[f'raw_{db}'])
            for stream in collection_name}

        )
        def load_select_collection(context: OpExecutionContext):
            """Use the dlt mongodb source to reflect an entire database schema and load select tables from it.

            This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
            """

            pipeline = dlt.pipeline(
                pipeline_name="local_mongo",
                destination='bigquery',
                dataset_name="mongo_select_hint",
            )

            logger = get_dagster_logger()
            data = mongodb(url, db).with_resources(*collection_name)

            load_info = pipeline.run(data, write_disposition="replace")
						return tuple([None for _ in context.selected_output_names])

        multi_assets.append(load_select_collection)

        asset_job = define_asset_job(f"{db}_assets", AssetSelection.groups(db))

        jobs.append(asset_job)

        return multi_assets, jobs

dlt_assets, dlt_jobs = dlt_asset_factory(database_collection)
```

### Step 4: Running the Dagster Web Server

We can run the `dagster dev` command to start the web server. We can see that each collection is converted into a separate asset by Dagster. We can materialize our assets to ingest the data into BigQuery.

![Asset Factory](https://d1ice69yfovmhk.cloudfront.net/images/dlt-dagster_asset_factory.png)

The resulting data in BigQuery:

![Data Ingestion in BigQuery from MongoDB](https://d1ice69yfovmhk.cloudfront.net/images/dlt-dagster_mongo_bigquery.png)

## **Conclusion:**

In this demo, we looked at how to orchestrate dlt pipelines using dagster. We started off by creating a simple dlt pipeline and then converted the pipeline into an asset and resource before orchestrating. 

We also looked at how we can orchestrate dlt MongoDB verified sources using Dagster. We utilized the Dagster `**@multi_asset`** feature to create a **`dlt_asset_factory` which** converts each collection under a database to a separate asset allowing us to create more robust data pipelines.

Both `dlt` and dagster can be easily run on local machines. By combining the two we can build data pipelines at great speed and rigorously test them before shipping to production.