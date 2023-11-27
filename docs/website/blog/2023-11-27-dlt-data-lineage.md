---
slug: dlt-data-lineage
title: "Data Lineage using dlt and dbt."
image: https://d1ice69yfovmhk.cloudfront.net/images/data_lineage_overview.jpeg
authors:
    name: Zaeem Athar
    title: Junior Data Engineer
    url: https://github.com/zem360
    image_url: https://images.ctfassets.net/c4lg2g5jju60/5tZn4cCBIesUYid17g226X/a044d2d471ebd466db32f7868d5c0cc8/Zaeem.jpg?w=400&h=400&q=50&fm=webp
tags: [dlt, dbt, Data Lineage]
---
:::info
TL;DR: In this blog post, we'll create a data lineage view for our ingested data by utlizing the `dlt` load_info.
:::

## Why data lineage is important ?

Data lineage is an important tool in an arsenal of a data engineer. It showcases the journey of data from its source to its destination. It captures all the pitstops made and can help identify issues in the data pipelines by offering a birds eye view of the data.

As data engineers, data lineage enables us to trace and troubleshoot the datapoints we offer to our stakeholders. It is also an important tool that can be used to meet regulation regarding privacy. Moreover, it can help us evaluate how any changes upstream in a pipeline effects the  downstream source. There are many types of data lineage, the most commonly used types are the following:

- Table lineage shows us the raw data sources that are used to form a new table. Table lineage tracks the flow of data, showing how data moves forward through various processes and transformations.
- Row lineage reveals the data flow at a more granular level. It refers to tracking and understanding of individual rows of data as they move through various stages in a data processing pipeline. It is a subset of table lineage that focuses specifically on the journey of individual records or rows rather than the entire dataset.
- Column lineage specifically focuses on tracking and documenting the flow and transformation of individual columns or fields within different tables and views in the data.


## Project Overview:

In this demo, we will showcase how you can leverage the `dlt` pipeline **[load_info](https://dlthub.com/docs/running-in-production/running#inspect-and-save-the-load-info-and-trace)** to create table, row and column lineage for your data. The code for the demo is available on [GitHub](https://github.com/dlt-hub/demo-data-lineage).

The `dlt` load_info encapsulates useful information pertaining the loaded data. It contains the pipeline, dataset name, the destination information and list of loaded packages among other elements. Within the load_info packages, you will find a list of all tables and columns created at the destination during loading of the data. It can be used to display all the schema changes that occur during data ingestion and implement data lineage.

We will work with the example of a skate shop that runs an online shop using Shopify, in addition to its physical stores. The data from both sources is extracted using `dlt` and loaded into BigQuery.

![Data Lineage Overview](https://d1ice69yfovmhk.cloudfront.net/images/data_lineage_overview.jpeg)

In order to run analytics workloads, we will create a transformed **fact_sales** table using dbt and the extracted raw data. The fact_sales table can be used to answer all the sales related queries for the business. 

The load_info produced by `dlt` for both pipelines is also populated into BigQuery. We will use this information to create a Dashboard in Metabase that shows the data lineage for the fact_sales table.

## Implementing Data Lineage:

To get started install `dlt` and dbt:

```jsx
pip install dlt
pip install dbt-bigquery
```

As we will be ingesting data into BigQuery we first need to create service account credentials for BigQuery. You can find more info on setting up a service account in the `dlt` [docs](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery).

We will be using CSV files as our data sources for this demo. We are using some test Shopify data and [Kaggle Supermarket](https://www.kaggle.com/datasets/aungpyaeap/supermarket-sales) dataset as our sources.

 `dlt` provides [verified Shopify source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/shopify) to directly extract data from the Shopify API.

### Step 1: Initialize a dlt pipeline

To get started we initialize a dlt pipeline and selecting BigQuery as our destination by running the following command:

```python
dlt init data_lineage bigquery
```

This will create default scaffolding to build our pipeline. Install the dependencies by running the following command:

```python
pip install -r requirements.txt
```

## Loading the data
As a first step, we will load the sales data from skate shops online and physical store and into BigQuery. In addition to the sales data we will also ingest the dlt load_info into BigQuery. This will help us track changes in our pipeline.

### Step 2: Adding the dlt pipeline code

In the `data_lineage.py` file remove the default code and add the following:

```python
FILEPATH = "data/supermarket_sales.csv"
FILEPATH_SHOPIFY = "data/orders_export_1.csv"

class Data_Pipeline:
    def __init__(self, pipeline_name, destination, dataset_name):
        self.pipeline_name = pipeline_name
        self.destination = destination
        self.dataset_name = dataset_name

    def run_pipeline(self, data, table_name, write_disposition):
        # Configure the pipeline with your destination details
        pipeline = dlt.pipeline(
            pipeline_name=self.pipeline_name, destination=self.destination, dataset_name=self.dataset_name
        )
        # Run the pipeline with the provided data
        load_info = pipeline.run(data, table_name=table_name, write_disposition=write_disposition)

        # Pretty print the information on data that was loaded
        print(load_info)
        return load_info
```

Any changes in the underlying data are captured by the dlt load_info. To showcase this we will filter the data to remove the **Branch** and **Tags** columns from Store and Shopify data respectively and run the pipeline. Later, we will add back the columns and rerun the pipeline. These new columns added will be recorded in the load_info packages. 

We add the load_info back to BigQuery to use in our Dashboard. The Dashboard will provide an overview of the schema changes each time the pipeline is executed and also track the row level lineage for any new table that we create.

```python
if __name__ == "__main__":

    data_store = pd.read_csv(FILEPATH)
    data_shopify = pd.read_csv(FILEPATH_SHOPIFY)

    #filtering some data. 
    select_c_data_store = data_store.loc[:, data_store.columns.difference(['Branch'])]
    select_c_data_shopify = data_shopify.loc[:, data_shopify.columns.difference(['Tags'])]

    pipeline_store = Data_Pipeline(pipeline_name='pipeline_store', destination='bigquery', dataset_name='sales_store')
    pipeline_shopify = Data_Pipeline(pipeline_name='pipeline_shopify', destination='bigquery', dataset_name='sales_shopify')

		load_a = pipeline_store.run_pipeline(data=select_c_data_store, table_name='sales_info', write_disposition='replace')
    load_b = pipeline_shopify.run_pipeline(data=select_c_data_shopify, table_name='sales_info', write_disposition='replace')

    pipeline_store.run_pipeline(data=load_a.load_packages, table_name="load_info", write_disposition="append")
    pipeline_shopify.run_pipeline(data=load_b.load_packages, table_name='load_info', write_disposition="append")
```

### Step 3: Run the dlt pipeline.

To run the pipeline execute the following command:

```python
python data_lineage.py
```

This will load the data into BigQuery. We now need to remove the column filters from the code and rerun the pipeline. This will add the filtered columns to the tables in BigQuery. The change will be captured by `dlt`.

## Data Transformation and Lineage

Now that both the Shopify and Store data is available in BigQuery we will use **dbt** to transform the data.

### Step 4: Initialize a dbt project and define model

To get started initialize a dbt project in the root directory:

```python
dbt init sales_dbt
```

Next, in the `sales_dbt/models` we define the dbt models. The first model will be the `fact_sales.sql`. The skate shop has two data sources the online Shopify source and the physical Store source. We need to combine the data from both sources to create a unified reporting feed. The **fact_sales** table will be our unified source.

Code for `fact_sales.sql`:

```sql
{{ config(materialized='table') }}

select 
  invoice_id,
  city,
  unit_price,
  quantity,
  total,
  date,
  payment,
  info._dlt_id,
  info._dlt_load_id,
  loads.schema_name,
  loads.inserted_at
from {{source('store', 'sales_info')}} as info
left join {{source('store', '_dlt_loads')}} as loads 
on  info._dlt_load_id = loads.load_id

union all

select 
  name as invoice_id,
  billing_city,
  lineitem_price,
  lineitem_quantity,
  total,
  created_at,
  payment_method,
  info._dlt_id,
  info._dlt_load_id,
  loads.schema_name,
  loads.inserted_at
from {{source('shopify', 'sales_info')}} as info
left join {{source('shopify', '_dlt_loads')}} as loads
on  info._dlt_load_id = loads.load_id
where financial_status = 'paid'
```

In the query, we join the sales information for each source with its dlt load_info. This will help us keep track of the number of rows added with each pipeline run. The `schema_name` identifies the source that populated the table and helps establish the table lineage. While the `_dlt_load_id` identifies the pipeline run that populated the each row and helps establish row level lineage. The sources are combined to create a **fact_sales** table by doing a union over both sources. 

Next, we define the **`schema_change.sql`** model to capture the changes in the table schema using following query:

```sql
{{ config(materialized='table') }}

select *
from {{source('store', 'load_info__tables__columns')}}

union all 

select *
from {{source('shopify', 'load_info__tables__columns')}}
```

In the query, we combine the load_info for both sources by doing a union over the sources. The resulting **schema_change** table contains records of the column changes that occur on each pipeline run. This will help us track the column lineage and will be used to create our Data Lineage Dashboard.

### Step 5: Run the dbt package

In the `data_lineage.py` add the the code to run the dbt package using `dlt`.

```python
pipeline_transform = dlt.pipeline(pipeline_name='pipeline_transform', destination='bigquery', dataset_name='sales_transform')

    venv = Venv.restore_current()
    here = os.path.dirname(os.path.realpath(__file__))

    dbt = dlt.dbt.package(
        pipeline_transform, 
        os.path.join(here, "sales_dbt/"),
        venv=venv
        )

    models = dbt.run_all()

    for m in models:
            print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")
```

Next, run the pipeline using the following command:

```python
python data_lineage.py
```

Once the pipeline is run a new dataset called **sales_transform** will be created in BigQuery which will contain the **fact_sales** and **schema_changes** tables that we defined in the dbt package.

### Step 6: Visualising the lineage in Metabase

To access the BigQuery data in Metabase we need to connect BigQuery to Metabase. Follow the Metabase [docs](https://www.metabase.com/docs/latest/databases/connections/bigquery) to connect BigQuery to Metabase.

Once BigQuery is connected with Metabase use the SQL Editor to create the the first table. The **Data Load Overview** table gives an overview of the dlt pipelines that populated the **fact_sales** table. It shows the pipeline names and the number of rows loaded into the **fact_sales** table by each pipeline.

![Metabase Report](https://d1ice69yfovmhk.cloudfront.net/images/data_lineage_metabase_report.png)

This can be used to track the rows loaded by each pipeline. An upper and lower threshold can be set and when our pipelines add rows above or below the threshold that can act as our canary in a the coal mine .

Next, we will visualize the **fact_sales** and the **schema_changes** as a table and add the `dlt_load_id` as a filter. The resulting Data Lineage Dashboard will give us an overview of the table, row and column level lineage for our data.

![Data Lineage Dashboard](https://d1ice69yfovmhk.cloudfront.net/images/data_lineage_dashboard.gif)

When we filter by the **dlt_load_id** the dashboard will filter for the specific pipeline run. In the **Fact Sales** table the column *schema_name* identifies the raw sources that populated the table (Table lineage). The table also shows only the rows that were added for the pipeline run (Row Lineage). Lastly, the **Updated Columns** table revels the columns that were added for filtered pipeline run (Column Lineage). 

When we ran the pipeline initially we filtered out the **Tags** column and later reintroduced it and ran the pipeline again. The **Updated Columns** shows that the Tags column was added to the Fact Sales table with the new pipeline run.

## Conclusion:

Data lineage provides an overview of the data journey from the source to destination. It is an important tool that can help troubleshoot pipeline. dlt load_info provides an alternative solution to visualizing data lineage by tracking changes in the underlying data.

Although `dlt` currently does not support data flow diagrams, it tracks changes in the data schema that can be used to create dashboards that provides an overview of table, row and column lineage for the loaded data.