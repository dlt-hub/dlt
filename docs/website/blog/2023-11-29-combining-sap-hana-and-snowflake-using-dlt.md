---
slug: dlt-dagster
title: "Combining SAP HANA and Snowflake using dlt"
image: https://dlt-static.s3.eu-central-1.amazonaws.com/images/sap_snowflake_blog_data_link_image.png
authors:
    name: Rahul Joshi
    title: Data Science Intern at dltHub
    url: https://github.com/rahuljo
    image_url: https://avatars.githubusercontent.com/u/28861929?v=4
tags: [SAP, SAP HANA, Snowflake, Cloud, ETL]
---
:::info
**TL;DR: While most companies continue to build their businesses on top of SAP, when it comes to analytics, they prefer to take advantage of the price and elastic compute of modern cloud infrastructure. As a consequence, we get several dlt users asking for a simple and low-cost way to migrate from SAP to cloud data warehouses like Snowflake. In this blog, I show how you can build a custom SAP connector with dlt and use it to load SAP HANA tables into Snowflake.**
:::  

![Blog image](https://dlt-static.s3.eu-central-1.amazonaws.com/images/sap_snowflake_blog_data_link_image.png)

In case you haven’t figured it out already, we at dltHub love creating blogs and demos. It’s fun, creative, and gives us a chance to play around with many new tools. We are able to do this mostly because, like any other modern tooling, dlt just *fits* in the modern ecosystem. Not only does dlt have existing [integrations](https://dlthub.com/docs/dlt-ecosystem) (to, for example, GCP, AWS, dbt, airflow etc.) that can simply be “plugged in”, but it is also very simple to customize it to integrate with almost any other modern tool (such as [Metabase](https://dlthub.com/docs/blog/postgresql-bigquery-metabase-demo), [Holistics](https://dlthub.com/docs/blog/MongoDB-dlt-Holistics), [Dagster](https://dlthub.com/docs/blog/dlt-dagster), [Prefect](https://dlthub.com/docs/blog/dlt-prefect) etc.). 

But what about enterprise systems like SAP? They are, after all, the most ubiquitous tooling out there: according to SAP [data](https://assets.cdn.sap.com/sapcom/docs/2017/04/4666ecdd-b67c-0010-82c7-eda71af511fa.pdf), 99 out of 100 largest companies are SAP customers. A huge part of the reason for this is that their ERP system is still the gold standard in terms of effectivity and reliability. However, when it comes to OLAP workloads like analytics, machine learning, predictive modelling etc., [many companies prefer the convenience and cost savings of modern cloud solutions](https://www.statista.com/statistics/967365/worldwide-cloud-infrastructure-services-market-share-vendor/) like GCP, AWS, Azure, etc..

So, wouldn’t it be nice to be able to integrate SAP into the modern ecosystem?

Unfortunately, this is not that simple. Unlike modern tools, SAP does not integrate easily with non-SAP systems, and migrating data out from SAP is complicated and/or costly. This often means that ERP data stays separate from analytics data, resulting in data silos and preventing companies from making the most out of their analytics.

## Creating a dlt integration  
  
As a first step, I decided to create a custom dlt integration to SAP’s in-memory data warehouse: SAP HANA. I chose SAP HANA since it’s an OLAP database, and our users have been specifically asking us for this connector. Plus, given its SQL backend and [Python API](https://developers.sap.com/tutorials/hana-clients-python.html), dlt should also have no problem connecting to it. 

I then use this pipeline to load SAP HANA tables into Snowflake,  since Snowflake is cloud agnostic and can be run in different environments (such AWS, GCP, Azure, or any combination of the three). This is how I did it:  
  
**Step 1: I created an instance in [SAP HANA cloud](https://www.sap.com/products/technology-platform/hana.html).**

(*I used [this helpful tutorial](https://www.youtube.com/watch?v=hEQCGBAn7Tc&list=PLkzo92owKnVwtyoQRRN2LsQlTHzNE-0US) to navigate SAP HANA.*)

![SAP instance](https://dlt-static.s3.eu-central-1.amazonaws.com/images/sap_snowflake_blog_creating_sap_instance.png)

**Step 2: I inserted some sample data.**  
![SAP insert data](https://dlt-static.s3.eu-central-1.amazonaws.com/images/sap_snowflake_blog_inserting_data_in_sap.png)
  
**Step 3: With tables created in SAP HANA, I was now ready to create a dlt pipeline to extract it into Snowflake:**

Since SAP HANA has a SQL backend, I decided to extract the data using dlt’s [SQL source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database)

1. I first created a dlt pipeline
    
    `dlt init sql_database snowflake`  
    
2. I then passed the connection string for my HANA instance inside the loading function in `sql_database_pipeline.py`. (Optional: I also specified the tables that I wanted to load in `sql_database().with_resources("v_city", "v_hotel", "room")` )
3. Before running the pipeline I installed all necessary requirements using 
    
    `pip install -r requirements.txt`
    
    The dependencies inside `requirements.txt` are for the general SQL source. To extract data specifically from HANA, I also installed the packages `hdbcli` and `sqlalchemy-hana`.
    

**Step 4: I finally ran the pipeline using `python sql_database_pipeline.py`. This loaded the tables into Snowflake.**  

![Data in Snowflake](https://dlt-static.s3.eu-central-1.amazonaws.com/images/sap_snowflake_blog_data_loaded_into_snowflake.png)
  
## Takeaway

The dlt SAP HANA connector constructed in this demo works like any other dlt connector, and is able to successfully load data from SAP HANA into data warehouses like Snowflake.

Furthermore, the demo only used a toy example, but the SQL source is a production-ready source with incremental loading, merges, data contracts etc., which means that this pipeline could also be configured for production use-cases.

Finally, the dlt-SAP integration has bigger consequences: it allows you to add other tools like dbt, airflow etc. easily into an SAP workflow, since all of these tools integrate well with dlt.

## Next steps

This was a just first step into exploring what’s possible. Creating a custom dlt connector worked pretty well for SAP HANA, and there are several possible next steps, such as converting this to a verified source, or building other SAP connectors.

1. **Creating a verified source for SAP HANA:** This should be pretty straight-forward since it would require a small modification of the existing SQL source.
2. **Creating a dlt connector for SAP S/4 HANA:** S/4 HANA is SAP’s ERP software that runs on the HANA database. The use case would be to load ERP tables from S/4 HANA into other data warehouses like Snowflake. Depending on the requirements, there are two ways to go about it:
    1. **Low volume data:** This would again be straight-forward. SAP offers [REST API end points](https://api.sap.com/products/SAPS4HANACloud/apis/ODATA) to access ERP tables, and dlt is designed to be able to load data from any such end point. 
    2. **High volume data:** dlt can also be configured for the use case of migrating large volumes of data with fast incremental or merge syncs. But this would require some additional steps, such as configuring the pipeline to access HANA backend directly from Python hdbcli.
