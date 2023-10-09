---
slug: dlt-ops-startups
title: "PDF invoices → Real-time financial insights: How I stopped relying on an engineer to automate my workflow and learnt to do it myself"
image: /img/invoice_flowchart.png
authors:
  name: Anna Hoffmann
  title: COO
  url: https://github.com/ahoffix
  image_url: https://avatars.githubusercontent.com/u/12297835?v=4
tags: [PDF parsing, PDF invoice, PDF to DWH]
---

:::info
**TL;DR** *I set up a data pipeline that automatically extracts data from PDF invoices in my Gmail and puts it all in a single Google Sheet where I can easily keep of track of it. I did this using the python library [dlt](https://dlthub.com/docs/intro) that uses [langchain](https://www.langchain.com/) and LLMs to read PDF data and converts it into structured tables.*
:::

I am Anna, co-founder & COO of dltHub. As an ops lead with many years of running SMB-size startups, I find myself juggling a myriad of tasks, from administration, finance, and people to customer support or sales. These tasks come with their own data, all of which are crucial for making decisions. This creates a huge scope for automation, but unfortunately getting engineering support is not always easy. Whether it's integrating tools with APIs or managing data efficiently, the waiting game can be frustrating.

So, I often end up doing manual tasks such as budgeting, cost estimation, updating CRM, or preparing audits. I have been dreaming about automating these processes.

For example, I need to analyze expenses in order to prepare a budget estimation. I get numerous PDFs daily in a dedicated Gmail group inbox. I was wondering to which extent [dlt](https://github.com/dlt-hub/dlt) can help fulfill my automation dream. I decided to work with Alena from our data team on an internal project.

![invoice flow chart](/img/invoice_flowchart.png)

## Use Case

Imagine this scenario: your team receives numerous invoices as email attachments daily. You need to extract and analyze the data within these invoices to gain insights crucial to your operations. This is where the data load tool (`dlt`) steps in.

Alena created a [pipeline](https://github.com/dlt-hub/dlt_invoices) using `dlt` that automates the process of translating invoices received as email attachments in a specific Google email group and stores them in a database (for example, [BigQuery](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery) or [DuckDB](https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb)).

As a non-coder working in tech startups for a long time, I finally got a chance to learn how to use the terminal and run a simple pipeline.

Here's a summary of how it works.

## Let’s get started

In this article, I will show you an example of loading structured data from invoices received by email into [BigQuery](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery). For more details, check the [README.md](https://github.com/dlt-hub/dlt_invoices) in the GitHub repository.

### Step 1. Preparation

Make sure that you have all you need:

- Make sure you have Python 3.x installed on your system.
- Use a virtual environment (more details on how to [set up the environment](https://dlthub.com/docs/reference/installation#set-up-environment)).
- Install the `dlt` library by using `pip install "dlt[bigquery]"`.
- Create a project folder on your laptop. I called mine “unstructured_data_pipeline”.
- We will need access to LLM, Langchain will use OpenAI models by default, so we also used an OpenAI API token.
- Using a tool like Visual Studio makes it easier.

### Step 2. Initiate the pipeline

To create the pipeline, we will use the `dlt` verified source [unstructured_data](https://github.com/dlt-hub/verified-sources/blob/master/sources/unstructured_data/README.md), which includes the verified source [inbox](https://github.com/dlt-hub/verified-sources/blob/master/sources/unstructured_data/inbox/README.md).

- Init the pipeline by using `dlt init unstructured_data bigquery`.
- Install necessary requirements `pip install -r requirements.txt`.

### Step 3. Set up your credentials

The `dlt` [init command](https://dlthub.com/docs/reference/command-line-interface#dlt-init) creates folder `.dlt` in your project directory, and clones the source code from the [verified-sources repository](https://github.com/dlt-hub/verified-sources).

- Open `.dlt/secrets.toml` file on your laptop.
- Enter the OpenAI secrets:

    ```
    [sources.unstructured_data]
    openai_api_key = "openai_api_key"
    ```

- Enter your email account secrets in the same section `[sources.unstructured_data]`:

    ```
    host = 'imap.example.com'
    email_account = "example@example.com"
    password = 'set me up!'
    ```

  Check [here](https://github.com/dlt-hub/dlt_invoices#configure-inbox-source) how to configure the inbox source.

- Enter the BigQuery secrets:

    ```
    [destination.bigquery]
    location = "US"
    [destination.bigquery.credentials]
    project_id = "set me up!"
    private_key = "set me up!"
    client_email = "set me up!"
    ```


Read more about [dlt credentials](https://dlthub.com/docs/general-usage/credentials) and [BigQuery credentials](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery).

### Step 5: Define your queries

This is the part where you can define what you’d like to see as an outcome.

Queries example:

```python
INVOICE_QUERIES = {
    "recipient_company_name": "Who is the recipient of the invoice? Just return the name. If you don't know, then return None",
    "invoice_amount": "What is the total amount of the invoice? Just return the amount as decimal number, no currency or text. If you don't know, then return None",
    "invoice_date": "What is the date of the invoice? Just return the date. If you don't know, then return None",
    "invoice_number": "What is the invoice number? Just return the number. If you don't know, then return None",
    "service_description": "What is the description of the service that this invoice is for? Just return the description. If you don't know, then return None",
}
```

Customize the INVOICE_QUERIES dictionary in the `unstructured_data/settings.py` file if you want to extract other information, or if your invoices have a different structure.

### Step 6: Run the pipeline!

And now the magic happens. Use the following command to run the pipeline:

```shell
python unstructured_data_pipeline.py
```

In the next step, `dlt` will save all processed structured data to the database (in my case, BigQuery).

### Step 7: Check the outcome in BigQuery

If you load it to BigQuery like I did in my example, then you can look at your data using BigQuery UI or export it directly to a Google sheet.

### Step 8: Deploy

Now you can [deploy this script with GitHub Actions](https://dlthub.com/docs/walkthroughs/deploy-a-pipeline/deploy-with-github-actions) as we did, so that it checks your incoming email every day and processes invoices automatically.

# Outcome**:**

Here’s how the result looks like in BigQuery:

![screenshot 1](/img/pdf_parse_outcome_1.png)

…and as a Google Sheet. You can easily export this table from BigQuery to Google Sheets using the Export button in the top right corner.

![screenshot 2](/img/pdf_parse_outcome_2.png)

Bonus: In order to have a Google Sheet with live updates, you can go to the Data tab in your
Spreadsheet → Data Connectors → BigQuery → choose your database and voila, your data will be updated automatically.

![screenshot 3](/img/pdf_parse_outcome_3.png)

# **Conclusion:**

### **This worked well:**

- `dlt` was good at extracting the data I needed, and it indeed worked in real-time. I needed some support from Alena when running the pipeline for the first time, but that’s because I had never coded before. 😊
- I was able to see the details that are relevant to my workaround budgeting.

### **This did not work well:**

- Some PDFs don’t get transformed correctly. Some details were missing or misspelled. That depends on the LLM, which extracts structured data from a raw text. And also on the invoice structure.
- And it only worked well with digital PDFs, though not with JPG/scanned versions. Luckily, 99% of all the invoices are of the former kind. However, you can even set up this process for the other kinds of documents by making modifications to [unstructured.io](https://unstructured.io/).

# Where to go next?

It was definitely a great start, and we will test it further. And I already have many other use cases where dlt pipelines could help with ops automation processes. E.g.:

- In creating a list of all contracts based on PDFs in a Google Drive folder (super useful for audits).
- In moving specific data to CRM (e.g. invoice related information about the customers).

This specific example illustrates just one way in which Operations Leads can harness the power of `dlt` to analyze data efficiently without relying on engineers for extensive support. By automating data processes and enabling real-time insights, `dlt` empowers small startups to make informed decisions and stay competitive in their respective markets.

In the startup world where time is of the essence, dlt has a chance to be the key to unlock data's full potential and accelerate operational efficiency. I’m looking forward to saying goodbye to endless waiting and hello to a world where Operations Leads can take control of their data needs, all thanks to `dlt`.
