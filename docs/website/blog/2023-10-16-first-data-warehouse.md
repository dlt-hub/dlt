---
slug: first-data-warehouse
title: "Your first data warehouse: A practical approach"
image: /img/oil-painted-dashboard.png
authors:
  name: Adrian Brudaru
  title: Open source data engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [first data warehouse]
---

> In this article I will not discuss the best data warehouse you could in theory build. Instead, I will describe how data warehousing projects pragmatically start in order to have an easy time building and improving without running into early limits.
>

Building a data warehouse is a complex endeavor, often too intricate to navigate flawlessly in the initial attempt. In this article, we'll provide insights and pointers to guide you in choosing the right stack for your data warehouse.

![hard coded dashboard](/img/oil-painted-dashboard.png)



## The Business requirements

Understanding the business's performance is the initial priority, and achieving this necessitates a comprehensive understanding of the business model and its various intricacies. Tracking key processes and Key Performance Indicators (KPIs) is fundamental as they provide insights into the business's health and performance across various aspects such as sales, marketing, customer engagement, operational efficiency, and financial health.

Collaboration with different departments is crucial to comprehensively grasp their unique perspectives and priorities. Engaging with stakeholders ensures that the data warehouse is designed to cater to a wide array of informational needs, aligning with the organizational goals and objectives.

Furthermore, identifying pivotal business drivers is essential. Beyond explicit feedback, it's crucial to recognize the primary business levers often represented by cross-departmental data. These drivers shed light on the core aspects that significantly impact the business's success. For instance, in an e-commerce business, the main levers might focus on increasing customer lifetime value, improving conversion rates, and optimizing ad spend to align with the customer's worth.

## The Tech stack

### Orchestration

Orchestration functions as the central control mechanism, overseeing and coordinating the execution of diverse data workflows.

For your first data warehouse build, opting for a managed solution often proves pragmatic.
Major cloud platforms provide managed versions of orchestrators like Airflow, ensuring reliability and relieving the burden of self-hosting.
While this convenience comes at some cost, the investment is justified considering the potential intricacies and management efforts associated with self-hosting, which could potentially outweigh server expenses.
Keep in mind that cloud vendors like gcp will only charge for the rented services/hardware, and so their managed airflow is priced the same as the one you would manage.

The most well known orchestrator is Airflow which is open source maintained by an open source community.

There are many newer orchestrators that improve on Airflow’s design and shortcomings, with varying features and approaches.
Prefect, Dagster, Mage, and Kestra stand out as prominent contenders, introducing unique features and approaches that push the boundaries of orchestration.

Besides standards, you can always go for simplicity by looking out of the box - Github Actions is actually an orchestrator and while not particularly feature rich, it is often sufficient for a basic load-trasnform setup.

### Ingestion

Future-proofing your data warehouse cannot be done by relying on the hope that vendors will fulfill your requirements. While it is easy to start with a SaaS pipeline solution, they are generally expensive and end up vendor locking you to their schema, creating migration pains if you want to move and improve.
There are also reasons to use SaaS such as not having the in-house python team or deciding to suffer the cost and outsource the effort.

But one way or another, you end up building custom pipelines for reasons like:

- SQL pipelines are simple to create but cost a ton on SaaS services.
- The vendor does not have all the endpoints and too few customers asked for it for them to care.
- You start using a new service the vendor doesn’t offer.

So, to have a clean setup, you would be better off standardizing a custom ingester. Here, you can write your own, or use the dlt library which is purpose-made and will generate database agnostic schemas, enabling migration between databases at the flip of a switch - making your test setups even easier.

If you do write your own, choose a common interchange format and create it to load from that (such as json) and have all your extractors output json.

You could also consider customizable solutions like Airbyte or Meltano. However, they follow their own paradigms, which ultimately create difficulties when trying to maintain or keep a stable, robust stack.

### Transforming Data

Transforming raw data into a structured, analytical format is a pivotal step in the data pipeline. In this domain, **dbt** stands out as a robust solution with widespread adoption, extensive documentation, and now, a standard tool. However, it's not the only player. Alternatives like **SQLMesh** are evolving this space, introducing enhancements tailored to specific use cases. For instance, SQLMesh's innovation in achieving database agnosticism through the use of sqlglot under the hood sets it apart.

When it comes to data modeling, **star schemas** emerge as the preferred choice for many due to their advantages, including efficient and clear code and support for ROLAP tools. However, it's crucial to note that the transformation code is both quantitative and complex, making adherence to best practices imperative for maintenance and scalability.

### Reverse ETL

While implementing **Reverse ETL** might not be an initial priority, it's essential to demystify the process.
For those new to pushing data via an API, it may seem intimidating.
Let's simplify - sending data to an API endpoint for loading or updating an object is similar to making a `GET` request.
Here's a straightforward example in Python:

```python
# Assuming data is in this format
import requests
# assume we have a table of contacts we want to push to Pipedrive.
data_table = [{'name': 'abc', 'email': 'abc@d.com'},]

# Post the data to this endpoint
API_URL = f'https://api.pipedrive.com/v1/persons?api_token={YOUR_API_TOKEN}&pipeline_id={PIPELINE_ID}'
for row in data_table:
    response = requests.post(API_URL, headers=headers, data=json.dumps(row))
```

For those seeking tools, Census and Hightouch are prominent players in this space.

## Dashboards & their usage paradigms

When it comes to dashboards, each tool follows its distinctive paradigm. For example, Tableau and PowerBI are good for analysts to make polished dashboards for business users, while Metabase offers more simplicity and self service for more technically able business users.

If you're uncertain about your choice, starting with something simple and rooted in ROLAP (Relational Online Analytical Processing) is a sound approach.
ROLAP plays a pivotal role and should not be underestimated—it's the linchpin for leveraging star schemas.

But what exactly is ROLAP? ROLAP lets you define links between tables,
allowing the tool to present data as if it's pre-joined,
performing actual joins only when needed.

Essentially, ROLAP transforms a star schema into what appears to be a single table for the end user.
This setup empowers users to navigate and explore data seamlessly using a "pivot-table" like interface
commonly found in BI tools.

By using ROLAP, we are able to maintain single versions of dimension tables,
and reduce maintenance efforts while increasing flexibility and velocity for the end user.

## Data stack Governance

This section sheds light on strategies for efficient management of your data stack. Here are key tips to get you started:

- **Version control is essential:**
  Version control, like using Git, is akin to having a safety net for your code. It ensures you can track changes, collaborate seamlessly, and revert to previous versions if needed.

- **Early alert setup:**
  Implementing alert mechanisms from the get-go is like having a diligent watchdog for your data. It helps you catch issues early, preserving trust in your data. Check out this [guide on using dlt to send alerts to Slack](https://dlthub.com/docs/running-in-production/running#using-slack-to-send-messages).

- **Streamlined workflows and CI/CD:**
  Streamlining your workflows and embracing CI/CD is like putting your data operations on an express lane. It speeds up development, minimizes errors, and ensures a smoother deployment process. If you're using Airflow on GCP, this [simple setup guide](https://dlthub.com/docs/reference/explainers/airflow-gcp-cloud-composer) is your friend.

- **Assumption testing:**
  Adding comprehensive tests is akin to having a safety net beneath a trapeze artist. It gives you the confidence to make changes or additions without fearing a crash.

- **Goal-oriented KPI definition:**
  When defining KPIs, always keep the end goal in mind. Tailor your KPIs to what matters most for each business function. Marketing may dance to the tune of signups, Finance to contracts, and Operations to active users.

- **Implement lineage for faster Troubleshooting:**
  Implementing lineage is like having a well-organized toolbox. It helps you trace and understand the journey of your data, making troubleshooting and model iteration a breeze.

These foundational practices form the cornerstone of effective data stack governance, ensuring a sturdy structure that grows with your data needs.

## In Conclusion: a simple beginning, a challenging growth

Initiating a data warehouse project doesn't have to be an uphill struggle.
In fact, starting with simplicity can often be the wisest path.
With minimal effort, you can accomplish a great deal of what a data team requires in the initial stages.

The true test lies in scaling—the journey from a streamlined beginning to a comprehensive,
organization-wide data infrastructure.
This evolution is where most of the challenge happens - adoption, stakeholder education and culture change happen in this step too.
However, it's worth noting that having an entire team of data experts right at the start of this journey is a rarity.
Therefore, while scaling is a critical aspect, delving into the intricacies of extensive
team and organizational scaling ventures beyond the scope of this article.


## Resources

If you're building on Google Cloud Platform (GCP), here are some tutorials and resources that can aid you in your data warehouse setup:

1. **Deploy Cloud Composer with CI/CD from GitHub Repo**
   Tutorial Link: [Deploy Cloud Composer with CI/CD](https://dlthub.com/docs/reference/explainers/airflow-gcp-cloud-composer)

2. **Deploy DLT to Cloud Composer**
   Tutorial Link: [Deploy dlt to Cloud Composer](https://dlthub.com/docs/walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer)

3. **Deploy dbt to Cloud Composer**
   Tutorial Link: [Deploy dbt to Cloud Composer](https://dlthub.com/docs/dlt-ecosystem/transformations/dbt)

4. **Setting up Alerts to Slack**
   Tutorial Link: [Setting up Alerts to Slack](https://dlthub.com/docs/running-in-production/running#using-slack-to-send-messages). For integrating it into on-failure callbacks, refer to the [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html)

5. **Example ROLAP Definition on Holistics Tool**
   Tutorial Link: [Example ROLAP Definition on Holistics Tool](https://dlthub.com/docs/blog/MongoDB-dlt-Holistics)


Want to discuss dlt and data lakes or warehouses?

- Dive into our [Getting Started.](https://dlthub.com/docs/getting-started)
- [Join the ⭐Slack Community⭐ for discussion and help!](https://dlthub.com/community)
