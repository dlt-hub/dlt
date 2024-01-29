---
slug: next-generation-data-platform
title: "The structured data lake: How schema evolution enables the next generation of data platforms"
image: https://dlthub.com/docs/img/dlthub-logo.png
authors:
  name: Adrian Brudaru
  title: Open source data engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [data platform, structured data lake, schema evolution]
---

:::info
[Google Colaboratory demo](https://colab.research.google.com/drive/1H6HKFi-U1V4p0afVucw_Jzv1oiFbH2bu#scrollTo=e4y4sQ78P_OM)

This colab demo was built and shown by our working student Rahul Joshi, for the Berlin Data meetup, where he talked about the state of schema evolution in the open source.
:::

# What is schema evolution?

In the fast-paced world of data, the only constant is change, and it usually comes unannounced.

### **Schema on read**

Schema on read means your data does not have a schema, but your consumer expects one. So when they read, they define the schema, and if the unstructured data does not have the same schema, issues happen.

### **Schema on write**

So, to avoid things breaking on running, you would want to define a schema upfront - hence you would structure the data. The problem with structuring data is that it’s a labor intensive process that makes people take pragmatic shortcuts of structuring only some data, which later leads to lots of maintenance.

Schema evolution means that a schema is automatically generated on write for the data, and automatically adjusted for any changes in the data, enabling a robust and clean environment downstream. It’s an automatic data structuring process that is aimed at saving time during creation, maintenance, and recovery.

# Why do schema evolution?

One way or another, produced raw unstructured data becomes structured during usage. So, which paradigm should we use around structuring?

Let’s look at the 3 existing paradigms, their complexities, and what a better solution could look like.

## The old ways

### **The data warehouse paradigm: Curating unstructured data upfront**

Traditionally, many organizations have adopted a 'curate first' approach to data management, particularly when dealing with unstructured data.

The desired outcome is that by curating the data upfront, we can directly extract value from it later. However, this approach has several pitfalls.

**Why curating unstructured data first is a bad idea**

1. **It's labor-intensive:** Unstructured data is inherently messy and complex. Curating it requires significant manual effort, which is time-consuming and error-prone.
2. **It's difficult to scale:** As the volume of unstructured data grows, the task of curating it becomes increasingly overwhelming. It's simply not feasible to keep up with the onslaught of new data. For example, Data Mesh paradigm tries to address this.
3. **It delays value extraction:** By focusing on upfront curation, organizations often delay the point at which they can start extracting value from their data. Valuable insights are often time-sensitive, and any delay could mean missed opportunities.
4. **It assumes we know what the stakeholders will need:** Curating data requires us to make assumptions about what data will be useful and how it should be structured. These assumptions might be wrong, leading to wasted effort or even loss of valuable information.

### **The data lake paradigm: Schema-on-read with unstructured data**

In an attempt to bypass upfront data structuring and curation, some organizations adopt a schema-on-read approach, especially when dealing with data lakes. While this offers flexibility, it comes with its share of issues:

1. **Inconsistency and quality issues:** As there is no enforced structure or standard when data is ingested into the data lake, the data can be inconsistent and of varying quality. This could lead to inaccurate analysis and unreliable insights.
2. **Complexity and performance costs:** Schema-on-read pushes the cost of data processing to the read stage. Every time someone queries the data, they must parse through the unstructured data and apply the schema. This adds complexity and may impact performance, especially with large datasets.
3. **Data literacy and skill gap:** With schema-on-read, each user is responsible for understanding the data structure and using it correctly, which is unreasonable to expect with undocumented unstructured data.
4. **Lack of governance:** Without a defined structure, data governance can be a challenge. It's difficult to apply data quality, data privacy, or data lifecycle policies consistently.

### **The hybrid approach: The lakehouse**

- The data lakehouse uses the data lake as a staging area for creating a warehouse-like structured data store.
- This does not solve any of the previous issues with the two paradigms, but rather allows users to choose which one they apply on a case-by-case basis.

## The new way

### **The current solution : Structured data lakes**

Instead of trying to curate unstructured data upfront, a more effective approach is to structure the data first with some kind of automation. By applying a structured schema to the data, we can more easily manage, query, and analyze the data.

Here's why structuring data before curation is a good idea:

1. **It reduces maintenance:** By automating the schema creation and maintenance, you remove 80% of maintenance events of pipelines.
2. **It simplifies the data:** By imposing a structure on the data, we can reduce its complexity, making it easier to understand, manage, and use.
3. **It enables automation:** Structured data is more amenable to automated testing and processing, including cleaning, transformation, and analysis. This can significantly reduce the manual effort required to manage the data.
4. **It facilitates value extraction:** With structured data, we can more quickly and easily extract valuable insights. We don't need to wait for the entire dataset to be curated before we start using it.
5. **It's more scalable:** Reading structured data enables us to only read the parts we care about, making it faster, cheaper, and more scalable.

Therefore, adopting a 'structure first' approach to data management can help organizations more effectively leverage their unstructured data, minimizing the effort, time, and complexity involved in data curation and maximizing the value they can extract from their data.

An example of such a structured lake would be parquet file data lakes, which are both, structured and inclusive of all data. However, the challenge here is creating the structured parquet files and maintaining the schemas, for which the delta lake framework provides some decent solutions, but is still far from complete.

## The better way

So, what if writing and merging parquet files is not for you? After all, file-based data lakes capture a minority of the data market.

### `dlt` is the first python library in the open source to offer schema evolution

`dlt` enables organizations to impose structure on data as it's loaded into the data lake. This approach, often termed as schema-on-load or schema-on-write, provides the best of both worlds:

1. **Easier maintenance:** By notifying the data producer and consumer of loaded data schema changes, they can quickly decide together how to adjust downstream usage, enabling immediate recovery.
2. **Consistency and quality:** By applying structure and data typing rules during ingestion, `dlt` ensures data consistency and quality. This leads to more reliable analysis and insights.
3. **Improved performance:** With schema-on-write, the computational cost is handled during ingestion, not when querying the data. This simplifies queries and improves performance.
4. **Ease of use:** Structured data is easier to understand and use, lowering the skill barrier for users. They no longer need to understand the intricate details of the data structure.
5. **Data governance:** Having a defined schema allows for more effective data governance. Policies for data quality, data privacy, and data lifecycle can be applied consistently and automatically.

By adopting a 'structure first' approach with `dlt`, organizations can effectively manage unstructured data in common destinations, optimizing for both, flexibility and control. It helps them overcome the challenges of schema-on-read, while reaping the benefits of a structured, scalable, and governance-friendly data environment.

To try out schema evolution with `dlt`, check out our [colab demo.](https://colab.research.google.com/drive/1H6HKFi-U1V4p0afVucw_Jzv1oiFbH2bu#scrollTo=e4y4sQ78P_OM)



![colab demo](/img/schema_evolution_colab_demo_light.png)

### Want more?

- Join our [Slack](https://dlthub.com/community)
- Read our [schema evolution blog post](https://dlthub.com/docs/blog/schema-evolution)
- Stay tuned for the next article in the series: *How to do schema evolution with* `dlt` *in the most effective way*