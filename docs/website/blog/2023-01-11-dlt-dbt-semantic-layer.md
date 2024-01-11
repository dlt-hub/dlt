---
slug: dlt-dbt-semantic-layer
title: "dlt & dbt in Semantic Modelling"
image: /img/people-stuck-with-tables-2.jpg
authors:
  name: Hiba Jamal
  title: Data Science intern at dlthub
  url: https://github.com/hibajamal
  image_url: https://avatars.githubusercontent.com/u/35984866?v=4
tags: [semantic modelling]
---

<aside>
💡 TLDR; Thanks to dbt’s new semantic layer feature we can now write “data pipeline” and “business user access” in the same sentence. In this article we explore how to create an end to end pipeline using the dlt library and dbt’s new feature.
</aside>

## The Chinese Whisper of Data

In the context of constructing a **modern data stack** through the development of various modular components for a data pipeline, our attention turns to the centralization of metrics and their definitions.

For the purposes of this demo, we’ll be looking specifically at how dlt and dbt come together to solve the problem of the data flow from data engineer → analytics engineer → data analyst → business user. That’s quite a journey. And just like any game of *Chinese whisper*, things certainly do get lost in translation.

<div style={{ paddingRight: '10%', paddingLeft: '10%', paddingBottom: '1%' }}>

![cover](/img/blog-dbt_sem-data-people.png)
<div>Taken from the real or fictitious book called 5th grade data engineering, 1998.</div>

</div>

To solve for this problem, both these tools come together and seamlessly integrate to create everything from data sources to uniform metric definitions, that can be handled centrally, and hence are a big aid to the data democracy practices of your company!

Here’s how a pipeline could look:
1. Extract and load with dlt: Dlt will automate data cleaning and normalization leaving you with clean data you can just use.
2. Create SQL models that simplify sources, if needed. This can include renaming and/or eliminating columns, identifying and setting down key constraints, fixing data types, etc.
3. Create and manage central metric definitions with the semantic layer.

## 1. Extract, Structure, & Load with dlt

The data being used is of a questionnaire, which includes questions, the options of those questions, respondents and responses. This data is contained within a nested json object, that we’ll pass as a raw source to dlt to structure, normalize and dump into a BigQuery destination.

```python
# initializing the dlt pipeline with your data warehouse destination
pipeline = dlt.pipeline(
        pipeline_name="survey_pipeline",
        destination="bigquery",
        dataset_name="questionnaire")

# running the pipeline (into a structured model)
# the dataset variable contains unstructured data
pipeline.run(dataset, table_name='survey')
```

The extract, and load steps of an ETL pipeline have been taken care of with these steps. Here’s what the final structure looks like in BigQuery:

![bigquery tables](/img/blog-dbt_sem-data-bqtables.png)

questionnaire is a well structured dataset with a base table, and child tables. The survey__questions and survey_questions__options are normalized tables with, the individual questions and options of those questions, respectively, connected by a foreign key. The same structure is followed with the __respondents tables, with survey__respondents__responses as our fact table.

## 2. Transformation with dbt

For transformation, we head to `dbt`.

- The tables created by dlt are loaded as sources into `dbt`, with the same columns and structure as created by `dlt`.
- Since not much change is required to our original data, we can utilization the model creation ability of `dbt` to create a metric, whose results can directly be pulled by users.

Say, we would like to find the average age of people by their favorite color. First, we’d create an SQL model to find the age per person, the sources used are presented in the following image:

![dag 1](/img/blog-dbt_sem-dag1.png)

Next, using this information, we can find the average age for each favorite color. The sources used are as follows:

![dag 2](/img/blog-dbt_sem-dag2.png)

This is one method of centralizing a metric definition or formula,  that you create a model out of it for people to directly pull into their reports.

## 3. Central Metric Definitions & Semantic Modelling with dbt

The other method of creating a metric definition, powered by MetricFlow, is the dbt semantic layer. Using MetricFlow we define our metrics in yaml files and then directly query them from any different reporting tool. Hence ensuring that no one gets a different result when they are trying to query company metrics and defining formulas and filters for themselves. For example, we created a semantic model named questionnaire, defining different entities, dimensions and measures. Like as follows:

```yaml
model: ref('fact_table') # where the columns referred in this model will be taken from
# possible joining key columns
entities:
  - name: id
    type: primary
# where in SQL you would: create the aggregation column
measures:
  - name: surveys_total
    description: The total surveys for each --dimension.
    agg: count
		# if all rows need to be counted then expr = 1
    expr: 1
# where in SQL you would: group by columns
dimensions:
	# default dbt requirement
  - name: surveyed_at
    type: time
    type_params:
        time_granularity: day
  # count entry per answer
	- name: people_per_color
    type: categorical
    expr: answer
	# count entry per question
  - name: question
    type: categorical
    expr: question
```

Next, a metric is created from it:

```yaml
metrics:
  - name: favorite_color
    description: Number of people with favorite colors.
    type: simple
    label: Favorite Colors
    type_params:
			# reference of the measure created in the semantic model
      measure: surveys_total
		filter: | # adding a filter on the "question" column for asking about favorite color
			{{  Dimension('id__question') }} = 'What is your favorite color?'
```

The DAG then looks like this:

![dag 3](/img/blog-dbt_sem-dag3.png)

We can now [query](https://docs.getdbt.com/docs/use-dbt-semantic-layer/quickstart-sl#test-and-query-metrics) this query, using whichever dimension we want. For example, here is a sample query: `dbt sl query --metrics favorite_color --group-by id__people_per_color`

The result of which is:

![query result](/img/blog-dbt_sem-query-result.png)

And just like that, the confusion of multiple people querying or pulling from different sources and different definitions get resolved. With aliases for different dimensions, the question of which column and table to pull from can be hidden - it adds a necessary level of abstraction for the average business end user.