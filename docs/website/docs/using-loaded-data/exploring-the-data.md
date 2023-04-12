---
title: Exploring the data
description: Exploring data that has been loaded
keywords: [exploring, loaded data, data quality]
---

# Exploring the data
Once you ran a pipeline locally, you can launch a web app that displays the loaded data.
To do so, run the cli command below with your pipeline name. The pipeline name is found in the code and also displayed in your terminal when loading.
```bash
dlt pipeline {pipeline_name} show
```

This will open a streamlit app with
- information about the loads
- tables and sample data
- sql client in which you can run queries to explore the data

## Data Quality Dashboards

After deploying a `dlt` pipeline, you might ask yourself: how can we know if the data is and remains high quality?

There are two ways to catch errors:
1. Tests
2. People [monitoring](../running-in-production/monitoring.md)

## Tests

The first time you load data from a pipeline you have built, you will likely want to test it. Plot the data on time series line charts and look for any interruptions or spikes, which will highlight any gaps or loading issues.

### Data usage as monitoring

Setting up monitoring is a good idea. However, in practice, often by the time you notice something is wrong through reviewing charts, someone in the business has likely already noticed something is wrong. That is, if there is usage of the data, then that usage will act as sort of monitoring.

### Plotting main metrics on line charts

In cases where data is not being used that much (e.g. only one marketing analyst is using some data alone), then it is a good idea to have them plot their main metrics on "last 7 days" line charts, so it's visible to them that something may be off when they check their metrics.

It's important to think about granularity here. A daily line chart, for example, would not catch hourly issues well. Typically, you will want to match the granularity of the time dimension (day/hour/etc) of the line chart with the things that could go wrong, either in the loading process or in the tracked process

If a dashboard is the main product of an analyst, they will generally watch it closely. Therefore, it's probably not necessary for a data engineer to include monitoring in their daily activities in these situations.

## Tools to create dashboards

[Metabase](https://www.metabase.com/), [Looker Studio](https://lookerstudio.google.com/u/0/), or [Streamlit](https://streamlit.io/) are some common tools that you might use to set up dashboards to explore data.

It's worth noting that while many tools are suitable for exploration, different tools enable your organisation to achieve different things. Some organisations use multiple tools for different scopes.
* Tools like Metabase are intended for data democratisation, where the business user can change the dimension, or granularity to answer follow up questions.
* Tools like Looker Data studio, Tableau are intended for minimal interaction curated dashboards that business users can filter and read as-is. They are suitable for organisations where business users are less data literate.
* Tools like Streamlit enable powerful customisations and building of complex apps that are best leveraged by python-first developers, but they do not support self service out of the box.
