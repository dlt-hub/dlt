---
title: Exploring the data
description: Exploring data that has been loaded
keywords: [exploring, loaded data, data quality]
---

# Exploring the data

## Data Quality Dashboards

After deploying a `dlt` pipeline, you might ask yourself: how can we know if the data is and remains high quality?

There are two ways to catch errors:
1. Tests
2. People [monitoring](../running-in-production/monitoring.md)

### Data usage as monitoring

Setting up monitoring is a good idea. However, in practice, often by the time you notice something is wrong through reviewing charts, someone in the business has likely already noticed something is wrong. That is, if there is usage of the data, then that usage will act as sort of monitoring.

### Plotting main metrics on line charts

In cases where data is not being used that much (e.g. only one marketing analyst is using some data alone), then it is a good idea to have them plot their main metrics on "last 7 days" line charts, so it's visible to them that something may be off when they check their metrics. 

It's important to think about granualrity here. A daily line chart, for example, would not catch hourly issues well. Typically, you will want to match the granularity of the time dimension (day/hour/etc) of the line chart with the things that could go wrong.

If a dashboard is the main product of an analyst, they will generally watch it closely. Therefore, it's probably not necessary for a data engineer to include monitoring in their daily activities in these situations.

## Tools to create dashboards

[Metabase](https://www.metabase.com/), [Looker Studio](https://lookerstudio.google.com/u/0/), or [Streamlit](https://streamlit.io/) are good tools that you might use to set up dashboards to explore data.