---
title: Ensuring data quality
description: Monitoring and testing data quality
keywords: [destination, schema, data, monitoring, testing, quality]
---

# Data quality dashboards

After deploying a `dlt` pipeline, you might ask yourself: How can we know if the data is and remains
high quality?

There are two ways to catch errors:

1. Tests.
1. People [monitoring.](../../running-in-production/monitoring.md)

## Tests

The first time you load data from a pipeline you have built, you will likely want to test it. Plot
the data on time series line charts and look for any interruptions or spikes, which will highlight
any gaps or loading issues.

### Data usage as monitoring

Setting up monitoring is a good idea. However, in practice, often by the time you notice something is wrong through reviewing charts, someone in the business has likely already noticed something is wrong. That is, if there is usage of the data, then that usage will act as a sort of monitoring.

### Plotting main metrics on line charts

In cases where data is not being used much (e.g., only one marketing analyst is using some data alone), then it is a good idea to have them plot their main metrics on "last 7 days" line charts, so it's visible to them that something may be off when they check their metrics.

It's important to think about granularity here. A daily line chart, for example, would not catch hourly issues well. Typically, you will want to match the granularity of the time dimension (day/hour/etc.) of the line chart with the things that could go wrong, either in the loading process or in the tracked process.

If a dashboard is the main product of an analyst, they will generally watch it closely. Therefore, it's probably not necessary for a data engineer to include monitoring in their daily activities in these situations.

## Tools to create dashboards

[Metabase](https://www.metabase.com/), [Looker Studio](https://lookerstudio.google.com/u/0/), and [Streamlit](https://streamlit.io/) are some common tools that you might use to set up dashboards to explore data. It's worth noting that while many tools are suitable for exploration, different tools enable your organization to achieve different things. Some organizations use multiple tools for different scopes:

- Tools like [Metabase](https://www.metabase.com/) are intended for data democratization, where the business user can change the dimension or granularity to answer follow-up questions.
- Tools like [Looker Studio](https://lookerstudio.google.com/u/0/) and [Tableau](https://www.tableau.com/) are intended for minimal interaction curated dashboards that business users can filter and read as-is with limited training.
- Tools like [Streamlit](https://streamlit.io/) enable powerful customizations and the building of complex apps by Python-first developers, but they generally do not support self-service out of the box.

