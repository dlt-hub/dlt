---
slug: ga4-internal-dashboard-demo
title: Internal Dashboard for Google Analytics 4
authors:
  name: Rahul Joshi
  title: Data Science Intern at dltHub
  url: https://github.com/rahuljo
  image_url: https://avatars.githubusercontent.com/u/28861929?v=4
tags: [internal dashboard, google analytics 4, streamlit]
---

:::info
**TL;DR: As of last week, there is a dlt pipeline that loads data from Google Analytics 4 (GA4). We’ve been excited about GA4 for a while now, so we decided to build some internal dashboards and show you how we did it.**
:::

### Why GA4?

We set out to build an internal dashboard demo based on data from Google Analytics (GA4). Google announced that they will stop processing hits for Universal Analytics (UA) on July 1st, 2023, so many people are now having to figure out how to set up analytics on top of GA4 instead of UA and struggling to do so. For example, in UA, a session represents the period of time that a user is actively engaged on your site, while in GA4, a `session_start` event generates a session ID that is associated with all future events during the session. Our hope is that this demo helps you begin this transition!

### Initial explorations

We decided to make a dashboard that helps us better understand data attribution for our blog posts (e.g. [As DuckDB crosses 1M downloads / month, what do its users do?](./2023-03-09-duckdb-1M-downloads-users.mdx)). Once we got our [credentials](https://dlthub.com/docs/general-usage/credentials) working, we then used the GA4 `dlt` pipeline to load data into a DuckDB instance on our laptop. This allowed us to figure out what requests we needed to make to get the necessary data to show the impact of each blog post (e.g. across different channels, what was the subsequent engagement with our docs, etc). We found it helpful to use [GA4 Query Explorer](https://ga-dev-tools.google/ga4/query-explorer/) for this, following [these steps](https://github.com/dlt-hub/ga4-internal-dashboard-demo/tree/main/intial-explorations).

### Internal dashboard

![Dashboard 1](/img/g4_dashboard_screen_grab_1.jpg) ![Dashboard 2](/img/g4_dashboard_screen_grab_2.jpg)

With the data locally locally, we were able to build the dashboard on our system using Streamlit. You can also do this on your system by simply cloning [this repo](https://github.com/dlt-hub/ga4-internal-dashboard-demo) and following the steps listed [here](https://github.com/dlt-hub/ga4-internal-dashboard-demo/tree/main/intial-explorations).  

After having the pipeline and the dashboard set up just how we liked it, we were now ready to deploy it.

### Deploying the data warehouse

We decided to deploy our Streamlit app on a [Google Cloud VM instance](https://cloud.google.com/compute). This means that instead of storing the data locally, it would need to be in a location that could be accessed by the Streamlit app. Hence we decided to load the data onto a PostgreSQL database in the VM. [See here](https://github.com/dlt-hub/ga4-internal-dashboard-demo/tree/main/internal-dashboards) for more details on our process.

### Deploying the `dlt` pipeline with GitHub Actions

Once we had our data warehouse set up, we were ready to deploy the pipeline. We then followed the [deploy a pipeline](https://dlthub.com/docs/walkthroughs/deploy-a-pipeline) walkthrough to configure and deploy a pipeline that will load the data daily onto our data warehouse.

### Deploying the dashboard

We finally deployed our Streamlit app on our Google Cloud VM instance by following [these steps](https://github.com/dlt-hub/ga4-internal-dashboard-demo/tree/main/internal-dashboards).

### Enjoy this blog post? Give `dlt` a ⭐ on [GitHub](https://github.com/dlt-hub/dlt) 🤜🤛