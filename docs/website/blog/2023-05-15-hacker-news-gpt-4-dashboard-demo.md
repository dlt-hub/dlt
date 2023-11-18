---
slug: hacker-news-gpt-4-dashboard-demo
title: Understanding how developers view ELT tools using the Hacker News API and GPT-4
authors:
  name: Rahul Joshi
  title: Data Science Intern at dltHub
  url: https://github.com/rahuljo
  image_url: https://avatars.githubusercontent.com/u/28861929?v=4
tags: [hacker news, gpt-4, streamlit]
---
:::info
**TL;DR: We created a Hacker News -> BigQuery `dlt` pipeline to load all comments related to popular ELT keywords and then used GPT-4 to summarize the comments. We now have a live [dashboard](http://34.28.70.28:8502/) that tracks these keywords and an accompanying [GitHub repo](https://github.com/dlt-hub/hacker-news-gpt-4-dashboard-demo) detailing our process.**
:::
## Motivation

To figure out how to improve `dlt`, we are constantly learning about how people approach extracting, loading, and transforming data (i.e. [ELT](https://docs.getdbt.com/terms/elt)). This means we are often reading posts on [Hacker News (HN)](https://news.ycombinator.com/), a forum where many developers like ourselves hang out and share their perspectives. But finding and reading the latest comments about ELT from their website has proved to be time consuming and difficult, even when using [Algolia Hacker News Search](https://hn.algolia.com/) to search.

So we decided to set up a `dlt` pipeline to extract and load comments using keywords (e.g. Airbyte, Fivetran, Matillion, Meltano, Singer, Stitch) from the HN API. This empowered us to then set up a custom dashboard and create one sentence summaries of the comments using GPT-4, which made it much easier and faster to learn about the strengths and weaknesses of these tools. In the rest of this post, we share how we did this for `ELT`. A [GitHub repo](https://github.com/dlt-hub/hacker-news-gpt-4-dashboard-demo) accompanies this blog post, so you can clone and deploy it yourself to learn about the perspective of HN users on anything by replacing the keywords.

## Creating a `dlt` pipeline for Hacker News

For the dashboard to have access to the comments, we needed a data pipeline. So we built a `dlt` pipeline that could load the comments from the [Algolia Hacker News Search API](https://hn.algolia.com/api) into BigQuery. We did this by first writing the logic in Python to request the data from the API and then following [this walkthrough](https://dlthub.com/docs/walkthroughs/create-a-pipeline) to turn it into a `dlt` pipeline.

With our `dlt` pipeline ready, we loaded all of the HN comments corresponding to the keywords from January 1st, 2022 onward.

## Using GPT-4 to summarize the comments

Now that the comments were loaded, we were ready to use GPT-4 to create a one sentence summary for them. We first filtered out any irrelevant comments that may have been loaded using simple heuritics in Python. Once we were left with only relevant comments, we called the `gpt-4` API and prompted it to summarize in one line what the comment was saying about the chosen keywords. If you don't have access to GPT-4 yet, you could also use the `gpt-3.5-turbo` API. 

Since these comments were posted in response to stories or other comments, we fed in the story title and any parent comments as context in the prompt. To avoid hitting rate-limit error and losing all progress, we ran this for 100 comments at a time, saving the results in the CSV file each time. We then built a streamlit app to load and display them in a dashboard. Here is what the dashboard looks like:

![dashboard.png](/img/hn_gpt_dashboard.png)

## Deploying the pipeline, Google Bigquery, and Streamlit app

With all the comments loaded and the summaries generated in bulk, we were ready to deploy this process and have the dashboard update daily with new comments.

We decided to deploy our streamlit app on a GCP VM. To have our app update daily with new data we did the following:
1. We first deployed our `dlt` pipeline using GitHub Actions to allow new comments to be loaded to BigQuery daily
2. We then wrote a Python script that could pull new comments from BigQuery into the VM and we scheduled to run it daily using crontab
3. This Python script also calls the `gpt-4` API to generate summaries only for the new comments
4. Finally, this Python script updates the CSV file that is being read by the streamlit app to create the dashboard. Check it out [here](http://34.28.70.28:8502/)! 
  
Follow the accompanying [GitHub repo](https://github.com/dlt-hub/hacker-news-gpt-4-dashboard-demo) to create your own Hacker News/GPT-4 dashboard.  
