---
slug: practice-api-sources
title: "API playground: Free APIs for personal data projects"
image: https://storage.googleapis.com/dlt-blog-images/blog-api.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [elt, free apis]
---

## Free APIs for Data Engineering

Practicing data engineering is better with real data sources.
If you are considering doing a data engineering project, consider the following:
- Ideally, your data has entities and activities, so you can model dimensions and facts.
- Ideally, the APIs have no auth, so they can be easily tested.
- Ideally, the API should have some use case that you are modelling and showing the data for.
- Ideally, you build end-to-end pipelines to showcase extraction, ingestion, modelling and displaying data.

This article outlines 10 APIs, detailing their use cases, any free tier limitations, and authentication needs.


## Material teaching data loading with dlt:

### Data talks club data engineering zoomcamp
* [Video](https://www.youtube.com/watch?v=oLXhBM7nf2Q)
* [Course step by step](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/workshops/dlt_resources/data_ingestion_workshop.md)
* [Colab notebook](https://colab.research.google.com/drive/1kLyD3AL-tYf_HqCXYnA3ZLwHGpzbLmoj#scrollTo=5aPjk0O3S_Ag&forceEdit=true&sandboxMode=true)

### Data talks club open source spotlight
* [Video](https://www.youtube.com/watch?v=eMbhyOECpcE)
* [Notebook](https://github.com/dlt-hub/dlt_demos/blob/main/spotlight_demo.ipynb)

### Docs
* [Getting started](https://dlthub.com/docs/getting-started)
* [Advanced pipeline tutorial](https://dlthub.com/docs/build-a-pipeline-tutorial)


## APIs Overview

### 1. **PokeAPI**
- **URL:** [PokeAPI](https://pokeapi.co/).
- **Use:** Import Pokémon data for projects on data relationships and stats visualization.
- **Free:** Rate-limited to 100 requests/IP/minute.
- **Auth:** None.

### 2. **REST Countries API**
- **URL:** [REST Countries](https://restcountries.com/).
- **Use:** Access country data for projects analyzing global metrics.
- **Free:** Unlimited.
- **Auth:** None.

### 3. **OpenWeather API**
- **URL:** [OpenWeather](https://openweathermap.org/api).
- **Use:** Fetch weather data for climate analysis and predictive modeling.
- **Free:** Limited requests and features.
- **Auth:** API key.

### 4. **JSONPlaceholder API**
- **URL:** [JSONPlaceholder](https://jsonplaceholder.typicode.com/).
- **Use:** Ideal for testing and prototyping with fake data. Use it to simulate CRUD operations on posts, comments, and user data.
- **Free:** Unlimited.
- **Auth:** None required.

### 5. **Quandl API**
- **URL:** [Quandl](https://www.quandl.com/tools/api).
- **Use:** For financial market trends and economic indicators analysis.
- **Free:** Some datasets require premium.
- **Auth:** API key.

### 6. **GitHub API**
- **URL:** [GitHub API](https://docs.github.com/en/rest)
- **Use:** Analyze open-source trends, collaborations, or stargazers data. You can use it from our [verified sources](https://dlthub.com/docs/dlt-ecosystem/verified-sources/) [repository](https://github.com/dlt-hub/verified-sources/tree/master/sources/github).
- **Free:** 60 requests/hour unauthenticated, 5000 authenticated.
- **Auth:** OAuth or personal access token.

### 7. **NASA API**
- **URL:** [NASA API](https://api.nasa.gov/).
- **Use:** Space-related data for projects on space exploration or earth science.
- **Free:** Rate-limited.
- **Auth:** API key.

### 8. **The Movie Database (TMDb) API**
- **URL:** [TMDb API](https://www.themoviedb.org/documentation/api).
- **Use:** Movie and TV data for entertainment industry trend analysis.
- **Free:** Requires attribution.
- **Auth:** API key.

### 9. **CoinGecko API**
- **URL:** [CoinGecko API](https://www.coingecko.com/en/api).
- **Use:** Cryptocurrency data for market trend analysis or predictive modeling.
- **Free:** Rate-limited.
- **Auth:** None.

### 10. Public APIs GitHub list
- **URL:** [Public APIs list](https://github.com/public-apis/public-apis).
- **Use:** Discover APIs for various projects. A meta-resource.
- **Free:** Varies by API.
- **Auth:** Depends on API.

Each API offers unique insights for data engineering, from ingestion to visualization. Check each API's documentation for up-to-date details on limitations and authentication.

## Example projects

Here are some examples from dlt users and working students:
- A pipeline that pulls data from an API and produces a dashboard in the [dbt blog](https://docs.getdbt.com/blog/serverless-dlt-dbt-stack).
- A [streaming pipeline on GCP](https://dlthub.com/docs/blog/streaming-pubsub-json-gcp) that replaces expensive tools such as Segment/5tran with a setup 50-100x cheaper.
- Another [streaming pipeline on AWS ](https://dlthub.com/docs/blog/dlt-aws-taktile-blog)for a slightly different use case.
- [Orchestrator + email + AI ](https://dlthub.com/docs/blog/dlt-kestra-demo-blog) + Slack to summarize emails.
- Evaluate a frontend tool to show your ability to [deliver end-to-end](https://dlthub.com/docs/blog/dlt-mode-blog).
- An end-to-end [data lineage implementation](https://dlthub.com/docs/blog/dlt-data-lineage) from extraction to dashboard.
- A bird pipeline and the associated schema management that ensures smooth operation [Part 1](https://publish.obsidian.md/lough-on-data/blogs/bird-finder-via-dlt-i), [Part 2](https://publish.obsidian.md/lough-on-data/blogs/bird-finder-via-dlt-ii).
- Japanese language demos [Notion calendar](https://stable.co.jp/blog/notion-calendar-dlt) and [exploring csv to bigquery with dlt](https://soonraah.github.io/posts/load-csv-data-into-bq-by-dlt/).
- Demos with [Dagster](https://dagster.io/blog/dagster-dlt) and [Prefect](https://www.prefect.io/blog/building-resilient-data-pipelines-in-minutes-with-dlt-prefect).

## Showcase your project
If you want your project to be featured, let us know in the [#sharing-and-contributing channel of our community Slack](https://dlthub.com/community).
