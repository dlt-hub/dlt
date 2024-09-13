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
* DTC Learners showcase   (review again)

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

### 11. News API
- **URL**: [News API](https://newsapi.ai/).
- **Use**: Get datasets containing current and historic news articles.
- **Free**: Access to current news articles.
- **Auth**: API-Key.

### 12. Exchangerates API
- **URL**: [Exchangerate API](https://exchangeratesapi.io/).
- **Use**: Get realtime, intraday and historic currency rates.
- **Free**: 250 monthly requests.
- **Auth**: API-Key.

### 13. Spotify API
- **URL**: [Spotify API](https://developer.spotify.com/documentation/web-api).
- **Use**: Get spotify content and metadata about songs.
- **Free**: Rate limit.
- **Auth**: API-Key.

### 14. Football API
- **URL**: [FootBall API](https://www.api-football.com/).
- **Use**: Get information about Football Leagues & Cups.
- **Free**: 100 requests/day.
- **Auth**: API-Key.

### 15. Yahoo Finance API
- **URL**: [Yahoo Finance API](https://rapidapi.com/sparior/api/yahoo-finance15/details).
- **Use**: Access a wide range of financial data.
- **Free**: 500 requests/month.
- **Auth**: API-Key.

### 16. Basketball API

- URL: [Basketball API](https://www.api-basketball.com/).
- Use: Get information about basketball leagues & cups.
- Free: 100 requests/day.
- Auth: API-Key.

### 17. NY Times API

- URL: [NY Times API](https://developer.nytimes.com/apis).
- Use: Get info about articles, books, movies and more.
- Free: 500 requests/day or 5 requests/minute.
- Auth: API-Key.

### 18. Spoonacular API

- URL: [Spoonacular API](https://spoonacular.com/food-api).
- Use: Get info about ingredients, recipes, products and menu items.
- Free: 150 requests/day and 1 request/sec.
- Auth: API-Key.

### 19. Movie database alternative API

- URL: [Movie database alternative API](https://rapidapi.com/rapidapi/api/movie-database-alternative/pricing).
- Use: Movie data for entertainment industry trend analysis.
- Free: 1000 requests/day and 10 requests/sec.
- Auth: API-Key.

### 20. RAWG Video games database API

- URL: [RAWG Video Games Database](https://rawg.io/apidocs).
- Use: Gather video game data, such as release dates, platforms, genres, and reviews.
- Free: Unlimited requests for limited endpoints.
- Auth: API key.

### 21. Jikan API

- **URL:** [Jikan API](https://jikan.moe/).
- **Use:** Access data from MyAnimeList for anime and manga projects.
- **Free:** Rate-limited.
- **Auth:** None.

### 22. Open Library Books API

- URL: [Open Library Books API](https://openlibrary.org/dev/docs/api/books).
- Use: Access data about millions of books, including titles, authors, and publication dates.
- Free: Unlimited.
- Auth: None.

### 23. YouTube Data API

- URL: [YouTube Data API](https://developers.google.com/youtube/v3/docs/search/list).
- Use: Access YouTube video data, channels, playlists, etc.
- Free: Limited quota.
- Auth: Google API key and OAuth 2.0.

### 24. Reddit API

- URL: [Reddit API](https://www.reddit.com/dev/api/).
- Use: Access Reddit data for social media analysis or content retrieval.
- Free: Rate-limited.
- Auth: OAuth 2.0.

### 25. World Bank API

- URL: [World bank API](https://documents.worldbank.org/en/publication/documents-reports/api).
- Use: Access economic and development data from the World Bank.
- Free: Unlimited.
- Auth: None.

Each API offers unique insights for data engineering, from ingestion to visualization. Check each API's documentation for up-to-date details on limitations and authentication.

## Using the above sources

You can create a pipeline for the APIs discussed above by using `dlt's` REST API source. Let’s create a PokeAPI pipeline as an example. Follow these steps:

1. Create a Rest API source:
    
   ```sh
   dlt init rest_api duckdb
   ```
    
2. The following directory structure gets generated:
    
   ```sh
   rest_api_pipeline/
   ├── .dlt/
   │   ├── config.toml          # configs for your pipeline
   │   └── secrets.toml         # secrets for your pipeline
   ├── rest_api/                # folder with source-specific files
   │   └── ...
   ├── rest_api_pipeline.py     # your main pipeline script
   ├── requirements.txt         # dependencies for your pipeline
   └── .gitignore               # ignore files for git (not required)
   ```
    
3. Configure the source in `rest_api_pipeline.py`:
    
   ```py 
   def load_pokemon() -> None:
       pipeline = dlt.pipeline(
           pipeline_name="rest_api_pokemon",
           destination='duckdb',
           dataset_name="rest_api_data",
       )
   
       pokemon_source = rest_api_source(
           {
               "client": {
                   "base_url": "https://pokeapi.co/api/v2/",
               },
               "resource_defaults": {
                   "endpoint": {
                       "params": {
                           "limit": 1000,
                       },
                   },
               },
               "resources": [
                   "pokemon",
                   "berry",
                   "location",
               ],
           }
       )
   
    ```
    
For a detailed guide on creating a pipeline using the Rest API source, please read the Rest API source [documentation here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api).

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

## DTC learners showcase
Check out the incredible projects from our DTC learners:

1. [e2e_de_project](https://github.com/scpkobayashi/e2e_de_project/tree/153d485bba3ea8f640d0ccf3ec9593790259a646) by [scpkobayashi](https://github.com/scpkobayashi).
2. [de-zoomcamp-project](https://github.com/theDataFixer/de-zoomcamp-project/tree/1737b6a9d556348c2d7d48a91e2a43bb6e12f594) by [theDataFixer](https://github.com/theDataFixer).
3. [data-engineering-zoomcamp2024-project2](https://github.com/pavlokurochka/data-engineering-zoomcamp2024-project2/tree/f336ed00870a74cb93cbd9783dbff594393654b8) by [pavlokurochka](https://github.com/pavlokurochka).
4. [de-zoomcamp-2024](https://github.com/snehangsude/de-zoomcamp-2024) by [snehangsude](https://github.com/snehangsude).
5. [zoomcamp-data-engineer-2024](https://github.com/eokwukwe/zoomcamp-data-engineer-2024) by [eokwukwe](https://github.com/eokwukwe).
6. [data-engineering-zoomcamp-alex](https://github.com/aaalexlit/data-engineering-zoomcamp-alex) by [aaalexlit](https://github.com/aaalexlit).
7. [Zoomcamp2024](https://github.com/alfredzou/Zoomcamp2024) by [alfredzou](https://github.com/alfredzou).
8. [data-engineering-zoomcamp](https://github.com/el-grudge/data-engineering-zoomcamp) by [el-grudge](https://github.com/el-grudge).

Explore these projects to see the innovative solutions and hard work the learners have put into their data engineering journeys!

## Showcase your project
If you want your project to be featured, let us know in the [#sharing-and-contributing channel of our community Slack](https://dlthub.com/community).