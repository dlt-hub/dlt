---
slug: contributed-first-pipeline
title: "How I contributed my first data pipeline to the open source."
image:  https://storage.googleapis.com/dlt-blog-images/blog_my_first_data_pipeline.png
authors:
  name: Aman Gupta
  title: Junior Data Engineer
  url: https://github.com/dat-a-man
  image_url: https://dlt-static.s3.eu-central-1.amazonaws.com/images/aman.png
tags: [data ingestion, python sdk, ETL, python data pipelines, Open Source, Developer Tools]
---

Hello, I'm Aman Gupta. Over the past eight years, I have navigated the structured world of civil engineering, but recently, I have found myself captivated by data engineering. This newfound interest has led me to explore part-time data engineering gigs, sparked by a workshop hosted by **`dlt`** in November 2022, facilitated by my former mentor and co-founder of **`dlt`**, Adrian Brudaru.

They offered me a part-time job, and I began with tasks such as testing pipelines and documentation for **`dlt`**. At that time, contributing to dlt’s verified sources seemed like a high-hanging fruit. Yet, as I immersed myself deeper into the field, I started working on thrilling projects. These ranged from deploying pipelines in serverless environments on Google Cloud to tackling complex topics like incremental loading and schema evolution.

An opportunity arose when a client needed data migration from FreshDesk to BigQuery. I crafted a basic pipeline version, initially designed to support my use case. Upon presenting my basic pipeline to the dlt team, Alena Astrakhatseva, a team member, generously offered to review it and refine it into a community-verified source.

![image](https://storage.googleapis.com/dlt-blog-images/blog_my_first_data_pipeline.png)

My first iteration was straightforward—loading data in [replace mode](https://dlthub.com/docs/general-usage/incremental-loading#the-3-write-dispositions). While adequate for initial purposes, a verified source demanded features like [pagination](https://dlthub.com/docs/general-usage/http/overview#explicitly-specifying-pagination-parameters) and [incremental loading](https://dlthub.com/docs/general-usage/incremental-loading). To achieve this, I developed an API client tailored for the Freshdesk API, integrating rate limit handling and pagination:

```py
class FreshdeskClient:
    """
    Client for making authenticated requests to the Freshdesk API. It incorporates API requests with
    rate limit and pagination.
    """
    
    def __init__(self, api_key: str, domain: str):
        # Contains stuff like domain, credentials and base URL.
        pass

    def _request_with_rate_limit(self, url: str, **kwargs: Any) -> requests.Response:
        # Handles rate limits in HTTP requests and ensures that the client doesn't exceed the limit set by the server.
        pass

    def paginated_response(
        self,
        endpoint: str,
        per_page: int,
        updated_at: Optional[str] = None,
    ) -> Iterable[TDataItem]:
        # Fetches a paginated response from a specified endpoint.
        pass
```

To further make the pipeline effective, I developed resources that could handle incremental data loading. This involved creating [resources](https://dlthub.com/docs/general-usage/resource) that used **`dlt`**'s incremental functionality to fetch only new or updated data:

```py
def incremental_resource(
    endpoint: str,
    updated_at: Optional[Any] = dlt.sources.incremental(
        "updated_at", initial_value="2022-01-01T00:00:00Z"
    ),
) -> Generator[Dict[Any, Any], Any, None]:
    """
    Fetches and yields paginated data from a specified API endpoint.
    Each page of data is fetched based on the `updated_at` timestamp
    to ensure incremental loading.
    """

    # Retrieve the last updated timestamp to fetch only new or updated records.
    updated_at = updated_at.last_value

    # Use the FreshdeskClient instance to fetch paginated responses
    yield from freshdesk.paginated_response(
        endpoint=endpoint,
        per_page=per_page,
        updated_at=updated_at,
    )
```

With the steps defined above, I was able to load the data from Freshdesk to BigQuery and use the pipeline in production. Here’s a summary of the steps I followed:

1. Created a Freshdesk API token with sufficient privileges.
1. Created an API client to make requests to the Freshdesk API with rate limit and pagination.
1. Made incremental requests to this client based on the “updated_at” field in the response.
1. Ran the pipeline using the Python script.

To read the full documentation, [please refer to this.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/freshdesk)

While my journey from civil engineering to data engineering was initially intimidating, it has proved to be a profound learning experience. Writing a pipeline with **`dlt`** mirrors the simplicity of a GET request: you request data, yield it, and it flows from the source to its destination. Now, I help other clients integrate **`dlt`** to streamline their data workflows, which has been an invaluable part of my professional growth.

In conclusion, diving into data engineering has not only expanded my technical skill set but has also provided a new lens through which I view challenges and solutions. For those interested in the detailed workings of these pipelines, I encourage exploring **`dlt's`** [GitHub repository](https://github.com/dlt-hub/verified-sources) or diving into the documentation.