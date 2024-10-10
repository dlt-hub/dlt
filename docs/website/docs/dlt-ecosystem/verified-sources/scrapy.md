---
title: Scrapy
description: dlt verified source for Scraping using scrapy
keywords: [scraping, scraping verified source, scrapy]
---

# Scrapy

This verified source utilizes Scrapy, an open-source and collaborative framework for web scraping.
Scrapy enables efficient extraction of required data from websites.

## Setup guide

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init scraping duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/scraping_pipeline.py)
   with Scrapy as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on
[how to add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credentials

1. The `config.toml`, looks like:
   ```toml
   # put your configuration values here
   [sources.scraping]
   start_urls = ["URL to be scraped"] # please set me up!
   start_urls_file = "/path/to/urls.txt" # please set me up!
   ```
   > When both `start_urls` and `start_urls_file` are provided, they will be merged and deduplicated
   > to ensure Scrapy gets a unique set of start URLs.

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can securely
   store your access tokens and other sensitive information. It's important to handle this
   file with care and keep it safe.

1. Next, follow the [destination documentation](../../dlt-ecosystem/destinations) instructions to
   add credentials for your chosen destination, ensuring proper routing of your data to the final
   destination.
For more information, read [Secrets and Configs.](../../general-usage/credentials)

## Run the pipeline

In this section, we demonstrate how to use the `MySpider` class defined in "scraping_pipeline.py" to
scrape data from "https://quotes.toscrape.com/page/1/".

1. Start by configuring the `config.toml` as follows:

   ```toml
   [sources.scraping]
   start_urls = ["https://quotes.toscrape.com/page/1/"] # please set me up!
   ```

   Additionally, set destination credentials in `secrets.toml`, as [discussed](#add-credentials).

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```sh
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python scraping_pipeline.py
   ```

## Customization



### Create your own pipeline

If you wish to create your data pipeline, follow these steps:

1. The first step requires creating a spider class that scrapes data
   from the website. For example, the class `Myspider` below scrapes data from
   URL: "https://quotes.toscrape.com/page/1/".

   ```py
   class MySpider(Spider):
       def parse(self, response: Response, **kwargs: Any) -> Any:
           # Iterate through each "next" page link found
           for next_page in response.css("li.next a::attr(href)"):
               if next_page:
                   yield response.follow(next_page.get(), self.parse)

           # Iterate through each quote block found on the page
           for quote in response.css("div.quote"):
               # Extract the quote details
               result = {
                   "quote": {
                       "text": quote.css("span.text::text").get(),
                       "author": quote.css("small.author::text").get(),
                       "tags": quote.css("div.tags a.tag::text").getall(),
                   },
               }
               yield result

   ```

   > Define your own class tailored to the website you intend to scrape.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="scrapy_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., bigquery, redshift)
       dataset_name="scrapy_data",  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our
   [documentation](../../general-usage/pipeline).

1. To run the pipeline with customized scrapy settings:

   ```py
   run_pipeline(
       pipeline,
       MySpider,
       # you can pass scrapy settings overrides here
       scrapy_settings={
           # How many sub pages to scrape
           # https://docs.scrapy.org/en/latest/topics/settings.html#depth-limit
           "DEPTH_LIMIT": 100,
           "SPIDER_MIDDLEWARES": {
               "scrapy.spidermiddlewares.depth.DepthMiddleware": 200,
               "scrapy.spidermiddlewares.httperror.HttpErrorMiddleware": 300,
           },
           "HTTPERROR_ALLOW_ALL": False,
       },
       write_disposition="append",
   )
   ```

   In the above example, scrapy settings are passed as a parameter. For more information about
   scrapy settings, please refer to the
   [Scrapy documentation](https://docs.scrapy.org/en/latest/topics/settings.html).

1. To limit the number of items processed, use the "on_before_start" function to set a limit on
   the resources the pipeline processes. For instance, setting the resource limit to two allows
   the pipeline to yield a maximum of two resources.

   ```py
   def on_before_start(res: DltResource) -> None:
       res.add_limit(2)

   run_pipeline(
       pipeline,
       MySpider,
       batch_size=10,
       scrapy_settings={
           "DEPTH_LIMIT": 100,
           "SPIDER_MIDDLEWARES": {
               "scrapy.spidermiddlewares.depth.DepthMiddleware": 200,
               "scrapy.spidermiddlewares.httperror.HttpErrorMiddleware": 300,
           }
       },
       on_before_start=on_before_start,
       write_disposition="append",
   )
   ```

1. To create a pipeline using Scrapy host, use `create_pipeline_runner` defined in
   `helpers.py`. As follows:

   ```py
   scraping_host = create_pipeline_runner(pipeline, MySpider, batch_size=10)
   scraping_host.pipeline_runner.scraping_resource.add_limit(2)
   scraping_host.run(dataset_name="quotes", write_disposition="append")
   ```

