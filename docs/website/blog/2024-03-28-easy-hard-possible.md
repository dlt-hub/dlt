---
slug: yes-code-elt
title: "Yes code ELT: dlt make easy things easy, and hard things possible"
image:  https://storage.googleapis.com/dlt-blog-images/easy-things-easy.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [full code etl, yes code etl, etl, pythonic]
---

# There's code and then there's code

The concept of simplicity and automation in a programming language is not new.
Perl scripting language had the motto "Perl makes easy things easy and hard things possible".

The reason for this motto, was the difficulty of working with C, which requires more manual
handling of resources and also a compilation step.

Perl scripts could be written and executed rapidly, making it ideal for tasks that needed
quick development cycles. This ease of use and ability to handle complex tasks without
cumbersome syntax made Perl incredibly popular in its heyday.

Perl was introduced as a scripting language that emphasized getting things done.
It was created as a practical extraction and reporting tool, which quickly found
its place in system administration, web development, and network programming.

## History repeats, Python is a language for humans

![human-building](https://storage.googleapis.com/dlt-blog-images/easy-things-easy.png)

Python took the philosophy of making programming more accessible and human-friendly even further.
Guido van Rossum created Python with the goal of removing the drudgery from coding, choosing to
prioritize readability and simplicity. This design philosophy makes Python an intuitive language
not just for seasoned developers but for beginners as well. Its syntax is clean and expressive,
allowing developers to write fewer lines of code for tasks that would require more in Perl or other languages.
Python's extensive standard library, along with its powerful data structures, contribute to its
ability to handle complex applications with ease.

Python's widespread adoption across various domains, from web development to data science and machine
learning, is largely attributed to its accessibility.

Its simple syntax resembles natural language, which lowers the barrier to entry for programming.
Compared to Perl, Python offers an even more organized and readable approach to coding,
making it an ideal teaching language that prepares new developers for future challenges in software development.

And just like perl, it's used for data extraction and visualisation - but now it's done by normie humans,
not sysadmins or devs.

# dlt makes easy things fast, and hard things accessible

Following the principles of Perl and Python, dlt aimed to simplify the data engineering process.
dlt focuses on making the extraction and loading of data as straightforward as possible.

## dlt makes easy things fast

Starting from a simple abstraction like `pipeline.run(data, table_name="table")`,
where data can be any iterable such as a generator or dataframe, dlt enables robust loading.
Here is what the above function does, so you don't have to.
- It will (optionally) unpack nested lists into separate tables with generated join keys, and flatten nested dictionaries into a main row.
- If given a generator, it will consume it via microbatching, buffering to disk or external drives, never running out of memory (customisable).
- it will create "extract packages" of extracted data so if the downstream steps fail, it can resume/retry later.
- It will normalise the data into a shape that naturally fits the database (customisable).
- It will create "load packages" of normalised data so if the downstream steps fail, it can retry later.
- It infers and loads with the correct data types, for example from ISO timestamp strings (configurable).
- It can accept different types of write dispositions declaratively such as 'append', 'merge' and 'replace'.
- It will evolve the schema if we load a second time something with new columns, and it can alert the schema changes.
- It will even create type variant columns if data types change (and alert if desired).
- Or you can stop the schema from evolving and use the inferred schema or a modified one as a data contract
- It will report load packages associated with new columns, enabling passing down column level lineage

That's a lot of development and maintenance pain solved only at its simplest. You could say, the dlt loader doesn't break, as long as it encounters common data types.
If an obscure type is in your data, it would need to be added to dlt or converted beforehand.

### From robust loading to robust extraction

Building on the simple loading abstraction, dlt is more than a tool for simple things.

The next step in dlt usage is to leverage it for extraction. dlt offers the concepts of 'source' and 'resource',
A resource is the equivalent of a single data source, while a source is the group we put resources in to bundle them for usage.

For example, an API extractor from a single API with multiple endpoints, would be built as a source with multiple resources.

Resources enable you to easily configure how the data in that resource is loaded. You can create a resource by
decorating a method with the '@resource' decorator, or you can generate them dynamically.

Examples of dynamic resources
- If we have an api with multiple endpoints, we can put the endpoints in a list and iterate over it to generate resources
- If we have an endpoint that gives us datapoints with different schemas, we could split them by a column in the data.
- Similarly, if we have a webhook that listens to multiple types of events, it can dispatch each event type to its own table based on the data.
- Or, if we want to shard a data stream into day-shards, we could append a date suffix in the resource name dynamically.

Once we group resources into a source, we can run them together (or, we could still run the resources independently)

Examples of reasons to group resources into sources.
- We want to run (load) them together on the same schedule
- We want to configure them together or keep their schemas together
- They represent a single API and we want to publish them in a coherent, easy to use way.

So what are the efforts you spare when using dlt here?
- A source can function similar to a class, but simpler, encouraging code reuse and simplicity.
- Resources offer more granular configuration options
- Resources can also be transformers, passing data between them in a microbatched way enabling patters like enrichments or list/detail endpoints.
- Source schemas can be configured with various options such as pushing down top level columns into nested structures
- dlt's requests replacement has built in retries for non-permanent error codes. This safeguards the progress of long extraction jobs that could otherwise break over and over (if retried as a whole) due to network or source api issues.


### What else does dlt bring to the table?

Beyond the ease of data extraction and loading, dlt introduces several advanced features that further simplify data engineering tasks:

Asynchronous operations: dlt harnesses the power of asynchronous programming to manage I/O-bound and network operations efficiently. This means faster data processing, better resource utilization, and more responsive applications, especially when dealing with high volumes of data or remote data sources.

Flexible destinations and reverse ETL: dlt isn't just about pulling data in; it's about sending it where it needs to go. Whether it's a SQL database, a data lake, or a cloud-based storage solution or a custom reverse etl destination, dlt provides the flexibility to integrate with various destinations.

Optional T in ETL: With dlt, transformations are not an afterthought but a core feature. You can define transformations as part of your data pipelines, ensuring that the data is not just moved but refined, enriched, and shaped to fit your analytical needs. This capability allows for more sophisticated data modeling and preparation tasks to be streamlined within your ELT processes.

Data quality and observability: dlt places a strong emphasis on data quality and observability. It includes features for schema evolution tracking, data type validation, and error handling, and data contracts, which are critical for maintaining the integrity of your data ecosystem. Observability tools integrated within dlt help monitor the health and performance of your pipelines, providing insights into data flows, bottlenecks, and potential issues before they escalate.

Community and ecosystem: One of the most significant advantages of dlt is its growing community and ecosystem. Similar to Python, dlt benefits from contributions that extend its capabilities, including connectors, plugins, and integrations. This collaborative environment ensures that dlt remains at the forefront of data engineering innovation, adapting to new challenges and opportunities.

In essence, dlt is not just a tool but a comprehensive one stop shop that addresses the end-to-end needs of modern data ingestion. By combining the simplicity of Python with the robustness of enterprise-grade tools, dlt democratizes data engineering, making it accessible to a broader audience. Whether you're a data scientist, analyst, or engineer, dlt empowers you to focus on what matters most: deriving insights and value from your data.

## Conclusion

As Perl and Python have made programming more accessible, dlt is set to transform data engineering by making sophisticated data operations accessible to all. This marks a significant shift towards the democratization of technology, enabling more individuals to contribute to and benefit from the digital landscape. dlt isn't just about making easy things fast and hard things accessible; it's about preparing a future where data engineering becomes an integral part of every data professional's toolkit.
