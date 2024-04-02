---
slug: yes-code-elt
title: "YES code ELT: dlt make simple things fast, and hard things accessible"
image:  https://storage.googleapis.com/dlt-blog-images/reverse-etl.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [reverse etl, pythonic]
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
Here is an enumeration of things that the above function does, so you don't have to.
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

Once we group resources into a source, we can un them together (or, we could still run the resources independently)

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

### What else?

- async
- destinations
-















This approach allows data teams to quickly move data across systems and transform it in a way that's ready for analysis, without getting bogged down by the complexities traditionally associated with these tasks.

The essence of DLT is to democratize data engineering, making it accessible to not only data engineers but also analysts and scientists. By abstracting the more complex aspects of data processing, DLT tools enable users to focus on deriving insights rather than managing infrastructure or writing extensive code. This accessibility is crucial in today's data-driven world, where the ability to quickly adapt and process information can significantly impact decision-making and innovation.

In conclusion, just as Perl and Python have made programming more accessible, DLT is transforming data engineering by making easy things fast and hard things accessible. This evolution reflects a broader trend towards democratizing technology, ensuring that more people can contribute to and benefit from the digital landscape.





