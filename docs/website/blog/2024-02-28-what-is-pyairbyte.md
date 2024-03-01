---
slug: what-is-pyairbyte
title: "PyAirbyte - what it is and what it’s not"
image:  https://storage.googleapis.com/dlt-blog-images/pysquid.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [data observability, data pipeline observability]
---

## Intro

Here at dltHub, we work on the python library for data ingestion. So when I heard from Airbyte that they are building a library, I was intrigued and decided to investigate.

# What is PyAirbyte?

PyAirbyte is an interesting Airbyte’s initiative - similar to the one that Meltano had undertook 3 years ago. It provides a convenient way to download and install Airbyte sources and run them locally storing the data in a cache dataset. Users are allowed to then read the data from this cache.


A Python wrapper on the Airbyte source is quite nice and has a feeling close to [Alto](https://github.com/z3z1ma/alto). The whole process of cloning/pip installing the repository, spawning a separate process to run Airbyte connector and read the data via UNIX pipe is hidden behind Pythonic interface.


Note that this library is not an Airbyte replacement - the loaders of Airbyte and the library are very different. The library loader uses pandas.to_sql and sql alchemy and is not a replacement for Airbyte destinations that are available in Open Source Airbyte

# Questions I had, answered

- Can I run Airbyte sources with PyAirbyte? A subset of them.
- Can I use PyAirbyte to run a demo pipeline in a colab notebook? Yes.
- Would my colab demo have a compatible schema with Airbyte? No.
- Is PyAirbyte a replacement for Airbyte? No.
- Can I use PyAirbyte to develop or test during development Airbyte sources? No.
- Can I develop pipelines with PyAirbyte? no

# In conclusion

In wrapping up, it's clear that PyAirbyte is a neat little addition to the toolkit for those of us who enjoy tinkering with data in more casual or exploratory settings. I think this is an interesting initiative from Airbyte that will enable new usage patterns.

### Want to discuss?

[Join our slack community](https://dlthub.com/community) to take part in the conversation.