---
title: Load PDFs to Weaviate
description: Extract text from PDF and load it into a vector database
keywords: [pdf, weaviate, vector store, vector database, ]
---

import Header from '../_examples-header.md';

<Header
    intro="This example demonstrates how to extract text from PDF files and load it into Weaviate vector database."
    slug="pdf_to_weaviate"
    destination="weaviate"
    run_file="pdf_to_weaviate" />

Additionally we'll use PyPDF2 to extract text from PDFs. Make sure you have it installed:

```sh
pip install PyPDF2
```

## Example code

<!--@@@DLT_SNIPPET ./code/pdf_to_weaviate-snippets.py::pdf_to_weaviate -->


We start with a simple resource that lists files in specified folder. To that we add a **filter** function that removes all files that are not pdfs.

To parse PDFs we use [PyPDF](https://pypdf2.readthedocs.io/en/3.0.0/user/extract-text.html) and return each page from a given PDF as separate data item.

Parsing happens in `@dlt.transformer` which receives data from `list_files` resource. It splits PDF into pages, extracts text and yields pages separately
so each PDF will correspond to many items in Weaviate `InvoiceText` class. We set the primary key and use merge disposition so if the same PDF comes twice
we'll just update the vectors, and not duplicate.

Look how we pipe data from `list_files` resource (note that resource is deselected so we do not load raw file items to destination) into `pdf_to_text` using **|** operator.

Just before load, the `weaviate_adapter` is used to tell `weaviate` destination which fields to vectorize.

Now it is time to query our documents.
<!--@@@DLT_SNIPPET ./code/pdf_to_weaviate-snippets.py::pdf_to_weaviate_read-->


Above we provide URL to local cluster. We also use `contextionary` to vectorize data. You may find information on our setup in links below.

:::tip

Change the destination to `duckdb` if you do not have access to Weaviate cluster or not able to run it locally.

:::

Learn more:

- [Setup Weaviate destination - local or cluster](dlt-ecosystem/destinations/weaviate.md).
- [Connect the transformers to the resources](general-usage/resource#feeding-data-from-one-resource-into-another)
to load additional data or enrich it.
- [Transform your data before loading](general-usage/resource#customize-resources) and see some
 [examples of customizations like column renames and anonymization](general-usage/customising-pipelines/renaming_columns).
