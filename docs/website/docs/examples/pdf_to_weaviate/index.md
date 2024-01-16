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

```shell
pip install PyPDF2
```

## Example code

<!--@@@DLT_SNIPPET_START ./code/pdf_to_weaviate-snippets.py::pdf_to_weaviate -->
```py
import os

import dlt
from dlt.destinations.impl.weaviate import weaviate_adapter
from PyPDF2 import PdfReader


@dlt.resource(selected=False)
def list_files(folder_path: str):
    folder_path = os.path.abspath(folder_path)
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        yield {
            "file_name": filename,
            "file_path": file_path,
            "mtime": os.path.getmtime(file_path)
        }


@dlt.transformer(primary_key="page_id", write_disposition="merge")
def pdf_to_text(file_item, separate_pages: bool = False):
    if not separate_pages:
        raise NotImplementedError()
    # extract data from PDF page by page
    reader = PdfReader(file_item["file_path"])
    for page_no in range(len(reader.pages)):
        # add page content to file item
        page_item = dict(file_item)
        page_item["text"] = reader.pages[page_no].extract_text()
        page_item["page_id"] = file_item["file_name"] + "_" + str(page_no)
        yield page_item

pipeline = dlt.pipeline(
    pipeline_name='pdf_to_text',
    destination='weaviate'
)

# this constructs a simple pipeline that: (1) reads files from "invoices" folder (2) filters only those ending with ".pdf"
# (3) sends them to pdf_to_text transformer with pipe (|) operator
pdf_pipeline = list_files("assets/invoices").add_filter(
    lambda item: item["file_name"].endswith(".pdf")
) | pdf_to_text(separate_pages=True)

# set the name of the destination table to receive pages
# NOTE: Weaviate, dlt's tables are mapped to classes
pdf_pipeline.table_name = "InvoiceText"

# use weaviate_adapter to tell destination to vectorize "text" column
load_info = pipeline.run(
    weaviate_adapter(pdf_pipeline, vectorize="text")
)
row_counts = pipeline.last_trace.last_normalize_info
print(row_counts)
print("------")
print(load_info)
```
<!--@@@DLT_SNIPPET_END ./code/pdf_to_weaviate-snippets.py::pdf_to_weaviate -->

We start with a simple resource that lists files in specified folder. To that we add a **filter** function that removes all files that are not pdfs.

To parse PDFs we use [PyPDF](https://pypdf2.readthedocs.io/en/3.0.0/user/extract-text.html) and return each page from a given PDF as separate data item.

Parsing happens in `@dlt.transformer` which receives data from `list_files` resource. It splits PDF into pages, extracts text and yields pages separately
so each PDF will correspond to many items in Weaviate `InvoiceText` class. We set the primary key and use merge disposition so if the same PDF comes twice
we'll just update the vectors, and not duplicate.

Look how we pipe data from `list_files` resource (note that resource is deselected so we do not load raw file items to destination) into `pdf_to_text` using **|** operator.

Just before load, the `weaviate_adapter` is used to tell `weaviate` destination which fields to vectorize.

Now it is time to query our documents.
<!--@@@DLT_SNIPPET_START ./code/pdf_to_weaviate-snippets.py::pdf_to_weaviate_read-->
```py
import weaviate

client = weaviate.Client("http://localhost:8080")
# get text of all the invoices in InvoiceText class we just created above
print(client.query.get("InvoiceText", ["text", "file_name", "mtime", "page_id"]).do())
```
<!--@@@DLT_SNIPPET_END ./code/pdf_to_weaviate-snippets.py::pdf_to_weaviate_read-->

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
