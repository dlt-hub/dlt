---
title: Use an existing source
description: Use an existing source - how to leverage what others did before you
keywords: [Use an existing a data source]
---

# How to use an existing verified source

While dlt is primarily a pipeline building tool, on dlthub we host some common pipelines and also encourage the community to build and distribute high quality pipelines.
Read on.

## What is a verified source?

A verified source is a source which we test daily in our verified sources repository.
You can find them [here](../dlt-ecosystem/verified-sources).

Quality guarantees:
- We reviewed both the code and generated datasets and wrote demos and docs for them
- Many of them already in production.
- All of them are constantly tested on real data and distributed as simple Python code so they can be easily customized or hacked.

## Distribution, or how add sources to your pipelines

To get the code from [this](https://github.com/dlt-hub/verified-sources) repository, follow this guide to [use the dlt init command](../walkthroughs/add-a-verified-source).
This will create dlt scaffolding for credentials, configuration and versioning,
and copy the code from the verified sources repository, templated to the chosen destination.

Versioning and updates: Want to get the updated version of a pipeline? dlt init command done in the same location will read
the .dlt/version file and will upgrade your pipeline to the latest version. If you have
made changes to your local version, you will need to merge them.

## Contribution

To contribute to the verified sources repository, please read the [source contribution guide](https://github.com/dlt-hub/verified-sources/blob/master/CONTRIBUTING.md).

## Requesting a new source

To request a new source, open an issue [here](https://github.com/dlt-hub/verified-sources/issues/new?template=source-request.md) or upvote any existing request for that source.

## Request an extension

Something missing from a source? You can request it [here](https://github.com/dlt-hub/verified-sources/issues/new?template=extend-a-source.md).

## Customisation support

Do you want to extend or modify a source? consider contributing back,
so your updates are merged into the distributed version and carried forward in future updates,
preserving later compatibility with your version and allowing you to in turn
receive community updates.

