---
slug: data-product-docs
title: "The role of docs in data products"
image: /img/parrot-baby.gif
authors:
  name: Adrian Brudaru
  title: Open source data engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [data product, data as a product, data documentation, data product documentation]
---


# Lip service to docs

We often see people talk about data products or data as a product and they usually tackle the topics of:

- Concepts and how to think about data products
- Users and producers: Roles, responsibilities and blame around data products
- Data: Data quality and governance that is part of those products, data as a product
- Code: The code or technology powering the pipelines
- Infra: the infrastructure data products are run on

What we do not see is any practical advices or examples of how to implement these products. While the concepts often cover definition of data products as something with a use case, they fail to discuss the importance a user manual, or documentation.

# The role of the user manual

### So what is a data product?

A data product is a self-contained piece of data-powered software that serves a single use case. For example, it could be a pipeline that loads Salesforce data to Snowflake, or it could be a ML model hosted behind an api.

### What makes a data pipeline a data product?

The term product assumes more than just some code. A product is something that you can pick up and use and is thus different from someone’s python spaghetti.

For example, a product is:

- Reusable: The first thing needed here is a **solid documentation** that will enable other users to understand how to use the product
- Robust: Nothing kills the trust in data faster than bad numbers. To be maintainable, code must be simple, explicit, tested ****and **documented** :)

![Who the duck wrote this garbage??? Ah nvm… it was me…](/img/parrot-baby.gif)

- Secure: Everything from credentials to data should be secure. Depending on their requirements, that could mean keeping data on your side (no 3rd party tools), controlling data access, using SOC2 compliant credential stores, etc.
- Observable: Is it working? how do you know? you can automate a large part of this question by monitoring volume of data and schema changes, or whatever other important run parameters or changes you might have.
- Operationizable: Can we use it? do we need a rocket scientist, or can [little Bobby Tables](https://xkcd.com/327/) use it? That will largely depend on docs and the product itself

### So what is a data product made of?

Let’s look at the high level components

1. Structured data: A data product needs data. The code and data are tightly connected - a ML model or data pipeline cannot be trained or operate without data. Why structured? because our code will expect a structured input, so the data is going to be either explicitly structured upfront (”schema on write”), or structured implicitly on read  (”schema on read”).
2. Code
3. Docs for usage - Without a user manual, a complex piece of code is next to unusable.

### And where are the docs needed?

We will need top level docs, plus some for each of the parts described above.

1. Top level: **Purpose** of existence for the data product. The code describes the what and how - So focus the readme on the “why” and top level “what”. Similar to a problem description, this document explains what problem your product solves and enables the reader to understand the cost and impact it might have to use your product.
2. Structured data:
    1. A **data dictionary** enables users to gain literacy on the dataset.
    2. **Maintenance info:** information about the source, schema, tests, responsible person, how to monitor, etc.
3. Code & Usage manual: This one is harder. You need to convey a lot of information in an effective manner, and depending on who your user is, you need to convey that information in a different format. According to the **[leading brains on the topic of docs](https://www.writethedocs.org/videos/eu/2017/the-four-kinds-of-documentation-and-why-you-need-to-understand-what-they-are-daniele-procida/)**, these are the **4 relevant formats you should consider.** They will enable you to write high quality, comprehensive and understandable docs that cover the user’s intention.
    - learning-oriented tutorials
    - goal-oriented how-to guides
    - understanding-oriented discussions
    - information-oriented reference material

# dlt pipeline as an example

Dlt is a library that enables us to build data products. By building with dlt, you benefit from simple code and accessible docs.
Here’s how a dlt pipeline documentation could be structured:

- Top level: [intro doc](https://dlthub.com/docs/intro)
- Data dictionary: Schema info belongs to each pipeline and can be found [here](https://dlthub.com/docs/blog/dlt-lineage-support). To get sample values, you could write a query. We plan to enable its generation in the future via a “describe” command.
- Maintenance info: See here [how to set up schema evolution alerts](https://dlthub.com/docs/blog/schema-evolution#whether-you-are-aware-or-not-you-are-always-getting-structured-data-for-usage). You can also capture load info such as row counts to monitor loaded volume for abnormalities, as described in the post under data dictionary.
- Code and usage: We are structuring all our [docs](https://dlthub.com/docs/intro) to follow the best practices around the 4 types of docs, generating a comprehensive, recognisable documentation. We also have GPT assistant on docs and we answer questions in slack for conversational help.

# In conclusion

Stop thinking about data, code and docs in isolation - they do not function independently, they are different parts of the same product. To produce quality documentation, focus on the why, let the code show the what and how. and use [standard formats](https://www.writethedocs.org/videos/eu/2017/the-four-kinds-of-documentation-and-why-you-need-to-understand-what-they-are-daniele-procida/) for teaching complex tooling

Want to create data products with dlt? What are you waiting for?

- Dive into our [Getting Started.](https://dlthub.com/docs/getting-started)
- [Join the ⭐Slack Community⭐ for discussion and help!](https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g)
