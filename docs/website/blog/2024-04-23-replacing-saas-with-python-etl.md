---
slug: replacing-saas-elt
title: "Replacing Saas ETL with Python dlt: A painless experience for Yummy.eu"
image:  https://storage.googleapis.com/dlt-blog-images/martin_salo_tweet.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [full code etl, yes code etl, etl, python elt]
---

About [Yummy.eu](https://about.yummy.eu/)

Yummy is a Lean-ops meal-kit company streamlines the entire food preparation process for customers in emerging markets by providing personalized recipes,
nutritional guidance, and even shopping services. Their innovative approach ensures a hassle-free, nutritionally optimized meal experience,
making daily cooking convenient and enjoyable.


Yummy is a food box business. At the intersection of gastronomy and logistics, this market is very competitive. To make it in this market, Yummy needs to be fast and informed in their operations.

### Pipelines are not yet a commodity.

At Yummy, time is the most valuable resource. When presented with the option to buy vs build, it’s a no brainer - BUY!

Yummy’s use cases for data pipelining was copying sql data with low latency and high SLA for operational usage as well as analytical.

Unfortunately, money does not buy ~~happiness~~ fast, reliable pipelines.

## A rant about how SaaS vendors will rip you off if they can: A common experience of opaque pricing and black hat practices

There is lots of literature about how saas vendors take money where they shouldn’t, for example:

- **Pricing transparency**: By billing for things you cannot measure, it is difficult to plan cost and you might be surprised by 2-10x cost compared to what you thought (never pleasantly - that would be advertised upfront).
- **Default to max payment:** New table? no problem, vendors will make sure to replicate it by default at max cost to you (full refresh). Imagine if your waiter decided to bring you desert and bill you for it, you’d be outraged, no?
- **Customization options**: While the automation of processes simplifies data integration significantly, it also limits customization. Greater flexibility in customizing ETL processes would benefit those with complex data workflows and specific business requirements.
- **Connector dependency**: The extensive range of connectors greatly facilitates data integration across various sources. However, the functionality heavily relies on the availability and maintenance of these connectors. More frequent updates and expansions to the connector library would ensure robust data integration capabilities for all users.
- **Feature access across different plans**: Many essential features are accessible only at higher subscription tiers. Providing a broader range of critical features across all plans would make the platform more competitive and accessible to a wider range of businesses.
- **Data sync frequencies**: Limitations on data sync frequencies in lower-tier plans can hinder businesses that require more frequent updates. Offering more flexibility with sync frequencies across all plans would better support the needs of businesses with dynamic data requirements.

In the case of Martin, cost was not even the main factor - the delays were a pain and so was the lack of reliability. And when Martin added a state log table to the production database and that ended up full replicated over and over by default, generating stupid high bills, he had enough.

This is a very common experience. And you won’t get your money back by complaining either, despite the actual cost of delivering you the service was only 1% of your bill at most. Because, it was no accident!

Suppose you set up a tool for the first time. It’s the vendor’s responsibility to take your attention through all the considerations they created, and give you the information with clarity.

If I were in the shoes of the person who had to explain this bill to finance, I would avoid those tools like the plague. To be overcharged, and dependent on poor quality service? I’ll take a hard pass.

### Back to what’s important: Velocity, Reliability, Speed, time, money

Cost aside, the main requirement for Yummy was

- Velocity: it should not take much of our time, fast to set up
- Reliabilty: the data should not stop flowing as it’s used for operations.
- Speed: It should be fast to run
- ideally, it would also be cheap
- ideally, it would be able to extract once and write to multiple destinations.

Besides the velocity to set up, none of the other requirements were met by the vendor.

## 10x faster, 182x cheaper with dlt + async + modal

Fast to build, fast & cheap to run.

- dlt lets you go from idea to pipeline in minutes due to its simplicity
- async lets you leverage parallelism
- [Modal](https://modal.com/) give you orchestrated parallelism at low cost

By building a dlt postgres source with async functionality and running it on Modal, the important stuff was done.

- Velocity: easy to build with dlt.
- Reliabilty: Both dlt and modal are reliable.
- Speed: 10x gains over the saas tool
- cost: 183x cheaper than the saas vendor (it doesn’t help their case to have costly defaults)
- ideally, it would be able to extract once and write to multiple destinations. Dlt can, to do this with a saas vendor you would need to chain pipelines and thus also multiply costs and delays.

### The build

Yummy needed the data to stay competitive, so Martin wasted no more time, and set of to create one of the cooler pipelines i’ve seen - a postgres async source that will extract data async by slicing tables and passing the data to dlt, which can also process it in paralel.

https://gist.github.com/salomartin/c0d4b0b5510feb0894da9369b5e649ff

### The outcome

ETL cost down 182x per month, sync time improved 10x using Modal labs and dlt and dropping fivetran.

[![salo-martin-tweet](https://storage.googleapis.com/dlt-blog-images/martin_salo_tweet.png)](https://twitter.com/salomartin/status/1755146404773658660)

## Closing thoughts

Taking back control of your data stack has never been easier, given the ample open source options.

SQL copy pipelines **are** a commodity. They are mostly interchangeable, there is no special transformation or schema difference between varieties of them,
and you can find many of them for free online. Besides the rarer ones that have special requirements, there' little uniqueness
distinguishing various sql copy pipelines from each other.

SQL to SQL copy pipelines remain one of the most common use cases in the industry. Despite the low complexity of such pipelines,
this is where many vendors make a fortune, charging thousands of euros monthly for something that could be run for the cost of a few coffees.

At dltHub, we encourage you to use simple, free resources to take back control of your data deliverability and budgets.

From our experience, the cost of setting up a SQL pipeline can be minutes.

Try these pipelines:

- [30+ sql database sources](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database)
- [Martin’s async postgres source](https://gist.github.com/salomartin/c0d4b0b5510feb0894da9369b5e649ff)
- [Arrow + connectorx](https://www.notion.so/Martin-Salo-Yummy-2061c3139e8e4b7fa355255cc994bba5?pvs=21) for up to 30x faster copies

Need help? [Join our community!](https://dlthub.com/community)