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

Yummy is a food box business. At the intersection of gastronomy and logistics, this market is very competitive.
To make it in this market, Yummy needs to be fast and informed in their operations.

### Pipelines are not yet a commodity.

At Yummy, time is the most valuable resource. When presented with the option to buy vs build, Yummy's CTO Martin thought not to waste time and BUY!

Yummy’s use cases for data pipelining was copying sql data with low latency and high SLA for operational usage as well as analytical.

Unfortunately, money does not buy ~~happiness~~ fast, reliable pipelines.

### What’s important: Velocity, Reliability, Speed, time. Money is secondary.

Cost aside, the main requirement for Yummy was as below:

- Velocity: it should not take much of our time, fast to set up
- Reliabilty: the data should not stop flowing as it’s used for operations.
- Speed: It should load data with low latency
- ideally, it would also be cheap
- ideally, it would be able to extract once and write to multiple destinations.

Martin found the velocity to set up to be good, but everything else lacking.

## Enough is enough! Black hat practices of vendors drove Martin away.

Martin, like many others, was very open to using the saas service and was happy to not do it himself.

However, he quickly ran into the dark side of saas vendor service. Initially, the latency and reliability were annoying, but not enough reason to move.

Martin's patience ran out when a state log table added to his production database was automatically replicated in full, repeatedly.

This default setting led to exorbitantly high charges that were neither justified nor sustainable, pushing him to seek better solutions.

This is a common issue which is "by design" as people complain about it for over a decade. But the majority of customers were "born yesterday" into the etl marketplace and the vendors are ready to take them for a ride.

<aside>
💡 One thing's for sure, without human intervention, that new table isn't getting used, so the only reason to default to copying a table instead of notifying is purely to take your money. This is a standard, known black-hat practice in the industry for a decade and hasn't going away.
</aside>


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

Yummy needed the data to stay competitive, so Martin wasted no more time, and set of to create one of the cooler pipelines I’ve seen - a postgres async
source that will extract data async by slicing tables and passing the data to dlt, which can also process it in parallel.

Check out [Martin's async postgres source in this gist here](https://gist.github.com/salomartin/c0d4b0b5510feb0894da9369b5e649ff).

### The outcome

ETL cost down 182x per month, sync time improved 10x using Modal labs and dlt and dropping fivetran.
Martin was happy enough that he agreed to go on a call and tell us about it :)

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