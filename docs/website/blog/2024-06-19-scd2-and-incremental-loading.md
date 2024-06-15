---
slug: scd2-and-incremental-loading
title: "Slowly Changing Dimensions and Incremental loading strategies"
authors:
  name: Aman Gupta
  title: Junior Data Engineer
  url: https://github.com/dat-a-man
  image_url: https://dlt-static.s3.eu-central-1.amazonaws.com/images/aman.png
tags: [scd2, incremental loading, slowly changing dimensions, python data pipelines]
---

Data flows over time. Recognizing this is crucial for building effective data pipelines. This article focuses on the temporal aspect of data, especially when evaluating the volume of data processed in previous runs and planning for new data loads. You can easily tackle this aspect by using timestamps or cursor fields.

Incremental loading is a key technique here. It captures the latest changes by tracking where the last data chunk was processed and starting the next load. This state management ensures you only process new or changed data.

Additionally, slowly changing dimensions (SCDs) play a vital role. They help capture changes over time, ensuring historical data accuracy. Using SCDs allows you to manage and track data changes, maintaining current and historical views. This article will delve into these concepts and provide insights on how to implement them effectively.

### **What is Slowly Changing Dimension Type 2 (SCD2)?**

Let’s consider Slowly Changing Dimension Type 2 (SCD2), a method that tracks changes over time without discarding history. For example, when a customer updates their address in a delivery platform, a new entry is created instead of replacing the old address. This marks each address’s relevance timeline, showcasing SCD2’s practical application.

When a customer updates their address, the previous one isn’t erased; a new row is added, and the old record is timestamped to indicate its validity period. This approach enriches the customer profile with a historical dimension rather than simply overwriting it.

For more information on how dlt handles SCD2 strategy, refer to the [documentation here](https://dlthub.com/docs/general-usage/incremental-loading#scd2-strategy).

### **Loading usually doesn’t preserve change history**

Incremental loading boosts data loading efficiency by selectively processing only new or modified data based on the source extraction pattern. This approach is similar to updating a journal with new events without rewriting past entries. For example, in a customer interaction tracking system, incremental loading captures only the most recent interactions, avoiding the redundancy of reprocessing historical data.

Consider two issues, “Issue A” and “Issue B”. Initially, both are processed. If the pipeline is set to increment based on an `updated_at` field and “Issue B” gets updated, only this issue will be fetched and loaded in the next run. The `updated_at` timestamp is stored in the pipeline state and serves as a reference point for the next data load. How the data is added to the table depends on the pipeline’s write disposition.

For more details on incremental loading, refer to the [documentation here](https://dlthub.com/docs/general-usage/incremental-loading).

### The change history:  SCD2 and Incremental loading

Combining SCD2 with incremental loading creates a symphony in data management. While SCD2 safeguards historical data, incremental loading efficiently incorporates new updates. This synergy is illustrated through the following example:

**Example 1: Customer Status Changes**

**Initial load with slowly changing dimensions enabled:**

- Alice and Bob start with the statuses "bronze" and "gold", respectively.

| customer_key | name  | status | _dlt_valid_from     | _dlt_valid_to | last_updated        |
| ------------ | ----- | ------ | ------------------- | ------------- | ------------------- |
| 1            | Alice | bronze | 2024-01-01 00:00:00 | NULL          | 2024-01-01 12:00:00 |
| 2            | Bob   | gold   | 2024-01-01 00:00:00 | NULL          | 2024-01-01 12:00:00 |

**Incremental load (Alice's status changes to silver):**
- Alice’s status update to "silver" triggers a new entry, while her "bronze" status is preserved with a timestamp marking its duration.

| customer_key | name  | status | _dlt_valid_from     | _dlt_valid_to       | last_updated        |
| ------------ | ----- | ------ | ------------------- | ------------------- | ------------------- |
| 1            | Alice | bronze | 2024-01-01 00:00:00 | 2024-02-01 00:00:00 | 2024-01-01 12:00:00 |
| 1            | Alice | silver | 2024-02-01 00:00:00 | NULL                | 2024-02-01 12:00:00 |
| 2            | Bob   | gold   | 2024-01-01 00:00:00 | NULL                | 2024-01-01 12:00:00 |


Incremental loading would process only Alice's record because it was updated after the last load, and slowly changing dimensions would keep the record until Alice’s status was bronze.

This demonstrates how using the `last_updated` timestamp, an incremental loading strategy, ensures only the latest data is fetched and loaded. Meanwhile, SCD2 helps maintain historical records.

### Simple steps to determine data loading strategy and write disposition

This decision flowchart helps determine the most suitable data loading strategy and write disposition:

1. Is your data stateful? Stateful data is subject to change, like your age. Stateless data does not change, for example, events that happened in the past are stateless.
    1. If your data is stateless, such as logs, you can just increment by appending new logs
    2. If it is stateful, do you need to track changes to it? 
        1. If yes, then use SCD2 to track changes
        2. If no, 
            1. Can you extract it incrementally (new changes only?) 
                1. If yes, load incrementally via merge
                2. If no, re-load fully via replace.

Below is a visual representation of steps discussed above:
![Image](https://storage.googleapis.com/dlt-blog-images/flowchart_for_scd2.png)

### **Conclusion**

Slowly changing dimensions detect and log changes between runs. The mechanism is not perfect and it will not capture if multiple changes occurred to a single data point between the runs - only the last state will be reflected.

However, they enable you to track things you could not before such as 

- Hard deletes
- Daily changes and when they occurred
- Different versions of entities valid at different historical times

Want to discuss?

[Join the dlt slack community](https://dlthub.com/community) to take part in the conversation.