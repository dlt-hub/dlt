---
title: Schema evolution
description: Schema evolution with dlt
keywords: [schema evolution, schema versioning, data contracts]
---

# Schema evolution

Schema evolution combines a technical process with a curation process, so let's understand the process, and where the technical automation needs to be combined with human curation.

## Whether you are aware or not, you are always getting structured data for usage

Data used is always structured, but usually produced unstructured.

Structuring it implicitly during reading is called "schema on read", while structuring it upfront is called "schema on write".

To fit unstructured data into a structured database, developers have to perform this transition before loading.
For data lake users who read unstructured data, their pipelines apply a schema during read - if this schema is violated, the downstream software will produce bad outcomes.

### We tried running away from our problems, but it didn't work.

Because structuring data is difficult to deal with, people have tried to not do it. But this created its own issues.
- Loading json into db without typing or structuring - This anti-pattern was created to shift the structuring of data to the analyst. While this is a good move for curation, the db support for structuring data is minimal and unsafe. In practice, this translates to the analyst spending their time writing lots of untested parsing code and pushing silent bugs to production.
- Loading unstructured data to lakes - This pattern pushes the curation of data to the analyst. The problem here is similar to the one above. Unstructured data is hard to analyse and curate, and the farther it is from the producer, the harder it is to understand.

So no, one way or another we are using schemas.

### If curation is hard, how can we make it easier?

- Make data easier to discover, analyze, explore. Structuring upfront would do that.
- Simplify the human process by decentralizing data ownership and curation - the analyst can work directly with the producer to define the dataset produced.

## Structuring & curating data are two separate problems. Together they are more than the sum of the parts.

The problem is that curating data is hard.
  - Typing and normalising data are technical processes.
  - Curating data is a business process.


Here's what a pipeline building process looks like:
1. Speak with the producer to understand what the data is. Chances are the producer does not document it and there will be many cases that need to be validated analytically.
2. Speak with the analyst or stakeholder to get their requirements. Guess which fields fulfill their requirements.
3. Combine the 2 pieces of info to filter and structure the data so it can be loaded.
4. Type the data (for example, convert strings to datetime).
5. Load the data to warehouse. Analyst can now validate if this was the desired data with the correct assumptions.
6. Analyst validates with stakeholder that this is the data they wanted. Stakeholder usually wants more.
7. Possibly adjust the data filtering, normalization.
8. Repeat entire process for each adjustment.

And when something changes,

1. The data engineer sees something break
2. they ask the producer about it
3. they notify the analyst about it
4. the analyst notifies the business that data will stop flowing until adjustments
5. the analyst discusses with the stakeholder to get any updated requirements
6. the analyst offers the requirements to the data engineer
7. the data engineer checks with the producer/data how the new data should be loaded
8. data engineer loads the new data
9. the analyst can now adjust their scripts, re-run them, and offer data to stakeholder



## Divide et impera! The two problems are technical and communicational, so let's let computers solve tech and let humans solve communication.

Before we start solving, let's understand the problem:
1. For usage, data needs to be structured.
2. Because structuring is hard, we try to reduce the amount we do by curating first or defering to the analyst by loading unstructured data.
3. Now we are trying to solve two problems at once: structuring and curation, with each role functioning as a bottleneck for the other.

So let's de-couple these two problems and solve them appropriately.
- The technical issue is that unstructured data needs to be structured.
- The curation issue relates to communication - so taking the engineer out of the loop would make this easier.

### Automate the tech: Structuring, typing, normalizing

The only reason to keep data unstructured was the difficulty of applying structure.

By automating schema inference, evolution, normalization, and typing, we can just load our jsons into structured data stores, and curate it in a separate step.

### Alert the communicators: When there is new data, alert the producer and the curator.

To govern how data is produced and used, we need to have a definition of the data that the producer and consumer can both refer to.
This has typically been tackled with data contracts - a type of technical test that would notify the producer and consumer of violations.

So how would a data contract work?
1. Human process:
   1. Humans define a data schema.
   2. Humans write a test to check if data conforms to the schema.
   3. Humans implement notifications for test fails.
2. Technical process:
   1. Data is extracted.
   2. Data is staged to somewhere where it can be tested.
   3. Data is tested:
      1. If the test fails, we notify the producer and the curator.
      2. If the test succeeds, it gets transformed to the curated form.

So how would we do schema evolution with `dlt`?

1. Data is extracted, `dlt` infers schema and can compare it to the previous schema.
2. Data is loaded to a structured data lake (staging area).
3. Destination schema is compared to the new incoming schema.
   1. If there are changes, we notify the producer and curator
   2. If there are no changes, we carry on with transforming it to the curated form.

So, schema evolution is essentially a simpler way to do a contract on schemas.
If you had additional business-logic tests, you would still need to implement them in a custom way.


## The implementation recipe
1. Use `dlt`. It will automatically infer and version schemas, so you can simply check if there are changes. You can just use the [normaliser + loader](../general-usage/pipeline) or [build extraction with dlt](../general-usage/resource). If you want to define additional constraints, you can do so in the (schema)[../general-usage/schema].
2. [Define your slack hook](https://dlthub.com/docs/running-in-production/running#using-slack-to-send-messages) or create your own notification function.
3. [Capture the load job info and send it to the hook](../running-in-production/running#inspect-save-and-alert-on-schema-changes).
