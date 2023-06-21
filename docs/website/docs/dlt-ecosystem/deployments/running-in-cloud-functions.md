---
title: Running in cloud functions
description: how to run dlt in serverless functions
keywords: [dlt, webhook, serverless]
---

# Why run in cloud functions? Use cases and limitations

## Real time data stream processing:

Real time data ingestion and usage architectures usually take the following steps:

1. Collect raw data in a buffer. For this we might use streaming technologies or some ad hoc buffer.
1. Periodically read the buffer and process the data. Here we often use a serverless function that
   can validate schemas and load the data to the next stage.

The reason we use cloud functions for this case is that cloud functions can be spawned in parallel,
allowing us to scale gracefully. For example, if we call a function to process every chunk of X
size, then if the data volume varies, the only thing that changes is how many functions we call.

## Event-driven Data Processing (Webhooks, etc.)

Some applications can send events as they happen, and in order to capture them, you need an "always
up" listener application.

To implement event-driven data processing with serverless cloud functions, you typically follow
these steps:

1. Configure Event Source: Set up the event source, such as a webhook provider, to send events to
   the cloud function. This involves defining the event triggers, authentication mechanisms, and any
   required event payload or metadata.
1. Develop Cloud Function: Write the event processing logic in the chosen cloud function programming
   language. This logic defines how the incoming event data is processed, transformed, stored, or
   integrated with other systems or services. dlt enables you to validate schemas and process events
   in this step.
1. Deploy and Configure Cloud Function: Deploy the cloud function to the serverless function
   platform, such as AWS Lambda, Google Cloud Functions, or Azure Functions. Configure the function
   settings, including resource allocation, timeouts, and any required environment variables or
   dependencies.

## Batch ingestion

Cloud functions offer a powerful alternative for batch ingestion. Due to the possibility to run
multiple instances in parallel, using cloud functions to process data gives you easy access to
scalable compute, and can help you achieve consistent processing times.

> 💡 Read our
> [Walkthroughs: Deploy a pipeline with Google Cloud Functions](../../walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-functions)
> to find out more.
