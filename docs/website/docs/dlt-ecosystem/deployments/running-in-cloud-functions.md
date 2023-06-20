---
title: Running in cloud functions
description: how to run dlt in serverless functions
keywords: [dlt, webhook, serverless]
---

# Why run in cloud functions? Use cases and limitations

## Real time data stream processing:

Real time data ingestion and usage architectures usually take the following steps:
1. Collect raw data in a buffer. For this we might use streaming technologies or some ad hoc buffer.
2. Periodically read the buffer and process the data. Here we often use a serverless function that can validate schemas and load the data to the next stage.

The reason we use cloud functions for this case is that cloud functions can be spawned in parallel, allowing us to scale gracefully. For example, if we call a function to process every chunk of X size, then if the data volume varies, the only thing that changes is how many functions we call.

## Event-driven Data Processing (Webhooks, etc)

Some applications can send events as they happen, and in order to capture them, you need an "always up" listener application.

To implement event-driven data processing with serverless cloud functions, you typically follow these steps:

1. Configure Event Source: Set up the event source, such as a webhook provider, to send events to the cloud function. This involves defining the event triggers, authentication mechanisms, and any required event payload or metadata.
2. Develop Cloud Function: Write the event processing logic in the chosen cloud function programming language. This logic defines how the incoming event data is processed, transformed, stored, or integrated with other systems or services. dlt enables you to validate schemas and process events in this step.
3. Deploy and Configure Cloud Function: Deploy the cloud function to the serverless function platform, such as AWS Lambda, Google Cloud Functions, or Azure Functions. Configure the function settings, including resource allocation, timeouts, and any required environment variables or dependencies.

## Batch ingestion

Cloud functions offer a powerful alternative for batch ingestion. Due to the possibility to run multiple instances in parallel, using cloud functions to process data gives you easy access to scalable compute, and can help you achieve consistent processing times.

# How to set up dlt on a cloud function?

1. Choose platform to run on, and create a cloud function with python, and enough resources for what you want to process.
   * Make sure your cloud function hardware is sufficient by [configuring dlt buffer size](/docs/getting-started/build-a-data-pipeline#scaling)
   * Make sure that the function is running the [required](/docs/reference/installation) library version.
2. Set up dependencies: Make sure that all the required dependencies are packaged and included in your function.
3. Implement the function: Add the code itself and test it.
4. Configure the triggers and any security settings, test it.
5. Add the webhook to the data producing application or add credentials to the function, depending on what you are doing.
6. Consider [alerting](/docs/running-in-production/running#using-slack-to-send-messages) non-success or scchema changes - use dlt's notifications or plug into the cloud monitoring.
7. Define a monitoring strategy to keep track of function executions.

> 💡 Here's a step by step guide for [Google Cloud Function](../../walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-functions.md)