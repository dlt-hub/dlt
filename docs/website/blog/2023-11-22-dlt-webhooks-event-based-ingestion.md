---
slug: dlt-webhooks-on-cloud-functions-for-event-capture 
title: "Deploy Google Cloud Functions as webhooks to capture event-based data from GitHub, Slack, or Hubspot"
image:  https://dlt-static.s3.eu-central-1.amazonaws.com/images/webhook_blog_image.jpeg
authors:
  name: Aman Gupta
  title: Junior Data Engineer
  url: https://github.com/dat-a-man
  image_url: https://dlt-static.s3.eu-central-1.amazonaws.com/images/aman.png
tags: [data ingestion, python sdk, ETL, python data pipelines, Open Source, Developer Tools]
---

💡 This article explores methods for monitoring transactional events, allowing immediate action and data capture
that might be lost otherwise. We focus on Github, Slack, and Hubspot, demonstrating techniques applicable to 
low-volume transactional events (under 500k/month) within the free tier. For clickstream tracking or higher 
volumes, we recommend more scalable solutions.

There’s more than one way to sync data. Pulling data after it has been collected from APIs is a
classic way, but some types of data are better transmitted as an event at the time of happening. Our
approach is event-triggered and can include actions like:

| Application | Action                                |
|-------------|---------------------------------------|
| Slack       | Sending messages in Slack             |
| Github      | Commit, comment, or PR actions        |
| Hubspot     | Object creation or meeting specific criteria |


These actions initiate a webhook that sends a POST request to trigger a DLT pipeline for event
ingestion. The data is then loaded into BigQuery.

![pictorial_demonstration](https://dlt-static.s3.eu-central-1.amazonaws.com/images/webhook_blog_image.jpeg)

This setup enables real-time alerts or event storage for later use. For example, let’s say you want
to alert every time something happens - you’d want to be able to capture an event being sent to you
and act on it. Or, in some cases, you store it for later use. This guide covers a use case for
deploying and setting up webhooks.

### Why do we use webhooks?

Whenever we want to receive an event from an external source, we need a “recipient address” to which
they can send the data. To solve this problem, an effortless way is to use a URL as the address and
accept a payload as data.

### Why cloud functions?

The key reasons for using cloud functions include:

1. To have a ***URL up and accept the data payload***, we would need some service or API always to be
  up and ready to listen for the data.

1. Creating our application for this would be cumbersome and expensive. It makes sense to use some
  serverless service for low volumes of events.

1. On AWS, you would use API gateway + lambda to handle incoming events, but for GCP users, the
  option is more straightforward: Google Cloud functions come with an HTTP trigger, which enables
  you to ***create a URL and accept a payload.***

1. The pricing for cloud functions is unbeatable for low volumes: For ingesting an event with a minor
  function, assuming processing time to be a few seconds, we could invoke a few hundred thousand
  calls every month for free. For more pricing details, see the
  [GCP pricing page for cloud functions.](https://cloud.google.com/functions/pricing)

Let's dive into the deployment of webhooks and app setup, focusing next on triggers from GitHub,
Slack, and HubSpot for use cases discussed above.

## 1. GitHub Webhook

This GitHub webhook is triggered upon specified events such as pull requests (PRs), commits, or
comments. It relays relevant data to BigQuery. Set up the GitHub webhook by creating the cloud
function URL and configuring it in the GitHub repository settings.

### 1.1 Initialize GitHub webhook deployment

To set up the webhook, start by creating a cloud function. Follow these brief steps, and for an
in-depth guide, please refer to the detailed documentation.

1. Log into GCP and activate the Cloud Functions API.
1. Click 'Create Function' in Cloud Functions, and select your region and environment setup.
1. Choose HTTP as the trigger, enable 'Allow unauthenticated invocations', save, and click 'Next'.
1. Set the environment to Python 3.10 and prepare to insert code into main.py:
   ```python
   import dlt
   import json
   import time
   from google.cloud import bigquery

   def github_webhook(request):
       # Extract relevant data from the request payload
       data = request.get_json()

       Event = [data]

       pipeline = dlt.pipeline(
           pipeline_name='platform_to_bigquery',
           destination='bigquery',
           dataset_name='github_data',
       )

       pipeline.run(Event, table_name='webhook') #table_name can be customized
       return 'Event received and processed successfully.'
   ```
1. Name the function entry point "github_webhook" and list required modules in requirements.txt.
   ```text
   # requirements.txt
   dlt[bigquery]
   ```
1. Post-deployment, a webhook URL is generated, typically following a specific format.
   ```bash
   https://{region]-{project-id}.cloudfunctions.net/{cloud-function-name}
   ```

Once the cloud function is configured, it provides a URL for GitHub webhooks to send POST requests,
funneling data directly into BigQuery.

### 1.2 Configure the repository webhook in GitHub

Set up a GitHub repository webhook to trigger the cloud function on specified events by following
these steps:

1. Log into GitHub and go to your repository.
1. Click "Settings" > "Webhooks" > "Add webhook."
1. Enter the cloud function URL in "Payload URL."
1. Choose "Content-Type" and select events to trigger the webhook, or select "Just send me
   everything."
1. Click "Add webhook."

With these steps complete, any chosen events in the repository will push data to BigQuery, ready for
analysis.

## 2. Slack Webhook

This Slack webhook fires when a user sends a message in a channel where the Slack app is installed.
To set it up, set up a cloud function as below and obtain the URL, then configure the message events
in Slack App settings.

### 2.1 Initialize Slack webhook deployment

Set up the webhook by creating a cloud function, using the same steps as for the [GitHub webhook.](#1-github-webhook)


1. Here’s what `main.py` looks like:
   ```python
   import dlt
   from flask import jsonify

   def slack_webhook(request):
       # Handles webhook POST requests
       if request.method == 'POST':
           data = request.get_json()

           # Responds to Slack's verification challenge
           if 'challenge' in data:
               return jsonify({'challenge': data['challenge']})

           # Processes a message event
           if 'event' in data and 'channel' in data['event']:
               message_data = process_webhook_event(data['event'])

               # Configures and initiates a DLT pipeline
               pipeline = dlt.pipeline(
                   pipeline_name='platform_to_bigquery', 
                   destination='bigquery', 
                   dataset_name='slack_data',
               )

               # Runs the pipeline with the processed event data
               pipeline.run([message_data], table_name='webhook')
               return 'Event processed.'
           else:
               return 'Event type not supported', 400
       else:
           return 'Only POST requests are accepted', 405

   def process_webhook_event(event_data):
       # Formats the event data for the DLT pipeline
       message_data = {
           'channel': event_data.get('channel'),
           'user': event_data.get('user'),
           'text': event_data.get('text'),
           'ts': event_data.get('ts'),
           # Potentially add more fields according to event_data structure
       }
       return message_data
   ```
1. Name the entry point "slack_webhook" and include the necessary modules in **`requirements.txt`**,
   the same as the GitHub webhook setup.
1. Once the cloud function is configured, you get a URL for Slack events to send POST requests,
   funneling data directly into BigQuery.

### 2.2 Set up and configure a Slack app

Create and install a Slack app in your workspace to link channel messages from Slack to BigQuery as
follows:

1. Go to "Manage apps" in workspace settings; click "Build" and "Create New App".
1. Choose "from scratch", name the app, select the workspace, and create the app.
1. Under "Features", select "Event Subscription", enable it, and input the Cloud Function URL.
1. Add `message.channels` under "Subscribe to bot events".
1. Save and integrate the app to the desired channel.

With these steps complete, any message sent on the channel will push data to BigQuery, ready for
analysis.

## 3. Hubspot webhook

A Hubspot webhook can be configured within an automation workflow, applicable to contacts,
companies, deals, tickets, quotes, conversations, feedback submissions, goals and invoices. It
triggers upon specific conditions or data filters. To establish it, create a cloud function,
retrieve its URL, and input this in Hubspot's automation workflow settings for message events.

### 3.1 Initialize Hubspot webhook deployment

Set up the webhook by creating a cloud function, using the same steps as for the [GitHub webhook.](#1-github-webhook)


1. Here’s what `main.py`looks like:
   ```python
   import dlt
   from flask import jsonify

   def hubspot_webhook(request):
       # Endpoint for handling webhook POST requests from Hubspot
       if request.method == 'POST':
           # Get JSON data from the POST request
           data = request.get_json()

           # Initialize and configure the DLT pipeline
           pipeline = dlt.pipeline(
               pipeline_name=ßigquery',               # Destination service for the data
               dataset_name='hubspot_webhooks_dataset',  # BigQuery dataset name
           )

           # Execute the pipeline with the incoming data
           pipeline.run([data], table_name='hubspot_contact_events')
           
           # Return a success response
           return jsonify(message='HubSpot event processed.'), 200
       else:
           # Return an error response for non-POST requests
           return jsonify(error='Only POST requests are accepted'), 405

   ```
1. Name the entry point "your_webhook" and include the necessary modules in **`requirements.txt`**,
   the same as the GitHub webhook setup.
1. Once the cloud function is configured, you get a URL for Slack events to send POST requests,
   funneling data directly into BigQuery.

### 3.2 Configure a Hubspot automation workflow

To activate a Hubspot workflow with your webhook:

1. Go to Hubspot: "Automation" > "Workflows" > "Create workflow".
1. Start from scratch; choose "Company-based" for this example.
1. Set "Object created" as the trigger.
1. Add the "Send a webhook" action, use the "POST" method, and input your webhook URL.
1. Select the company properties to include, test, and save.

This triggers the webhook upon new company creation, sending data to Bigquery via DLT.

### In conclusion

Setting up a webhook is straightforward.

Using dlt with schema evolution, we can accept the events without worrying about their schema.
However, for events with custom schemas or vulnerable to bad data quality or abuse, consider using
dlt’s data contracts.
