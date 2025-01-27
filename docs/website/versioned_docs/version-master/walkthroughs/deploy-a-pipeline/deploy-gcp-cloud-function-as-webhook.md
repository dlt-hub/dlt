# Deploy GCP Cloud Function as a webhook

A webhook is a way for one application to send automated messages or data to another application in real time. Unlike traditional APIs, which require constant polling for updates, webhooks allow applications to push information instantly as soon as an event occurs. This event-driven architecture enables faster and more responsive interactions between systems, saving valuable resources and improving overall system performance.

With this `dlt` Google Cloud event ingestion webhook, you can ingest the data and load it to the destination in real time as soon as a post request is triggered by the webhook. You can use this cloud function as an event ingestion webhook on various platforms such as Slack, Discord, Stripe, PayPal, and any other as per your requirement.

You can set up a GCP cloud function webhook using `dlt` as follows:

## 1. **Initialize deployment**

1. Sign in to your GCP account and enable the Cloud Functions API.
2. Go to the Cloud Functions section and click Create Function. Set up the environment and select the region.
3. Configure the trigger type; you can use any trigger, but for this example, we will use HTTP and select "Allow unauthenticated invocations".
4. Click "Save" and then "Next".
5. Select "Python 3.10" as the environment.
6. Use the code provided to set up the cloud function for event ingestion:

    ```py
    import dlt
    import time
    from google.cloud import bigquery
    from dlt.common import json

    def your_webhook(request):
        # Extract relevant data from the request payload
        data = request.get_json()

        Event = [data]

        pipeline = dlt.pipeline(
            pipeline_name='platform_to_bigquery',
            destination='bigquery',
            dataset_name='webhooks',
        )

        pipeline.run(Event, table_name='webhook') #table_name can be customized
        return 'Event received and processed successfully.'
    ```

7. Set the function name as "your_webhook" in the Entry point field.
8. In the requirements.txt file, specify the necessary packages:

    ```text
    # Function dependencies, for example:
    # package>=version
    dlt
    dlt[bigquery]
    ```

9. Click on "Deploy" to complete the setup.

> You can now use this cloud function as a webhook for event ingestion on various platforms such as Slack, Discord, Stripe, PayPal, and any other as per your requirement. Just remember to use the “Trigger URL” created by the cloud function when setting up the webhook. The Trigger URL can be found in the Trigger tab.


## 2. **Monitor (and manually trigger) the webhook**

To manually test the function you have created, you can send a manual POST request as a webhook using the following code:

```sh
import requests

webhook_url = 'please set me up!' # Your cloud function Trigger URL
message = {
    'text': 'Hello, Slack!',
    'user': 'dlthub',
    'channel': 'dlthub'
}

response = requests.post(webhook_url, json=message)
if response.status_code == 200:
  print('Message sent successfully.')
else:
  print('Failed to send message. Error:', response.text)
```

> Replace the webhook_url with the Trigger URL for the cloud function created.
Now, after setting up the webhook using cloud functions, every time an event occurs, the data will be ingested into your specified destination.

