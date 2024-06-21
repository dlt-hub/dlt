---
slug: google-forms-to-notion
title: "Syncing Google Forms data with Notion using dlt"
authors:
  name: Aman Gupta
  title: Junior Data Engineer
  url: https://github.com/dat-a-man
  image_url: https://dlt-static.s3.eu-central-1.amazonaws.com/images/aman.png
tags: [google forms, cloud functions, google-forms-to-notion]
---

## Why do we do it?

Hello, I'm Aman, and I assist the dlthub team with various data-related tasks. In a recent project, the Operations team needed to gather information through Google Forms and integrate it into a Notion database. Initially, they tried using the Zapier connector as a quick and cost-effective solution, but it didn’t work as expected. Since we’re at dlthub, where everyone is empowered to create pipelines, I stepped in to develop one that would automate this process.

The solution involved setting up a workflow to automatically sync data from Google Forms to a Notion database. This was achieved using Google Sheets, Google Apps Script, and a `dlt` pipeline, ensuring that every new form submission was seamlessly transferred to the Notion database without the need for manual intervention.

## Implementation

So here are a few steps followed:

**Step 1: Link Google Form to Google Sheet**

Link the Google Form to a Google Sheet to save responses in the sheet. Follow [Google's documentation](https://support.google.com/docs/answer/2917686?hl=en#zippy=%2Cchoose-where-to-store-responses) for setup.

**Step 2: Google Apps Script for Data Transfer**

Create a Google Apps Script to send data from Google Sheets to a Notion database via a webhook. This script triggers every time a form response is saved.

**Google Apps Script code:**

```text
function sendWebhookOnEdit(e) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  var range = sheet.getActiveRange();
  var updatedRow = range.getRow();
  var lastColumn = sheet.getLastColumn();
  var headers = sheet.getRange(1, 1, 1, lastColumn).getValues()[0];
  var updatedFields = {};
  var rowValues = sheet.getRange(updatedRow, 1, 1, lastColumn).getValues()[0];

  for (var i = 0; i < headers.length; i++) {
    updatedFields[headers[i]] = rowValues[i];
  }

  var jsonPayload = JSON.stringify(updatedFields);
  Logger.log('JSON Payload: ' + jsonPayload);

  var url = 'https://your-webhook.cloudfunctions.net/to_notion_from_google_forms'; // Replace with your Cloud Function URL
  var options = {
    'method': 'post',
    'contentType': 'application/json',
    'payload': jsonPayload
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    Logger.log('Response: ' + response.getContentText());
  } catch (error) {
    Logger.log('Failed to send webhook: ' + error.toString());
  }
}
```

**Step 3: Deploying the ETL Pipeline**

Deploy a `dlt` pipeline to Google Cloud Functions to handle data transfer from Google Sheets to the Notion database. The pipeline is triggered by the Google Apps Script.

1. Create a Google Cloud function.
2. Create `main.py` with the Python code below.
3. Ensure `requirements.txt` includes `dlt`.
4. Deploy the pipeline to Google Cloud Functions.
5. Use the function URL in the Google Apps Script.

:::note
This pipeline uses  `@dlt.destination` decorator which is used to set up custom destinations. Using custom destinations is a part of `dlt's` reverse ETL capabilities. To read more about `dlt's` reverse ETL pipelines, please read the [documentation here.](https://dlthub.com/docs/dlt-ecosystem/destinations/destination)
:::

**Python code for `main.py` (Google cloud functions) :**

```py
import dlt
from dlt.common import json
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from datetime import datetime
from dlt.sources.helpers import requests

@dlt.destination(name="notion", batch_size=1, naming_convention="direct", skip_dlt_columns_and_tables=True)
def insert_into_notion(items: TDataItems, table: TTableSchema) -> None:
    api_key = dlt.secrets.value  # Add your notion API key to "secrets.toml"
    database_id = "your_notion_database_id"  # Replace with your Notion Database ID
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-02-22"
    }

    for item in items:
        if isinstance(item.get('Timestamp'), datetime):
            item['Timestamp'] = item['Timestamp'].isoformat()
        data = {
            "parent": {"database_id": database_id},
            "properties": {
                "Timestamp": {
                    "title": [{
                        "text": {"content": item.get('Timestamp')}
                    }]
                },
                # Add other properties here
            }
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print(response.status_code, response.text)

def your_webhook(request):
    data = request.get_json()
    Event = [data]

    pipeline = dlt.pipeline(
        pipeline_name='platform_to_notion',
        destination=insert_into_notion,
        dataset_name='webhooks',
        full_refresh=True
    )

    pipeline.run(Event, table_name='webhook')
    return 'Event received and processed successfully.'
```

### Step 4: Automation and Real-Time updates

With everything set up, the workflow automates data transfer as follows:

1. Form submission saves data in Google Sheets.
2. Google Apps Script sends a POST request to the Cloud Function.
3. The `dlt` pipeline processes the data and updates the Notion database.

# Conclusion

We initially considered using Zapier for this small task, but ultimately, handling it ourselves proved to be quite effective. Since we already use an orchestrator for our other automations, the only expense was the time I spent writing and testing the code. This experience demonstrates that `dlt` is a straightforward and flexible tool, suitable for a variety of scenarios. Essentially, wherever Python can be used, `dlt` can be applied effectively for data loading, provided it meets your specific needs.