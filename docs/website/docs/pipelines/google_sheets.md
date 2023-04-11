---
title: Google Sheets
description: dlt pipeline for Google Sheets API
keywords: [google sheets api, google sheets pipeline, google sheets]
---

# Google Sheets

This pipeline can be used to load data from a [Google sheets](https://www.google.com/sheets/about/) workspace onto a [destination](./general-usage/glossary.md#destination) of your choice.

1. `dlt` loads each sheet in the workspace as a separate table in the destination.
2. The tables in destination have same name as individual sheets.
3. For **merged** cells, `dlt` retains only that cell value which was taken during the merge(e.g., top-leftmost), and every other cell in the merge is given a null value.
4. [**Named Ranges**](https://support.google.com/docs/answer/63175?hl=en&co=GENIE.Platform%3DDesktop) are loaded as a separate column with an automatically generated header.

## Google Sheets API authentication

### Get API credentials:

Before creating the pipeline, we need to first get the necessary API credentials:

1. Sign in to [[console.cloud.google.com](http://console.cloud.google.com/)].
2. [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create#creating) if you don't already have one.
3. Enable Google Sheets API:
    1. In the left panel under *APIs & Services*, choose *Enabled APIs & services*.
    2. Click on *+ ENABLE APIS AND SERVICES* and find and select Google Sheets API.
    3. Click on *ENABLE*.
4. Generate credentials:
    1. In the left panel under *IAM & Admin*, select *Service Accounts*.
    2. In the service account table click on the three dots under the column "Actions" for the service account that you wish to use.
    3. Select *Manage Keys*.
    4. Under *ADD KEY* choose *Create new key*, and for the key type JSON select *CREATE*.
    5. This downloads a .json which contains the credentials that we will be using later.

### Share the Google Sheet with the API:

To allow the API to access the Google Sheet, open the sheet that you wish to use and do the following:

1. Select the share button on the top left corner. 

![Share_Button](docs_images/Share_button.png)

2. In *Add people and groups*, add the *client_email* with at least viewer privileges. You will find this *client_email* in the JSON that you downloaded above.

![Add people](docs_images/Add_people.png)

3. Finally, click on *Copy link* and save the link. This will need to be added to the `dlt` script.

Here you will find a setup guide for the [Google sheets](https://www.google.com/sheets/about/) pipeline.

## Initialize the pipeline

We can now create the pipeline.

Initialize a `dlt` project with the following command:

```bash
dlt init google_sheets bigquery
```
Here, we chose BigQuery as the destination. To choose a different destination, replace `bigquery` with your choice of destination. 

Running this command will create a directory with the following structure:
```shell
directory
├── .dlt
│   ├── .pipelines
│   ├── config.toml
│   └── secrets.toml
└── google_sheets
    ├── helpers
    │   ├── __.init.py__
    │   ├── api_calls.py
    │   └── data_processing.py
    ├── .gitignore
    ├── google_sheets_pipelines.py
    └── requirements.txt
```

## Add credentials

1. Open `.dlt/secrets.toml`
2. From the .json that you downloaded earlier, copy `project_id`, `private_key`, and `client_email` under `[sources.google_spreadsheet.credentials]`
```python
[sources.google_spreadsheet.credentials]
project_id = "set me up" # GCP Source project ID!
private_key = "set me up" # Unique private key !(Must be copied fully including BEGIN and END PRIVATE KEY)
client_email = "set me up" # Email for source service account
location = "set me up" #Project Location For ex. “US”
```
3. Enter credentials for your chosen destination as per the [docs](https://dlthub.com/docs/destinations#google-bigquery)

## Add Spreadsheet ID and URL

1. The following two constants need to be added to `google_sheets_pipelines.py`

```python
# constants
SPREADSHEET_ID = "Set_me_up" # Spread Sheet ID (as per Step Two)
SPREADSHEET_URL = "Set_me_up"  #URL from Google Sheets > Share > Copy Link
```

2. The spreadsheet URL can be found in Google Sheets > Share Button ( at top right) > Copy the link. 
3. Assign the copied link to `SPREADSHEET_URL`.
4. The `SPREADSHEET_ID` can be found in `SPREADSHEET_URL`, for example. if the SPREADSHEET_URL is as below: 

```python
https://docs.google.com/spreadsheets/d/1VTtCiYgxjAwcIw7UM1_BSaxC3rzIpr0HwXZwd2OlPD4/edit?usp=sharing
```

Then `SPREADSHEET_ID` is between …spreadsheets/d/**SPREADSHEET_ID**/edit?usp=sharing i.e. as below

```python
**1VTtCiYgxjAwcIw7UM1_BSaxC3rzIpr0HwXZwd2OlPD4**
```

5. After filling in the variables as above, save the file. ( Ctrl + S or Cmd + S ).

## Run the pipeline
1. Install the requirements by using the following command

```python
pip install -r requirements.txt
```

2. Run the pipeline by using the following command

```python
python3 google_sheets_pipelines.py
```

3. Use `dlt pipeline google_sheets_pipeline show` to make sure that everything loaded as expected.