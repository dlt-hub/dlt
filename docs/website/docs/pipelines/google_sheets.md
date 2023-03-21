# Google sheets

Here you will find a setup guide for the [Google sheets](https://www.google.com/sheets/about/) pipeline.

## Set up account
1.  Set up a Google account
2.  Login to google sheets using your google credentials.

## Data loading info

### Google sheets workspace

1. The google sheets **workspace** is made of one or more sheets.
2. `dlt` loads all the **sheets** in google sheets workspace as separate tables in the destination.
3. The tables in destination have same name as individual sheets.

### Named ranges
1. A named range is a simple name given to a range of cells. For more info on named ranges, please check the [docs](https://support.google.com/docs/answer/63175?hl=en&co=GENIE.Platform%3DDesktop).
2. `dlt` also loads the **named ranges** in google sheets as a separate table. The first row of the named range is the header. 
3. The table names are the same as the names of the named ranges.

### Merged Cells
1. For merged cells, `dlt` loads all the information in the merged cell into the table and treats others as NULL.

## Initialize the pipeline

**Initialize the pipeline by using the following command with your [destination](/destinations.md) of choice:**
```bash
dlt init pipedrive [destination]
```

This will create a directory that includes the following file structure:
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

## Configuring `dlt` credentials

To access google sheets, you need to create a google cloud (GCP) service account with at least viewer rights. 

1. Open [[console.cloud.google.com](http://console.cloud.google.com/)] and log in. 
2. Go to **“API and Services”** and enable the google sheets API**.**
3. In the credentials tab of the google sheets API, either use the service accounts already listed or create a new service account.
4. To create a new service account go to manage service accounts in the bottom right of the credentials tab.
    1. Select create a service account.
    2. Set up a source service account with at least the viewer privileges. 
    3. Create a key for the service account. 
    4. Copy the source credentials as displayed below from the IAM service account key created or GCP secrets manager.
5. To read more about service account creation, please read [docs.](https://support.google.com/a/answer/7378726?hl=en)
6. In the `.dlt` folder, you will find `secrets.toml`, which looks like this:

```python
# google sheets source
[sources.google_spreadsheet.credentials]
project_id = "set me up" # GCP Source project ID!
private_key = "set me up" # Unique private key !(Must be copied fully including BEGIN and END PRIVATE KEY)
client_email = "set me up" # Email for source service account
location = "set me up" #Project Location For ex. “US”

#BigQuery Destination
[destination.bigquery.credentials]
client_email = "set me up" # Email for destination service account
private_key = "set me up" # Unique private key !(Must be copied fully including BEGIN and END PRIVATE KEY)
project_id = "set me up" # GCP Destination project ID!
```

7. Add the source credentials with the credentials as [configured above](#configuring-dlt-credentials)
8. Add the credentials required by your destination (e.g. [Google BigQuery](http://localhost:3000/docs/destinations#google-bigquery))

## Share google sheet with the service account

1. In the Google sheet, go to the share button in the top left corner. 

![Share_Button](docs_images/Share_button.png)

2. In Add people and groups, add the source `client_email` ID with at least viewer privileges.

![Add people](docs_images/Add_people.png)

3. Copy the sharing link by clicking on the **copy link** button.

## Configure the pipeline

1. The following two constants are needed to be configured, in **google_sheets_pipelines.py**

```python
# constants
SPREADSHEET_ID = "Set_me_up" # Spread Sheet ID (as per Step Two)
SPREADSHEET_URL = "Set_me_up"  #URL from Google Sheets > Share > Copy Link
```

2. The **SPREADSHEET_URL** can be found in Google Sheets > Share Button ( at top right) > Copy the link. 
3. The copied link is SPREADSHEET_URL.
4. The **SPREADSHEET_ID** can be found in SPREADSHEET_URL, for example. if the SPREADSHEET_URL is as below: 

```python
https://docs.google.com/spreadsheets/d/1VTtCiYgxjAwcIw7UM1_BSaxC3rzIpr0HwXZwd2OlPD4/edit?usp=sharing
```

Then the SPREADSHEET_ID is, between …spreadsheets/d/**SPREADSHEET_ID**/edit?usp=sharing i.e. as below

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

3. Use `dlt pipeline pipedrive_pipeline show` to make sure that everything loaded as expected.

