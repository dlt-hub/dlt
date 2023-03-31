# GitHub

This pipeline can be used to load data on issues or pull requests from any GitHub repository onto a [destination](https://dlthub.com/docs/destinations) of your choice.

This pipeline can access the GitHub API from two `dlt` sources:
1. `github_reactions` with the resource end-points `issues` and `pullRequests`.
2. `github_repo_events` with the resource end-point `repo_events`.

## Grab the API auth token
You can optionally add API access tokens to avoid making requests as an unauthorized user. Note: If you wish to load reaction data, then the access token is mandatory.

To get the API token, sign-in to your GitHub account and follow these steps:

1. Click on your profile picture on the top right corner.
2. Choose *Settings*.
3. Select *Developer settings* on the left panel.
4. Under *Personal access tokens*, click on *Generate a personal access token* (preferably under *Tokens(classic)*).
5. Grant at least the following scopes to the token by checking them.


| public_repo | Limits access to public repositories. |
| --- | --- |
| read:repo_hook | Grants read and ping access to hooks in public or private repositories. |
| read:org | Read-only access to organization membership, organization projects, and team membership. |
| read:user | Grants access to read a user's profile data. |
| read:project | Grants read only access to user and organization projects. |
| read:discussion | Allows read access for team discussions. |

7. Finally select *Generate token*.
8. Copy the token and save it somewhere. This will be added later in the `dlt` configuration.

You can learn more about GitHub authentication in the docs [here](https://docs.github.com/en/rest/overview/authenticating-to-the-rest-api?apiVersion=2022-11-28#basic-authentication) and API token scopes [here](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/scopes-for-oauth-apps).

## Initialize the pipeline

Initialize the pipeline with the following command:
```
dlt init github bigquery
```
Here, we chose BigQuery as the destination. To choose a different destination, replace `bigquery` with your choice of destination. 

Running this command will create a directory with the following structure:
```bash
github_pipeline
├── .dlt
│   ├── .pipelines
│   ├── config.toml
│   └── secrets.toml
├── github
│   └── __pycache__
│   └── __init__.py
│   └── queries.py
├── .gitignore
├── github_pipeline.py
└── requirements.txt
```

## Add credentials

1. In the `.dlt` folder, you will find `secrets.toml`, which looks like this:

```bash
# Put your secret values and credentials here
# Note: Do not share this file and do not push it to GitHub!
# Github access token (must be classic for reactions source)
[sources.github]
access_token="GITHUB_API_TOKEN"
[destination.bigquery.credentials] # the credentials required will change based on the destination
project_id = "set me up" # GCP project ID
private_key = "set me up" # Unique private key (including `BEGINand END PRIVATE KEY`)
client_email = "set me up" # Service account email
location = "set me up" # Project location (e.g. “US”)
```

2. Replace `"GITHUB_API_TOKEN"` with the API token you [copied above](#grab-the-api-auth-token) or leave it blank if not specified.
3. Follow the instructions in the [Destinations](https://dlthub.com/docs/destinations) document to add credentials for your chosen destination. 

## Modify the script `github_pipeline.py`

To load the data from the desired repository onto the desired destination, a source method needs to be specified in `github_pipeline.py`. For this, you can either write your own method, or modify one of the three example templates:  

|Function name| Description |
| --- | --- |
| load_duckdb_repo_reactions_issues_only | Loads data on user reactions to issues and user comments on issues from the duckdb repository onto the specified destination. To run this method, it is necessary to add the API access token in `.dlt/secrets.toml`. |
| load_airflow_events | Loads all the events associated with the airflow repository onto the specified destination. |
| load_dlthub_dlt_all_data | Loads data on user reactions to issues, user comments on issues, pull requests, and pull request comments from the `dlt` repository onto the specified destination. To run this method, it is necessary to add the API access token in `.dlt/secrets.toml`. |

Include the source method in the `__main__` block and comment out any other functions. For example, if the source method is `load_airflow_event`, the code block would look as follows:
```python
if __name__ == "__main__" :
    # load_duckdb_repo_reactions_issues_only()
    load_airflow_events()
    # load_dlthub_dlt_all_data()

```

## Run the pipeline

1. Install the necessary dependencies by running the following command:
```
pip install -r requirements.txt
```
2. Now the pipeline can be run by using the command:
```
python3 github_pipeline.py
```
3. To make sure that everything is loaded as expected, use the command:
```
dlt pipeline github_pipeline show
```
## Customize source methods

You can customize the existing templates in `github_pipeline.py` to load from any repository of your choice.

### a. Load GitHub events from any repository

For this, you can modify the method `load_airflow_events`. By default, this method loads events from the Apache Airflow repository (https://github.com/apache/airflow). The owner name of this repository is `apache` and the repository name is `airflow`. To load events from a different repository, change the owner name and the repository name in the function to that of your chosen repository.

The general template for this method is as follows:
```python
def load_<repo_name>_events() -> None: 
    """Loads <repo_name> events. Shows incremental loading. Forces anonymous access token"""
    pipeline = dlt.pipeline("github_events", destination=<destination_name> dataset_name="<repo_name>_events")
    data = github_repo_events(<owner_name>, <repo_name>, access_token="")
    print(pipeline.run(data))
    # does not load same events again
    data = github_repo_events(<owner_name>, <repo_name>, access_token="")
    print(pipeline.run(data))

```
1. By default, <repo_name> is `airflow` and <owner_name> is `apache`. <destination_name> is the destination specified when initiating the `dlt` project.
2. To load events from any other repository, change <repo_name> and <owner_name> in the method to that of the desired repository.
3. The argument `access_token`, if left blank, will make calls to the API anonymously. To add your API access token, include `[set access_token = dlt.secrets.value]`.
4. Lastly, include your source method in the `__main__` block:

```python
if __name__ == "__main__" :
    load_<repo_name>_events()
   
```

### b. Load GitHub reactions from any repository

The template source method for loading user reactions and comments on issues is `load_duckdb_repo_reactions_issues_only`. By default, this method loads data from the duckdb repository(https://github.com/duckdb/duckdb). The owner name and the repository name for this repository is `duckdb`. To load data from a different respository, change the owner name and the repository name in the function to that of your chosen repository. 

The general template for this method is as follows:

```python
def load_<owner_name>_<repo_name>_reactions() -> None:
    """Loads all issues, pull requests and comments for <repo_name> """
    pipeline = dlt.pipeline("github_reactions", destination=<destination_name>, dataset_name="<repo_name>_reactions", full_refresh=True)
    data = github_reactions(<owner_name>, <repo_name>)
    print(pipeline.run(data))

```
1. By default, <repo_name> is `duckdb` and <owner_name> is `duckdb`. <destination_name> is the destination specified when initiating the `dlt` project.
2. To load events from any other repository, change <repo_name> and <owner_name> in the method to that of the desired repository.
3. Use arguments `items_per_page` and `max_items` in `github_reactions` to set limits on the number of items per page and the total number of items. 

```python
data = github_reactions(<owner_name>, <repo_name>, items_per_page=100, max_items=300)
#Limits the items per page to 100 and the total number of items to 300
```
4. To limit the data to a single resource (example "issues"), use the method `with_resource`:

```python
data = github_reactions(<owner_name>, <repo_name>).with_resources("issues")
```
5. Lastly, include your source method in the `__main__` block:

```python
if __name__ == "__main__" :
    load_<owner_name>_<repo_name>_reactions()
   
```