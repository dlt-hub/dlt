# GitHub

Here you will find a setup guide for the [GitHub](https://docs.github.com/en) pipeline.

This is a pipeline created for the purpose of analyzing the reactions on github issues or pull requests for popular repositories

Github source contains 2 sources:

- **github_reactions** with resources:
    - `issues`
    - `pullRequests`
- **github_repo_events** with resource:
    - `repo_events`

Arguments:
  1. You can use an example repository from our sample pipelines or find the repository you want to track and get the owner name and repo name. For example, for apache’s airflow found at https://github.com/apache/airflow the owner is apache, and the repository is airflow.
  2. You can also get an access token https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token. If you do not use one, the calls will be made anonymously.

## Initialize the pipeline
**Initialize the pipeline by using the following command with your [destination](/destinations.md) of choice:**

```
dlt init github [destination]
```

This will create a directory that includes the following file structure:
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

## Grab the API auth token

**On GitHub:**
1. Go to your profile picture in the top right corner
2. Select the Settings page
3. Go to Developer settings
4. Select Personal access tokens
5. Go to Generate new token (preferably *classic token*)
6. Scopes defines access for personal tokens. Grant at least the following scopes to the token by checking them
    
    
|permission|description|
| --- | --- |
| public_repo | Limits access to public repositories. |
| read:repo_hook | Grants read and ping access to hooks in public or private repositories. |
| read:org | Read-only access to organization membership, organization projects, and team membership. |
| read:user | Grants access to read a user's profile data. |
| read:project | Grants read only access to user and organization projects. |
| read:discussion | Allows read access for team discussions. |
7. Go to Generate token
8. Copy the token(to be used in the dlt configuration)

You can learn more about GitHub authentication in the docs [here](https://docs.github.com/en/rest/overview/authenticating-to-the-rest-api?apiVersion=2022-11-28#basic-authentication) and API token scopes [here](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/scopes-for-oauth-apps).

## Configure `dlt` credentials

To use credentials, see below:

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

2. Replace **`GITHUB_API_TOKEN`** with the API token you [copied above](#grab-the-api-auth-token)
3. Add the credentials required by your destination (e.g. [Google BigQuery](http://localhost:3000/docs/destinations#google-bigquery))

## Configuring the pipeline

The python file `github_pipeline.py` has three example source templates that can be customized as per your need. These are the following functions in the template

|Function name| Description |
| --- | --- |
| load_duckdb_repo_reactions_issues_only | Loads user's reactions to the issues and issue comments into the destination |
| load_airflow_events | Loads all the events associated with airflow repo into the destination |
| load_dlthub_dlt_all_data | Loads user’s reactions to issues, issues comment, pull requests and pull request comments into the destination |

You can select to run any one function in the main method.

## Customizing the pipeline

You can customize `github_pipeline.py` to load **GitHub events** or **GitHub reactions** for a repo of your choice as follows

### a. To load the GitHub events

To load the GitHub events, the following code can be used

1. Use the GitHub owner's username as `github_username`
2. Use the name of the repository as `repo_name`

For example in this URL [*https://github.com/dlt-hub/dlt*],  `dlt-hub` is github_username and `dlt` is the name of the repository.

```python
def load_{repo_name}_events() -> None: 
    """Loads repo events. Shows incremental loading. Forces anonymous access token"""
    pipeline = dlt.pipeline("github_events", destination='bigquery', dataset_name="repo_events")
    data = github_repo_events("{github_username}", "{repo_name}", access_token="")
    print(pipeline.run(data))
    # does not load same events again
    data = github_repo_events("{github_username}", "{repo_name}", access_token="")
    print(pipeline.run(data))

#remove the curly braces before github_username and repo_name
```

3. In the above method access_token if left blank will make try to make calls to API anonymously.
4. To make calls from your GitHub account `[set access_token = dlt.secrets.value]`
5. In the main method, set the function

```python
if __name__ == "__main__" :
    load_{repo_name}_events()

   # if the reponame is dlt then use **load_dlt_events()**
```

### b. To load the GitHub reactions

To load the GitHub reactions, the following code can be used 

1. Use the GitHub owner's username as `github_username`
2. Use the name of the repository as `repo_name`

For example in this URL [*https://github.com/dlt-hub/dlt*], `dlt-hub` is github_username and `dlt` is the name of the repository.

```python
def load_{github_username}_{repo_name}_reactions() -> None:
    """Loads all issues, pull requests and comments for the repo """
    pipeline = dlt.pipeline("github_reactions", destination="bigquery", dataset_name="{repo_name}_reactions", full_refresh=True)
    data = github_reactions("{github_username}", "{repo_name}")
    print(pipeline.run(data))

#remove the curly braces before github_username and repo_name
```

3. To set items per page and max items in the pipeline you can modify code in the above function

```python
data = github_reactions("{github_username}", "{repo_name}", items_per_page=100, max_items=300)

#Say items per page is 100 and max items is 300.
```

4. To select only the reactions for a particular resource say “issues”, you can modify the code as follows

```python
data = github_reactions("{github_username}", "{repo_name}").with_resources("issues")
```

5. In the main method, set the function to run

## Run the pipeline

1. Install requirements for the pipeline by running the following command:
```
pip install -r requirements.txt

```
2. Run the pipeline with the following command:
```
python3 github_pipeline.py
```
3. Use `dlt pipeline github_pipeline show` to make sure that everything loaded as expected.