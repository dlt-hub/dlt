---
title: Zendesk
description: dlt pipeline for Zendesk API
keywords: [zendesk api, zendesk pipeline, zendesk]
---
# Zendesk

:::info
Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our slack community](https://dlthub-community.slack.com/join/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g) or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::


Zendesk is a cloud-based customer service and support platform. It offers a range of features, including ticket management, self-service options, knowledge base management, live chat, customer analytics, and talks.

Using this guide, you can set up a pipeline that can automatically load data from three possible Zendesk API Clients ([Zendesk support](https://developer.zendesk.com/api-reference/ticketing/introduction/), [Zendesk chat](https://developer.zendesk.com/api-reference/live-chat/introduction/), [Zendesk talk](https://developer.zendesk.com/api-reference/voice/talk-api/introduction/)) onto a [destination](../destinations/) of your choice.

## Get API credentials

Before running the pipeline, you will need to get API credentials. You do not need to get credentials for all of the APIs, but only for those from which you wish to request data. The steps for this are detailed below:

Credentials for Zendesk support API

Zendesk support can be authenticated using any one of the following:

1. [subdomain](#subdomain) + email address + password
2. [subdomain](#subdomain) + email address + [API token](#zendesk-support-api-token)
3. [subdomain](#subdomain) + [OAuth token](#zendesk-support-oauth-token)

The simplest way to authenticate is via subdomain + email address + password, since these details are already available and you don't have to generate any tokens. Alternatively, you can also use API tokens or OAuth tokens.

### Subdomain
1. To get the subdomain, simply login to your Zendesk account and grab it from the url.
2. For example, if the url is https://www.dlthub.zendesk.com, then the subdomain will be `dlthub`.


### Zendesk support API token

1.  Go to Zendesk products in the top right corner and select Admin Center.

  <img src="https://raw.githubusercontent.com/dlt-hub/dlt/devel/docs/website/docs/dlt-ecosystem/verified-sources/docs_images/Zendesk_admin_centre.png" alt="Admin Centre" width="400"/>


2. Select Apps and Integrations.

3. In the left pane, under APIs, choose Zendesk API from the menu on the left, and enable the “**Password access**” and “**Token access**” as shown below.


<img src="https://raw.githubusercontent.com/dlt-hub/dlt/devel/docs/website/docs/dlt-ecosystem/verified-sources/docs_images/Zendesk_token_access.png" alt="Token Access" width = "70%" />

4. Click on “**Add API token**”, enter a description, and note the `API token`.

    ***********This token will be displayed only once and should be noted***********
### Zendesk support OAuth token
To get an `OAuth token` follow these steps:
1.  Go to Zendesk products in the top right corner and select Admin Center.

  <img src="https://raw.githubusercontent.com/dlt-hub/dlt/devel/docs/website/docs/dlt-ecosystem/verified-sources/docs_images/Zendesk_admin_centre.png" alt="Admin Centre" width="400"/>

2. Select Apps and Integrations.
3. In the left pane, under APIs, choose Zendesk API from the menu on the left and go to “OAuth Clients” tab.
4. Click on “Add OAuth Client” and add the details like “Client Name”, “Description”, “Company” , “Redirect URL (if any)”.
5. Click on save, and a secret token will be displayed, copy it.
6. Now you need to make a curl request using the following command

```bash
    curl https://{subdomain}.zendesk.com/oauth/tokens \
      -H "Content-Type: application/json" \
      -d '{"grant_type": "password", "client_id": "{client_name}",
        "client_secret": "{your_client_secret}", "scope": "read",
        "username": "{zendesk_username}", "password": "{zendesk_password}"}' \
      -X POST
```

  Alternatively, you can use the following Python script:

  ```python
    import requests
    import json

    subdomain = "set_me_up"
    client_name = "set_me_up" # generated in the steps above
    client_secret = "set_me_up" # generated in the steps above
    zendesk_username = "set_me_up" # zendesk email address
    zendesk_password = "set_me_up" # zendesk password

    url = f'https://{subdomain}.zendesk.com/oauth/tokens'
    headers = {'Content-Type': 'application/json'}
    data = {
        'grant_type': 'password',
        'client_id': client_name,
        'client_secret': client_secret,
        'scope': 'read',
        'username': zendesk_username,
        'password': zendesk_password
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))

    print(response.json()['access_token'])
  ```

7. Include the following in the code above:

| Credentials | Description |
| --- | --- |
| subdomain | Your Zendesk subdomain |
| client_name  | Unique identifier given to the OAuth client created above |
| client_secret | secret token generated for the OAuth client |
| zendesk_username  | Your Zendesk email address |
| zendesk password | Your Zendesk password |

8. After running the above curl command in terminal (or the Python script), you will get an access token in the response.

9. This is the OAuth token. Save it, as this will need to be added to the pipeline.

Credentials for Zendesk chat API

To authenticate Zendesk chat, you will need the following credentials:

[subdomain](#subdomain-1) + [OAuth token](#zendesk-chat-oauth-token)

(Please note that the OAuth token for Zendesk chat and Zendesk support are different, and you need to follow different procedures to generate each.)


### Subdomain
1. To get the subdomain, simply login to your Zendesk account and grab it from the url.
2. For example, if the url is https://www.dlthub.zendesk.com, then the subdomain will be `dlthub`.

### Zendesk chat OAuth token

1. Login to Zendesk chat. Or go to “Chat” using Zendesk products in the top right corner.

  <img src="https://raw.githubusercontent.com/dlt-hub/dlt/devel/docs/website/docs/dlt-ecosystem/verified-sources/docs_images/Zendesk_admin_centre.png" alt="Admin Centre" width="400"/>

2. In Zendesk chat, go to **Settings**(on the left) **> Account > API > Add API client.**
3. Enter the details like client name, company, and redirect URLs (if you don’t have redirect URLs; use: [http://localhost:8080](http://localhost:8080/)).
4. Note down the displayed `client ID` and `secret`.
5. The simplest way to get Zendesk chat `OAuth token` is to use the URL given below.
```bash
https://www.zopim.com/oauth2/authorizations/new?response_type=token&redirect_uri=http%3A%2F%2Flocalhost%3A8080&client_id={client_id}&scope=read&subdomain={subdomain_name}
```
For more information or an alternative method, see the [documentation](https://developer.zendesk.com/documentation/live-chat/getting-started/auth/#authorization-code-grant-flow).
6. In the URL, replace `client_id` and `subdomain_name` with your client ID and subdomain. (***also remove the curly brackets***)
7. Paste it in a browser and hit enter.
8. Click on Allow.
9. After the redirect, the secret token will be displayed in the address bar of the browser as below:
```bash
http://localhost:8080/#**access_token=cSWY9agzy9hsgsEdX5F2PCsBlvSu3tDk3lh4xmISIHFhR4lKtpVqqDRVvkiZPqbI**&token_type=Bearer&scope=read

#access token is "**cSWY9agzy9hsgsEdX5F2PCsBlvSu3tDk3lh4xmISIHFhR4lKtpVqqDRVvkiZPqbI"**
```
10. Save the access token. This will need to be added to the pipeline later.


Credentials for Zendesk talk API

1. The method for getting the credentials for Zendesk talk is the same as that for [Zendesk support](#grab-zendesk-support-credentials).
2. You can reuse the same credentials from Zendesk support or generate new ones.

## Initialize the pipeline

Initialize the pipeline with the following command:

```bash
dlt init zendesk duckdb
```

Here, we chose duckdb as the destination. To choose a different destination, replace `duckdb` with your choice of destination.


## Add credentials

1. Add credentials for the Zendesk API and your chosen destination in `.dlt/secrets.toml`.

```python
#Zendesk support credentials
[sources.zendesk.zendesk_support.credentials]
password = "set me up" # Include this if you with to authenticate using subdomain + email address + password
subdomain = "subdomian" # Copy subdomain from the url https://[subdomain].zendesk.com
token = "set me up" # Include this if you wish to authenticate using the API token
email = "set me up" # Include this if you with to authenticate using subdomain + email address + password
oauth_token = "set me up" # Include this if you wish to authenticate using OAuth token

[sources.zendesk.zendesk_chat.credentials]
subdomain = "subdomian". # Copy subdomain from the url https://[subdomain].zendesk.com
oauth_token = "set me up" # Follow the steps under Zendesk chat credentials to get this token

#Zendesk talk credentials
[sources.zendesk.zendesk_talk.credentials]
password = "set me up" # Include this if you with to authenticate using subdomain + email address + password
subdomain = "subdomian" # Copy subdomain from the url https://[subdomain].zendesk.com
token = "set me up" # Include this if you wish to authenticate using the API token
email = "set me up" # Include this if you with to authenticate using subdomain + email address + password
oauth_token = "set me up" # Include this if you wish to authenticate using OAuth token
```

2. Only add credentials for the APIs from which you wish to request data and remove the rest.
3. Add credentials as required by your destination. See [here](../destinations/) for steps on how to do this.

## Specify source methods in `zendesk_pipeline.py`

1. You can easily specify which APIs the pipeline will load the data from by modifying the data loading function `incremental_load_all_default` in the script `zendesk_pipeline.py`.
2. By default, the function calls all three source methods, `zendesk_support()`, `zendesk_chat()`, and `zendesk_talk()`.
3. To adjust this, simply remove the lines that correspond to the APIs from which you do not wish to request data.
4. Also make the corresponding change in the line `info = pipeline.run(data=[data_support, data_chat, data_talk])`.

```python
def incremental_load_all_default():
    """
    Loads all possible tables for Zendesk Support, Chat, Talk
    """
    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(pipeline_name="dlt_zendesk_pipeline", destination="duckdb", full_refresh=True, dataset_name="sample_zendesk_data3")

    # zendesk support source function
    data_support = zendesk_support(load_all=True)
    # zendesk chat source function
    data_chat = zendesk_chat()
    # zendesk talk source function
    data_talk = zendesk_talk()
    # run pipeline with all 3 sources
    info = pipeline.run(data=[data_support, data_chat, data_talk])
    return info
```

## Run the pipeline

1. Install requirements for the pipeline by running the following command:

`pip install -r requirements.txt`

2. Run the pipeline with the following command:

`python3 zendesk_pipeline.py`

3. To make sure everything is loaded as expected, use the command:

`dlt pipeline zendesk_pipeline show`


# Customizations

The Zendesk pipeline has some default customizations that make it more useful:

1. **Pivoting ticket fields:** By default, the pipeline pivots the custom fields in tickets when loading the data, which allows the custom fields to be used as columns after loading. This behavior is due to the fact that the boolean parameter `pivot_ticket_fields` in the source method `zendesk_support()` is set to `True` by default. To change this, set `pivot_ticket_fields=False` when calling the source method from inside the data loading function.
```python
data_support = zendesk_support(pivot_ticket_fields=False)
```

  Alternatively, this can be explicitly done by using the function `load_support_with_pivoting` in the script `zendesk_pipeline.py`.
  ```python
  def load_support_with_pivoting():
    """
    Loads Zendesk Support data with pivoting. Simply done by setting the pivot_ticket_fields to true - default option. Loads only the base tables.
    """
    pipeline = dlt.pipeline(pipeline_name="zendesk_support_pivoting", destination='duckdb', full_refresh=False)
    data = zendesk_support(load_all=False, pivot_ticket_fields=True)
    info = pipeline.run(data=data)
    return info
  ```
  Simply include this function in the `__main__` block when running the pipeline.
  ```python
  if __name__ == "__main__":
    load_support_with_pivoting()
  ```

2. **Custom field rename**: The pipeline loads the custom fields with their label names. If the label changes between loads, the initial label continues to be used (is persisted in state). To reset the labels, do a full refresh. This can be achieved by passing `full_refresh=True` when calling `dlt.pipeline`:
```python
pipeline = dlt.pipeline(pipeline_name="dlt_zendesk_pipeline", destination='duckdb', full_refresh=True, dataset_name="sample_zendesk_data3")
```
