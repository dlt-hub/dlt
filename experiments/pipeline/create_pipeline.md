## Mockup code for generic template credentials

This is a toml file, for BigQuery credentials destination and instruction how to add source credentials.

I assume that new pipeline is accessing REST API

```toml
# provide credentials to `taktile` source below, for example
# api_key = "api key to access taktile endpoint"

[gcp_credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```

## Mockup code for taktile credentials

```toml
taktile_api_key="96e6m3/OFSumLRG9mnIr"

[gcp_credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```


## Mockup code pipeline script template with nice UX

This is a template made for BiqQuery destination and the source named `taktile`. This already proposes a nice structure for the code so the pipeline may be developed further.


```python
import requests
import dlt

# The code below is an example of well structured pipeline
# @Ty if you want I can write more comments and explanations

@dlt.source
def taktile_data():
    # retrieve credentials via DLT secrets
    api_key = dlt.secrets["api_key"]

    # make a call to the endpoint with request library
    resp = requests.get("https://example.com/data", headers={"Authorization": api_key"})
    resp.raise_for_status()
    data = resp.json()

    # you may process the data here

    # return resource to be loaded into `data` table
    return dlt.resource(data, name="data")

dlt.run(taktile_data(), destination="bigquery")
```


## Mockup code of taktile pipeline script with nice UX

Example for the simplest ad hoc pipeline without any structure

```python
import requests
import dlt

resp = requests.get(
    "https://taktile.com/api/v2/logs",
    headers={"Authorization": dlt.secrets["taktile_api_key"]})
resp.raise_for_status()
data = resp.json()

dlt.run(data["result"], name="logs", destination="bigquery")
```

Example for endpoint returning only one resource:

```python
import requests
import dlt

@dlt.source
def taktile_data():
    resp = requests.get(
        "https://taktile.com/api/v2/logs",
        headers={"Authorization": dlt.secrets["taktile_api_key"]})
    resp.raise_for_status()
    data = resp.json()

    return dlt.resource(data["result"], name="logs")

dlt.run(taktile_data(), destination="bigquery")
```

With two resources:

```python
import requests
import dlt

@dlt.source
def taktile_data():
    resp = requests.get(
        "https://taktile.com/api/v2/logs",
        headers={"Authorization": dlt.secrets["taktile_api_key"]})
    resp.raise_for_status()
    logs = resp.json()["results"]

    resp = requests.get(
        "https://taktile.com/api/v2/decisions",
        headers={"Authorization": dlt.secrets["taktile_api_key"]})
    resp.raise_for_status()
    decisions = resp.json()["results"]

    return dlt.resource(logs, name="logs"), dlt.resource(decisions, name="decisions")

dlt.run(taktile_data(), destination="bigquery")
```
