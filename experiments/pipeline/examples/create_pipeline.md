
## Example for the simplest ad hoc pipeline without any structure

```python
import requests
import dlt
from dlt.destinations import bigquery

resp = requests.get(
    "https://taktile.com/api/v2/logs?from_log_id=1",
    headers={"Authorization": dlt.secrets["taktile_api_key"]})
resp.raise_for_status()
data = resp.json()

dlt.run(data["result"], name="logs", destination=bigquery)
```

## Example for endpoint returning only one resource:

```python
import requests
import dlt

# it will use function name `taktile_data` to name the source and schema
@dlt.source
def taktile_data(initial_log_id, taktile_api_key):
    resp = requests.get(
        "https://taktile.com/api/v2/logs?from_log_id=%i" % initial_log_id,
        headers={"Authorization": taktile_api_key})
    resp.raise_for_status()
    data = resp.json()

    # yes you can return a list of values and it will work
    return dlt.resource(data["result"], name="logs")

taktile_data(1).run(destination=bigquery)
# this below also works
# dlt.run(taktile_data(1), destination=bigquery)
```

## With two resources:
also shows how to select just one resource to be loaded

```python
import requests
import dlt

@dlt.source
def taktile_data(initial_log_id, taktile_api_key):
    resp = requests.get(
        "https://taktile.com/api/v2/logs?from_log_id=%i" % initial_log_id,
        headers={"Authorization": taktile_api_key})
    resp.raise_for_status()
    logs = resp.json()["results"]

    resp = requests.get(
        "https://taktile.com/api/v2/decisions%i" % initial_log_id,
        headers={"Authorization": taktile_api_key})
    resp.raise_for_status()
    decisions = resp.json()["results"]

    return dlt.resource(logs, name="logs"), dlt.resource(decisions, name="decisions", write_disposition="replace")

# load all resources
taktile_data(1).run(destination=bigquery)
# load only decisions
taktile_data(1).select("decisions").run(....)
```
note:
`dlt.resource` takes all the parameters (ie. `write_disposition` or `columns` that let you define the table schema fully)

**alternative form which uses iterators** for very long responses that for example use HTTP chunked:

```python
import requests
import dlt

@dlt.source
def taktile_data(initial_log_id, taktile_api_key):

    # it will use the function name `logs` to name the resource/table
    # yield the data which is really long jsonl stream
    @dlt.resource
    def logs():
        resp = requests.get(
            "https://taktile.com/api/v2/logs?from_log_id=%i" % initial_log_id,
            stream=True,
            headers={"Authorization": taktile_api_key})
        resp.raise_for_status()
        for line in resp.text():
            yield json.loads(line)

    # here we provide name and write_disposition directly
    @dlt.resource(name="decisions", write_disposition="replace")
    def decisions_reader():
        resp = requests.get(
            "https://taktile.com/api/v2/decisions%i" % initial_log_id,
            headers={"Authorization": taktile_api_key})
        resp.raise_for_status()
        return resp.json()["results"]

    return logs, decisions_reader
```

## With pipeline state and incremental load


from_log_id = dlt.state.get("from_log_id") or initial_log_id
```python
import requests
import dlt

# it will use function name `taktile_data` to name the source and schema
@dlt.source
def taktile_data(initial_log_id, taktile_api_key):
    from_log_id = dlt.state.get("from_log_id") or initial_log_id
    resp = requests.get(
        "https://taktile.com/api/v2/logs?from_log_id=%i" % initial_log_id,
        headers={"Authorization": taktile_api_key})
    resp.raise_for_status()
    data = resp.json()

    # write state before returning data

    # yes you can return a list of values and it will work
    yield dlt.resource(data["result"], name="logs")


taktile_data(1).run(destination=bigquery)

