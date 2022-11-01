
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