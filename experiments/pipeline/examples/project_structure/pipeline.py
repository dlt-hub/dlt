import requests
import dlt
from dlt.destinations import bigquery


# explain `dlt.source` a little here and last_id and api_key parameters
@dlt.source
def twitter_data(last_id, api_key):
    # example of Bearer Authentication
    # create authorization headers
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    # explain the `dlt.resource` and the default table naming, write disposition etc.
    @dlt.resource
    def example_data():
        # make a call to the endpoint with request library
        resp = requests.get("https://example.com/data?last_id=%i" % last_id, headers=headers)
        resp.raise_for_status()
        # yield the data from the resource
        data = resp.json()
        # you may process the data here
        # example transformation?
        # return resource to be loaded into `example_data` table
        # explain that data["items"] contains a list of items
        yield data["items"]

    # return all the resources to be loaded
    return example_data

# configure the pipeline
dlt.pipeline(destination=bigquery, dataset="twitter")
# explain that api_key will be automatically loaded from secrets.toml or environment variable below
load_info = twitter_data(0).run()
# pretty print the information on data that was loaded
print(load_info)
