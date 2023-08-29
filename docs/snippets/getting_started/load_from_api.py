# @@@SNIPSTART getting_started_index_snippet_api
import dlt
from dlt.sources.helpers import requests

# url to request dlt-hub followers
url = f"https://api.github.com/users/dlt-hub/followers"
# make the request and check if succeeded
response = requests.get(url)
response.raise_for_status()

pipeline = dlt.pipeline(
    pipeline_name='from_api',
    destination='duckdb',
    dataset_name='github_data',
)
# the response contains a list of followers
load_info = pipeline.run(response.json(), table_name="followers")
print(load_info)
# @@@SNIPEND