# @@@SNIPSTART getting_started_index_snippet_incremental_merge
import dlt
from dlt.sources.helpers import requests

@dlt.resource(
    table_name="issues",
    write_disposition="merge",
    primary_key="id",
)
def get_issues(
    updated_at = dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    # NOTE: we read only open issues to minimize number of calls to the API. There's a limit of ~50 calls for not authenticated Github users
    url = f"https://api.github.com/repos/dlt-hub/dlt/issues?since={updated_at.last_value}&per_page=100&sort=updated&directions=desc&state=open"

    while True:
        response = requests.get(url)
        response.raise_for_status()
        yield response.json()

        # get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]

pipeline = dlt.pipeline(
	pipeline_name='github_issues_merge',
	destination='duckdb',
	dataset_name='github_data_merge',
)
load_info = pipeline.run(get_issues)
row_counts = pipeline.last_trace.last_normalize_info
print(row_counts)
print("------")
print(load_info)
# @@@SNIPEND