# @@@SNIPSTART getting_started_index_snippet_incremental
import dlt
from dlt.sources.helpers import requests


@dlt.resource(table_name="issues", write_disposition="append")
def get_issues(
    created_at=dlt.sources.incremental("created_at", initial_value="1970-01-01T00:00:00Z")
):
    # NOTE: we read only open issues to minimize number of calls to the API. There's a limit of ~50 calls for not authenticated Github users
    url = "https://api.github.com/repos/dlt-hub/dlt/issues?per_page=100&sort=created&directions=desc&state=open"

    while True:
        response = requests.get(url)
        response.raise_for_status()
        yield response.json()

        # stop requesting pages if the last element was already older than initial value
        # note: incremental will skip those items anyway, we just do not want to use the api limits
        if created_at.start_out_of_range:
            break

        # get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]


pipeline = dlt.pipeline(
    pipeline_name='github_issues_incremental',
    destination='duckdb',
    dataset_name='github_data_append',
)
load_info = pipeline.run(get_issues)
row_counts = pipeline.last_trace.last_normalize_info
print(row_counts)
print("------")
print(load_info)
# @@@SNIPEND