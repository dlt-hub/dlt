# @@@SNIPSTART getting_started_index_snippet_replace
import dlt

data = [
    {'id': 1, 'name': 'Alice'},
    {'id': 2, 'name': 'Bob'}
]

pipeline = dlt.pipeline(
    pipeline_name='replace_data',
    destination='duckdb',
    dataset_name='mydata',
)
load_info = pipeline.run(data, table_name="users", write_disposition="replace")
print(load_info)
# @@@SNIPEND


# @@@SNIPSTART getting_started_index_snippet_incremental
import dlt
from dlt.sources.helpers import requests

@dlt.resource(table_name="issues", write_disposition="append")
def get_issues(
    created_at=dlt.sources.incremental("created_at", initial_value="1970-01-01T00:00:00Z")
):
    # url to request dlt-hub issues
    url = f"https://api.github.com/repos/dlt-hub/dlt/issues"

    while True:
        response = requests.get(url)
        page_items = response.json()

        if len(page_items) == 0:
            break
        yield page_items

        if "next" not in response.links:
            break
        url = response.links["next"]["url"]

        # stop requesting pages if the last element was already older than initial value
        # note: incremental will skip those items anyway, we just do not want to use the api limits
        if created_at.start_out_of_range:
            break

pipeline = dlt.pipeline(
    pipeline_name='github_issues',
    destination='duckdb',
    dataset_name='mydata',
)
# dlt works with lists of dicts, so wrap data to the list
load_info = pipeline.run(get_issues)
print(load_info)
# @@@SNIPEND


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
    # url to request dlt-hub issues
    url = f"https://api.github.com/repos/dlt-hub/dlt/issues?since={updated_at.last_value}"

    while True:
        response = requests.get(url)
        page_items = response.json()

        if len(page_items) == 0:
            break
        yield page_items

        if "next" not in response.links:
            break
        url = response.links["next"]["url"]

pipeline = dlt.pipeline(
	pipeline_name='github_issues_merge',
	destination='duckdb',
	dataset_name='mydata',
)
# dlt works with lists of dicts, so wrap data to the list
load_info = pipeline.run(get_issues)
print(load_info)
# @@@SNIPEND