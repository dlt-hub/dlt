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