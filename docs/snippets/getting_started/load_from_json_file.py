# @@@SNIPSTART getting_started_index_snippet_json
import dlt
from dlt.common import json

with open("json_file.json", 'rb') as file:
    data = json.load(file)

pipeline = dlt.pipeline(
    pipeline_name='from_json',
    destination='duckdb',
    dataset_name='mydata',
)

# NOTE: test data that we load is just a dictionary so we enclose it in a list
# if your JSON contains a list of objects you do not need to do that
load_info = pipeline.run([data], table_name="json_data")
print(load_info)
# @@@SNIPEND