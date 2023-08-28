# create test json file
import tempfile
import json

# Create a temporary file in the system's temporary directory
with tempfile.NamedTemporaryFile(mode='w') as temp_file:
    # Create a dictionary with some sample JSON data
    data = {'id': 1, 'name': 'Alice', 'children': {'id': 1, 'name': 'Eve'}}

    # Write the JSON data to the temporary file
    json.dump(data, temp_file)

    # Get the path of the temporary file
    json_path = temp_file.name

    # Move the file pointer back to the beginning of the file
    temp_file.seek(0)

    # @@@SNIPSTART getting_started_index_snippet_json
    import json
    import dlt

    with open(json_path, 'r') as file:
        data = json.load(file)

    pipeline = dlt.pipeline(
        pipeline_name='from_json',
        destination='duckdb',
        dataset_name='mydata',
    )
    # dlt works with lists of dicts, so wrap data to the list
    load_info = pipeline.run([data], table_name="json_data")
    print(load_info)
    # @@@SNIPEND


# Create a temporary file in the system's temporary directory
with tempfile.NamedTemporaryFile(mode='w') as temp_file:
    # create test csv file
    import pandas as pd

    df = pd.DataFrame([
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'}
    ])
    # Get the path of the temporary file and save
    csv_path = temp_file.name
    df.to_csv(csv_path, index=False)

    # Move the file pointer back to the beginning of the file
    temp_file.seek(0)

    # @@@SNIPSTART getting_started_index_snippet_csv
    import dlt
    import pandas as pd

    df = pd.read_csv(csv_path)
    data = df.to_dict(orient='records')

    pipeline = dlt.pipeline(
        pipeline_name='from_csv',
        destination='duckdb',
        dataset_name='mydata',
    )
    load_info = pipeline.run(data, table_name="csv_data")
    print(load_info)
    # @@@SNIPEND



# @@@SNIPSTART getting_started_index_snippet_api
import dlt
import requests

# url to request dlt-hub followers
url = f"https://api.github.com/users/dlt-hub/followers"
# make the request and return the json
data = requests.get(url).json()

pipeline = dlt.pipeline(
    pipeline_name='from_api',
    destination='duckdb',
    dataset_name='mydata',
)
# dlt works with lists of dicts, so wrap data to the list
load_info = pipeline.run([data], table_name="followers")
print(load_info)
# @@@SNIPEND


# @@@SNIPSTART getting_started_index_snippet_db
import dlt
import duckdb

db_name = "from_api.duckdb"  # Replace this with the path to your DuckDB database file
sql_query = "SELECT * FROM mydata.followers"  # Replace with your SQL query

# connect to the DuckDB
con = duckdb.connect(db_name)
# execute SQL query and fetch result
result = con.execute(sql_query).fetch_df()
# close the connection
con.close()
data = result.to_dict(orient="records")

pipeline = dlt.pipeline(
    pipeline_name='from_database',
    destination='duckdb',
    dataset_name='mydata',
)
load_info = pipeline.run(data, table_name="data_table")
print(load_info)
# @@@SNIPEND
