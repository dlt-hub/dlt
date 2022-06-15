# Quickstart Guide: Data Load Tool (DLT)

## **TL;DR: This guide shows you how to load a JSON document into Google BigQuery using DLT.**

![](docs/DLT-Pacman-Big.gif)

## 1. Grab the demo

a. Clone the example repository:
```
git clone https://github.com/scale-vector/dlt-quickstart-example.git
```

b. Enter the directory:
```
cd dlt-quickstart-example
```

c. Open the files in your favorite IDE / text editor:
- `data.json` (i.e. the JSON document you will load)
- `credentials.json` (i.e. contains the credentials to our demo Google BigQuery warehouse)
- `quickstart.py` (i.e. the script that uses DLT)

## 2. Set up a virtual environment

a. Ensure you are using either Python 3.8 or 3.9:
```
python3 --version
```

b. Create a new virtual environment:
```
python3 -m venv ./env
```

c. Activate the virtual environment:
```
source ./env/bin/activate
```

## 3. Install DLT and support for the target data warehouse

a. Install DLT using pip:
```
pip3 install python-dlt
```

b. Install support for Google BigQuery:
```
pip3 install python-dlt[gcp]
```

## 4. Configure DLT

a. Import necessary libaries
```
import base64
import json
from dlt.common.utils import uniq_id
from dlt.pipeline import Pipeline, GCPPipelineCredentials
```

b. Create a unique prefix for your demo Google BigQuery table
```
schema_prefix = 'demo_' + uniq_id()[:4]
```

c. Name your schema
```
schema_name = 'example'
```

d. Name your table
```
parent_table = 'json_doc'
```

e. Specify your schema file location
```
schema_file_path = "schema.yml"
```

f. Load credentials
```
f = open('credentials.json')
gcp_credentials_json = json.load(f)
f.close()

# Private key needs to be decoded (because we don't want to store it as plain text)
gcp_credentials_json["private_key"] = bytes([_a ^ _b for _a, _b in zip(base64.b64decode(gcp_credentials_json["private_key"]), b"quickstart-sv"*150)]).decode("utf-8")
credentials = GCPPipelineCredentials.from_services_dict(gcp_credentials_json, schema_prefix)
```

## 5. Create a DLT pipeline

a. Instantiate a pipeline
```
pipeline = Pipeline(schema_name)
```

b. Create the pipeline with your credentials
```
pipeline.create_pipeline(credentials)
```

## 6. Load the data from the JSON document

a. Load JSON document into a dictionary
```
f = open('data.json')
data = json.load(f)
f.close()
```

## 7. Pass the data to the DLT pipeline

a. Extract the dictionary into a table
```
pipeline.extract(iter(data), table_name=parent_table)
```

b. Unpack the pipeline into a relational structure
```
pipeline.unpack()
```

c. Save schema to `schema.yml` file
```
schema = pipeline.get_default_schema()
schema_yaml = schema.as_yaml()
f = open(schema_file_path, "a")
f.write(schema_yaml)
f.close()
```

*question: do we really want to append to the schema file?*

## 8. Use DLT to load the data

a. Load
```
pipeline.load()
```

b. Make sure there are no errors
```
completed_loads = pipeline.list_completed_loads()
# print(completed_loads)
# now enumerate all complete loads if we have any failed packages
# complete but failed job will not raise any exceptions
for load_id in completed_loads:
    print(f"Checking failed jobs in {load_id}")
    for job, failed_message in pipeline.list_failed_jobs(load_id):
        print(f"JOB: {job}\nMSG: {failed_message}")
```

c. Run the script:
```
python3 quickstart.py
```

d. Inspect `schema.yml` that has been generated
```
vim schema.yml
```

## 9. Query the Google BigQuery table

a. Run SQL queries
```
def run_query(query):
    df = c._execute_sql(query)
    print(query)
    print(list(df))
    print()

with pipeline.sql_client() as c:

    # Query table for parents
    query = f"SELECT * FROM `{schema_prefix}_example.json_doc`"
    run_query(query)

    # Query table for children
    query = f"SELECT * FROM `{schema_prefix}_example.json_doc__children` LIMIT 1000"
    run_query(query)

    # Join previous two queries via auto generated keys
    query = f"""
        select p.name, p.age, p.id as parent_id,
            c.name as child_name, c.id as child_id, c._pos as child_order_in_list
        from `{schema_prefix}_example.json_doc` as p
        left join `{schema_prefix}_example.json_doc__children`  as c
            on p._record_hash = c._parent_hash
    """
    run_query(query)
```

b. See results like the following:

table: json_doc
```
{  "name": "Ana",  "age": "30",  "id": "456",  "_load_id": "1654787700.406905",  "_record_hash": "5b018c1ba3364279a0ca1a231fbd8d90"}
{  "name": "Bob",  "age": "30",  "id": "455",  "_load_id": "1654787700.406905",  "_record_hash": "afc8506472a14a529bf3e6ebba3e0a9e"}
```

table: json_doc__children
```
    # {"name": "Bill", "id": "625", "_parent_hash": "5b018c1ba3364279a0ca1a231fbd8d90", "_pos": "0", "_root_hash": "5b018c1ba3364279a0ca1a231fbd8d90",
    #   "_record_hash": "7993452627a98814cc7091f2c51faf5c"}
    # {"name": "Bill", "id": "625", "_parent_hash": "afc8506472a14a529bf3e6ebba3e0a9e", "_pos": "0", "_root_hash": "afc8506472a14a529bf3e6ebba3e0a9e",
    #   "_record_hash": "9a2fd144227e70e3aa09467e2358f934"}
    # {"name": "Dave", "id": "621", "_parent_hash": "afc8506472a14a529bf3e6ebba3e0a9e", "_pos": "1", "_root_hash": "afc8506472a14a529bf3e6ebba3e0a9e",
    #   "_record_hash": "28002ed6792470ea8caf2d6b6393b4f9"}
    # {"name": "Elli", "id": "591", "_parent_hash": "5b018c1ba3364279a0ca1a231fbd8d90", "_pos": "1", "_root_hash": "5b018c1ba3364279a0ca1a231fbd8d90",
    #   "_record_hash": "d18172353fba1a492c739a7789a786cf"}
```

SQL result:
```
    # {  "name": "Ana",  "age": "30",  "parent_id": "456",  "child_name": "Bill",  "child_id": "625",  "child_order_in_list": "0"}
    # {  "name": "Ana",  "age": "30",  "parent_id": "456",  "child_name": "Elli",  "child_id": "591",  "child_order_in_list": "1"}
    # {  "name": "Bob",  "age": "30",  "parent_id": "455",  "child_name": "Bill",  "child_id": "625",  "child_order_in_list": "0"}
    # {  "name": "Bob",  "age": "30",  "parent_id": "455",  "child_name": "Dave",  "child_id": "621",  "child_order_in_list": "1"}
```

## 10. Next steps

a. Replace `data.json` with data you want to explore

b. Check that the inferred types are correct in `schema.yml`

c. Set up your own Google BigQuery warehouse (and replace the credentials)

d. Use this new clean staging layer as the starting point for a semantic layer / analytical model (e.g. using dbt)
