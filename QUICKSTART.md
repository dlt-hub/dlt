# Quickstart Guide: Data Load Tool (DLT)

## **TL;DR: This guide shows you how to load a JSON document into Google BigQuery using DLT.**

![](docs/DLT-Pacman-Big.gif)

## 1. Grab the necessary files

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

*Question: why are we naming the schema?*

d. Name your table
```
parent_table = 'example_table'
```

*Question: why are we naming the table?*

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
pipeline.create_pipeline(credentials)
```

6b. Create the pipeline with your credentials
pipeline.create_pipeline(credentials)

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

b. Error capture - print, raise or handle.

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

b. See results like the following:

table: my_json_doc
```
{  "name": "Ana",  "age": "30",  "id": "456",  "_load_id": "1654787700.406905",  "_record_hash": "5b018c1ba3364279a0ca1a231fbd8d90"}
{  "name": "Bob",  "age": "30",  "id": "455",  "_load_id": "1654787700.406905",  "_record_hash": "afc8506472a14a529bf3e6ebba3e0a9e"}
```

table: my_json_doc__children
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

1. Replace `data.json` with data you want to explore

2. Plug in your own iterator or generator

3. Check that the inferred types are correct in `schema.yml`

4. Set up your own Google BigQuery warehouse (and replace the credentials)

5. Make the necessary transformations (e.g. with dbt) to create a semantic layer / analytical model on top of your new clean staging layer

*question: what does it mean to plug in your own iterator or generator?*

*Question: where should we mention ```pip install python-dlt[redshift]``` for Redshift?*