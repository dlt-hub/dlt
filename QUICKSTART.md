# Quickstart Guide: Data Load Tool (DLT)

## **TL;DR: This guide show you how to load a JSON document into Google BigQuery using DLT.**

## 1. Grab the files needed

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

*Question: where should we mention ```pip install python-dlt[redshift]``` for Redshift?*

## 5. Configure DLT

a. Name your schema

b. Name your table

c. Pass credentials

## 6. Create a pipeline

a. Instantiate a pipeline

b. Reuse existing schema

c. Create the pipeline with your credentials

*Question: what happens if schema file is non-existent?*

## 7. Load the data from the JSON document

a. Load JSON document into a dictionary

## 8. Pass the data to the pipeline and give it a table name.

a. Extract the dictionary into a SQL table

b. Unpack the pipeline into a relational structure

c. Save schema to file

*question: do we really want to append to the schema file?*

## 9. Load

a. Load

b. Error capture - print, raise or handle.

c. Inspect `schema.yml` that has been generated

## 10. Try querying the Google BigQuery table

a. Run SQL code

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

### 11. Run it yourself
- replace data.json with your own file
- plug your own iterator or generator
- check that the types are correct
- set up your own google bigquery
- create semantic model on top of the data