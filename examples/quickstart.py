from dlt.pipeline import Pipeline

# 1. configuration: name your schema, table, pass credentials
schema_prefix = 'demo_'
schema_name = 'example'
parent_table = 'my_json_doc'
gcp_credential_json_file_path = "/Users/adrian/PycharmProjects/sv/dlt/temp/scalevector-1235ac340b0b.json"
# if you re-use an edited schema, then uncomment this part, so you can save it to file
# schema_file_path = "examples/schemas/quickstart.yml"


# 2. Create a pipeline
credential = Pipeline.load_gcp_credentials(gcp_credential_json_file_path, schema_prefix)
pipeline = Pipeline(schema_name)
pipeline.create_pipeline(credential)

# If you want to re-use a curated schema, uncomment the below
# schema = Pipeline.load_schema_from_file(schema_file_path)
# pipeline.create_pipeline(credential, schema=schema)


# 3. Pass the data to the pipeline and give it a table name. Optionally unpack and handle schema.

rows = [{"name": "Ana", "age": 30, "id": 456, "children": [{"name": "Bill", "id": 625},
                                                           {"name": "Elli", "id": 591}
                                                           ]},

        {"name": "Bob", "age": 30, "id": 455, "children": [{"name": "Bill", "id": 625},
                                                           {"name": "Dave", "id": 621}
                                                           ]}
        ]

pipeline.extract(iter(rows), table_name=parent_table)
# tell the pipeline to un-nest the json into a relational structure
pipeline.unpack()

# If you want to save the schema to curate it and re-use it, uncomment the below
# schema = pipeline.get_default_schema()
# schema_yaml = schema.as_yaml()
# f = open(data_schema_file_path, "a")
# f.write(schema_yaml)
# f.close()

# 4. Load
pipeline.load()

# 5. Optional error handling - print, raise or handle.

# now enumerate all complete loads if we have any failed packages
# complete but failed job will not raise any exceptions
completed_loads = pipeline.list_completed_loads()
# print(completed_loads)
for load_id in completed_loads:
    print(f"Checking failed jobs in {load_id}")
    for job, failed_message in pipeline.list_failed_jobs(load_id):
        print(f"JOB: {job}\nMSG: {failed_message}")

# now you can use your data

# Tables created:

# SELECT *  FROM `scalevector.demo__example.my_json_doc`
# {  "name": "Ana",  "age": "30",  "id": "456",  "_load_id": "1654787700.406905",  "_record_hash": "5b018c1ba3364279a0ca1a231fbd8d90"}
# {  "name": "Bob",  "age": "30",  "id": "455",  "_load_id": "1654787700.406905",  "_record_hash": "afc8506472a14a529bf3e6ebba3e0a9e"}


# SELECT * FROM `scalevector.demo__example.my_json_doc__children` LIMIT 1000

# {"name": "Bill", "id": "625", "_parent_hash": "5b018c1ba3364279a0ca1a231fbd8d90", "_pos": "0", "_root_hash": "5b018c1ba3364279a0ca1a231fbd8d90", "_record_hash": "7993452627a98814cc7091f2c51faf5c"}
# {"name": "Bill", "id": "625", "_parent_hash": "afc8506472a14a529bf3e6ebba3e0a9e", "_pos": "0", "_root_hash": "afc8506472a14a529bf3e6ebba3e0a9e", "_record_hash": "9a2fd144227e70e3aa09467e2358f934"}
# {"name": "Dave", "id": "621", "_parent_hash": "afc8506472a14a529bf3e6ebba3e0a9e", "_pos": "1", "_root_hash": "afc8506472a14a529bf3e6ebba3e0a9e", "_record_hash": "28002ed6792470ea8caf2d6b6393b4f9"}
# {"name": "Elli", "id": "591", "_parent_hash": "5b018c1ba3364279a0ca1a231fbd8d90", "_pos": "1", "_root_hash": "5b018c1ba3364279a0ca1a231fbd8d90", "_record_hash": "d18172353fba1a492c739a7789a786cf"}

# and we can join them via auto generated keys
# select p.name, p.age, p.id as parent_id,
#        c.name as child_name, c.id as child_id, c._pos as child_order_in_list
# from `scalevector.demo__example.my_json_doc` as p
# left join `scalevector.demo__example.my_json_doc__children`  as c
#     on p._record_hash = c._parent_hash

# {  "name": "Ana",  "age": "30",  "parent_id": "456",  "child_name": "Bill",  "child_id": "625",  "child_order_in_list": "0"}
# {  "name": "Ana",  "age": "30",  "parent_id": "456",  "child_name": "Elli",  "child_id": "591",  "child_order_in_list": "1"}
# {  "name": "Bob",  "age": "30",  "parent_id": "455",  "child_name": "Bill",  "child_id": "625",  "child_order_in_list": "0"}
# {  "name": "Bob",  "age": "30",  "parent_id": "455",  "child_name": "Dave",  "child_id": "621",  "child_order_in_list": "1"}
