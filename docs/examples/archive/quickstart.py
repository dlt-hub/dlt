import base64
from typing import Any

import dlt

"""
This is example of ad-hoc pipeline where the data is directly passed to `run` function, schema hints are explicit and credentials are passed directly
or with the use of environment variables
"""

# 1. configuration: name your dataset, table, pass credentials
dataset_name = "dlt_quickstart"
pipeline_name = "dlt_quickstart"
table_name = "my_json_doc"

gcp_credentials_json = {
    "type": "service_account",
    "project_id": "zinc-mantra-353207",
    "private_key": "XFhETkYxMSY7Og0jJDgjKDcuUz8kK1kAXltcfyQqIjYCBjs2bDc3PzcOCBobHwg1TVpDNDAkLCUqMiciMD9KBBEWJgIiDDY1IB09bzInMkAdMDtCFwYBJ18QGyR/LBVEFQNQOjhIB0UXHhFSOD4hDiRMYCYkIxgkMTgqJTZBOWceIkgHPCU6EiQtHyRcH0MmWh4xDjowBkcMGSY8I38cLgk6NVYAGEU3ExcvPVQvBUYyIS5BClkyHB4MPkATM0BCeFwcFS9dNg8AJA40B0pYJUUxAjkbCzhZQj9mODk6f0Y6JRUBJyQhZysEWkU8MwU1LCsELF4gBStNWzsHAh4PXTVAOxA3PSgJUksFFgAwVxkZGiMwJT4UEgwFEn8/FRd/O1UmKzYRH19kCjBaLCAGIB0VUVk+Bh0zJzQtElJKOBIFAGULRQY7BVInOSAoGBdaMCYgIhMnCBhfNQsDFABFIH8+MD0JBjM0PEQxBwRGXwAiIBkoExgcFCYQQzE6AUAHCCQzSjpdKwcYFAIkHg1CG0o3NSBMEztEBQRYCgB9NwQofw8FOAohDzgCbBQ7MzQoJigUEyQzJlsWNRk7CxYDJS43Jj5BIj5IQQ8UPUtELURCRjBHFRcZMzs+MVAgAmQfGyJ/JjcTHgVWBzBJXEQ6TRgHXD0YCUI7fDQVAiUCMCALM1MbBxw8LCkCJQEySwIZNTJDSyBBJCE0OgsBIkBGSwkfEH8DUjlKM1E+H30nGxwAMxYpG0IpMARoA08dDQFWExs/Lh06VT0hHicQNlsiQQIHDE4UAV4ABAAjMkMFPTB9ISU3fws2GysuBBo1GR84OCJQWgdLBCg3R0Y8FwIYDUwACyAmOR1GIUYgBw86DDIFKkcRXkE9Exo6ERIxACIFHHxGRUJ/XicRPh0GIRBnRQwrQyc7JRRNNB0ieScTO0UYJzwRFRAdIH0WGjVDEVYGSkNSRyBvEk80OzkWDCtfLSc4dEYbJn84JD83ACYzREw6XR9EHxofFiEJQgR0BTBIMQQRBzccJjFMZQERRhsGGTo4NgYjMBkiMisDGyVAJCwbGExmRw48fyEgEUUdKREZBh0UOT89ITJcJSsZHhwjEyckBhURHAAuRhtkPBEEExkvPUNFEzslexlDJx4TIB5GIBZKNxwqGSN/HwAxEjwbXQNGB0YXGwIAASYDWBwibh0UJgZfFiEkJCQbW3kwESk7ODAFKhsACiFhADknNwwSEwoZEDNbYwM5SH8xUwobMCUGDnlBAzQwXiIPKwE5MUxDCjNCJCIhDCI3ThUnfCYkRxkoUiIbMxsfNWEpNzJGPDc4FAElJUxqHxIkfytbKAoMEjhBTkIhNkMsJ1spMydBI08aNwMHJw8aNxk5ARdbFBM9Fj8bPT4ZLhMsdTE9JCImFy8/OwoAGm8XAyF/MS8vJxsLAUZ9KjIrPwxVWwoNJB0OfDo3QR0vVwUWESBHFX1cMl5NDjskPUFOCltnB0cLDyg3ET1fKgoGfAY+O38/EA40MCBGBFgEPTMSLTsOJiAmHSNjNBQVHTwCIBQuUEoGRB4aGQ0YKBxHPg8GIUoaFEAcCikkNT4ONUNgBSBHfyMZAipBNyIBHyEnNx8vTD0kIggqN3g7FAgAAjUDCTI0JRcUMB8DNwo7DBhHOBhBRzcHBBI8EQERGQ5ZGHRBPjt/USwsHDBTAw5XET5AHgYSI0YNBQQmbkYhOiAuFjghQycCAWkpFUceOFUIEgEsBTVOGD8lEVFQLgc1DjU2bDoyBX8FEQpHHyUwW3cQEScNOUgGPhJRRzZmSkUdIj4UCRlCVxUsSRJBIk0lIjsWRAYoFWULHEcBRhclJw0RWSFnNj82fwFQM0EeUgoBWwBCAy0wNQU+Jzk7OFRAMhMCXQYsKyIRPFteGRdHRj4XBwNDBCYCXAkVKzA9GgkKJhAmGh8aLxt/DS4OIRtSFDl4ETEUGFtXMgEAJzYXSikkFQMkUBgVQ1A0QV4XGAA7BSIENDYgPQBUKS4jJhM6EwQsUBMHYTQsQn8oUjM2PBNdEmowHEA4HxFaNj4lQDd8CjxJPyA6ChtAUEZHHT0iOAVeCDMXFSAzXxUxMkMSIAg+RzwqKzVURkE2fxEQB0IyDQgzHBA5KDcDOS8aRSZZQ0BDMAkkEwIgMQwkKwx8JRkEFjgkWwkyJkUfdEAsSBMtGyA4RiVKBENDJCd/WzUvIzc2IBN6HTgcOQsJODYhUEVBRwQUe1hETkZeMS82VH0hPyc0PSZLODE4X1kAXlt7",  # noqa
    "client_email": "data-load-tool-public-demo@zinc-mantra-353207.iam.gserviceaccount.com",
}
db_dsn = "postgres://loader:loader@localhost:5432/dlt_data"

destination_name = "duckdb"
if destination_name == "bigquery":
    # we do not want to have this key verbatim in repo so we decode it here
    gcp_credentials_json["private_key"] = bytes(
        [
            _a ^ _b
            for _a, _b in zip(
                base64.b64decode(gcp_credentials_json["private_key"]), b"quickstart-sv" * 150
            )
        ]
    ).decode("utf-8")
    credentials: Any = gcp_credentials_json
elif destination_name == "redshift":
    credentials = db_dsn
else:
    credentials = None  # you do not need credentials for duckdb

# enable the automatic schema export so you can see the auto-generated schema
export_schema_path = "docs/examples/schemas/"

# 2. Create a pipeline
pipeline = dlt.pipeline(
    pipeline_name,
    destination=destination_name,
    dataset_name=dataset_name,
    export_schema_path=export_schema_path,
    dev_mode=True,
)


# 3. Pass the data to the pipeline and give it a table name. Optionally normalize and handle schema.

rows = [
    {
        "name": "Ana",
        "age": 30,
        "id": 456,
        "children": [{"name": "Bill", "id": 625}, {"name": "Elli", "id": 591}],
    },
    {
        "name": "Bob",
        "age": 30,
        "id": 455,
        "children": [{"name": "Bill", "id": 625}, {"name": "Dave", "id": 621}],
    },
]

load_info = pipeline.run(
    rows, table_name=table_name, write_disposition="replace", credentials=credentials
)

# 4. Optional error handling - print, raise or handle.
print()
print(load_info)
print()


# show to auto-generated schema
# print(pipeline.default_schema.to_pretty_yaml())

# now you can use your data

# Tables created:
print("Now lets SELECT some data:")
print()
with pipeline.sql_client() as c:
    query = "SELECT * FROM my_json_doc"
    df = c.execute_sql(query)
    print(query)
    print(list(df))
    print()
    # {  "name": "Ana",  "age": "30",  "id": "456",  "_dlt_load_id": "1654787700.406905",  "_dlt_id": "5b018c1ba3364279a0ca1a231fbd8d90"}
    # {  "name": "Bob",  "age": "30",  "id": "455",  "_dlt_load_id": "1654787700.406905",  "_dlt_id": "afc8506472a14a529bf3e6ebba3e0a9e"}

    query = "SELECT * FROM my_json_doc__children LIMIT 1000"
    df = c.execute_sql(query)
    print(query)
    print(list(df))
    print()
    # {"name": "Bill", "id": "625", "_dlt_parent_id": "5b018c1ba3364279a0ca1a231fbd8d90", "_dlt_list_idx": "0", "_dlt_root_id": "5b018c1ba3364279a0ca1a231fbd8d90",
    #   "_dlt_id": "7993452627a98814cc7091f2c51faf5c"}
    # {"name": "Bill", "id": "625", "_dlt_parent_id": "afc8506472a14a529bf3e6ebba3e0a9e", "_dlt_list_idx": "0", "_dlt_root_id": "afc8506472a14a529bf3e6ebba3e0a9e",
    #   "_dlt_id": "9a2fd144227e70e3aa09467e2358f934"}
    # {"name": "Dave", "id": "621", "_dlt_parent_id": "afc8506472a14a529bf3e6ebba3e0a9e", "_dlt_list_idx": "1", "_dlt_root_id": "afc8506472a14a529bf3e6ebba3e0a9e",
    #   "_dlt_id": "28002ed6792470ea8caf2d6b6393b4f9"}
    # {"name": "Elli", "id": "591", "_dlt_parent_id": "5b018c1ba3364279a0ca1a231fbd8d90", "_dlt_list_idx": "1", "_dlt_root_id": "5b018c1ba3364279a0ca1a231fbd8d90",
    #   "_dlt_id": "d18172353fba1a492c739a7789a786cf"}

    # and we can join them via auto generated keys
    query = """
        select p.name, p.age, p.id as parent_id,
            c.name as child_name, c.id as child_id, c._dlt_list_idx as child_order_in_list
        from my_json_doc as p
        left join my_json_doc__children  as c
            on p._dlt_id = c._dlt_parent_id
    """
    df = c.execute_sql(query)
    print(query)
    print(list(df))
    print()
    # {  "name": "Ana",  "age": "30",  "parent_id": "456",  "child_name": "Bill",  "child_id": "625",  "child_order_in_list": "0"}
    # {  "name": "Ana",  "age": "30",  "parent_id": "456",  "child_name": "Elli",  "child_id": "591",  "child_order_in_list": "1"}
    # {  "name": "Bob",  "age": "30",  "parent_id": "455",  "child_name": "Bill",  "child_id": "625",  "child_order_in_list": "0"}
    # {  "name": "Bob",  "age": "30",  "parent_id": "455",  "child_name": "Dave",  "child_id": "621",  "child_order_in_list": "1"}
