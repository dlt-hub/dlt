Follow this quick guide to implement DLT in your project

## Simple loading of one row:

### Install DLT
DLT is available in PyPi and can be installed with `pip install python-dlt`. Support for target warehouses is provided in extra packages:

`pip install python-dlt[redshift]` for Redshift

`pip install python-dlt[gcp]` for BigQuery

### Create a target credential
```
credential = {'type':'redshift',
                'host': '123.456.789.101'
                'port': '5439'
                'user': 'loader'
                'password': 'dolphins'

                }


```

### Initialise the loader with your credentials and load one json row
```
import dlt

loader = dlt(credential)

json_row = "{"name":"Gabe", "age":30}"

table_name = 'users'

loader.load(table_name, json_row)

```

## Loading a nested json object

```
import dlt

loader = dlt(credential)

json_row = "{"name":"Gabe", "age":30, "id":456, "children":[{"name": "Bill", "id": 625},
                                                            {"name": "Cill", "id": 666},
                                                            {"name": "Dill", "id": 777}
                                                            ]
            }"


table_name = 'users'


#unpack the nested json. To be able to re-pack it, we create the parent - child join keys via row / parent row hashes.

rows = loader.utils.unpack(table_name, json_row)

# rows are a generator that outputs the parent or child table name and the data row such as:

#("users", "{"name":"Gabe", "age":30, "id":456, "row_hash":"parent_row_md5"}")
#("users__children", "{"name":"Bill", "id":625, "parent_row_hash":"parent_row_md5", "row_hash":"child1_row_md5"}")
#("users__children", "{"name":"Cill", "id":666, "parent_row_hash":"parent_row_md5", "row_hash":"child2_row_md5"}")
#("users__children", "{"name":"Dill", "id":777, "parent_row_hash":"parent_row_md5", "row_hash":"child3_row_md5"}")


#loading the tables users, and users__children
for table, row in rows:
    loader.load(table_name, row)


#to recreate the original structure
select users.*, users__children.*
from users
left join users__children
    on users.row_hash = users__children.parent_row_hash
```
