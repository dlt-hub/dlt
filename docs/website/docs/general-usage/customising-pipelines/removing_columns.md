---
title: Removing columns
description: Removing columns by passing a list of column names
keywords: [deleting, removing, columns, drop]
---

# Removing columns

Removing columns before loading data into a database is a reliable method to eliminate sensitive or unnecessary fields. For example, in the given scenario, a source is created with a "country_id" column, which is then excluded from the database before loading.

Let's create a sample pipeline demonstrating the process of removing a column.

1. Create a source function that creates dummy data as follows:

   ```py
   import dlt

   # This function creates a dummy data source.
   @dlt.source
   def dummy_source():
       @dlt.resource(write_disposition="replace")
       def dummy_data():
           for i in range(3):
               yield {"id": i, "name": f"Jane Washington {i}", "country_code": 40 + i}

       return dummy_data()
   ```
   This function creates three columns: `id`, `name`, and `country_code`.

2. Next, create a function to filter out columns from the data before loading it into a database as follows:

   ```py
   from typing import Dict, List, Optional

   def remove_columns(doc: Dict, remove_columns: Optional[List[str]] = None) -> Dict:
       if remove_columns is None:
           remove_columns = []

       # Iterating over the list of columns to be removed
       for column_name in remove_columns:
           # Removing the column if it exists in the document
           if column_name in doc:
               del doc[column_name]

       return doc
   ```

   `doc`: The document (dict) from which columns will be removed.

   `remove_columns`: List of column names to be removed, defaults to None.

3. Next, declare the columns to be removed from the table, and then modify the source as follows:

   ```py
   # Example columns to remove:
   remove_columns_list = ["country_code"]

   # Create an instance of the source so you can edit it.
   data_source = dummy_source()

   # Modify this source instance's resource
   data_source = data_source.dummy_data.add_map(
       lambda doc: remove_columns(doc, remove_columns_list)
   )
   ```
4. You can optionally inspect the result:

   ```py
   for row in data_source:
       print(row)
   #{'id': 0, 'name': 'Jane Washington 0'}
   #{'id': 1, 'name': 'Jane Washington 1'}
   #{'id': 2, 'name': 'Jane Washington 2'}
   ```

5. At last, create a pipeline:

   ```py
   # Integrating with a dlt pipeline
   pipeline = dlt.pipeline(
       pipeline_name='example',
       destination='bigquery',
       dataset_name='filtered_data'
   )
   # Run the pipeline with the transformed source
   load_info = pipeline.run(data_source)
   print(load_info)
   ```

