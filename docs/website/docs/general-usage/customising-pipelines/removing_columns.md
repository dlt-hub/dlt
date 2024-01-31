---
title: Removing columns
description: Removing columns by passing list of column names
keywords: [deleting, removing, columns, drop]
---

# Removing columns

Removing columns before loading data into a database is a reliable method to eliminate sensitive or
unnecessary fields. For example, in the given scenario, a source is created with a "country_id" column,
which is then excluded from the database before loading.

```python
import dlt

@dlt.source
def dummy_source(prefix: str = None):
    """
    This function creates a dummy data source.
    It generates a sample dataset with columns 'id', 'name', and 'country_id'.
    """

    @dlt.resource(write_disposition='replace')
    def dummy_data():
        """
        This nested function yields a small set of dummy data.
        Each row consists of an 'id', a 'name', and a 'country_id'.
        """
        for _ in range(3):
            yield {'id': _, 'name': f'Jane Washington {_}', 'country_id': 90 + _}
    return dummy_data(),

def remove_columns(doc, remove_columns=None):
    """
    Removes specified columns from a document (row of data).

    This function is used to filter out columns from the data before loading it into a database,
    which can be useful for excluding sensitive or unnecessary information.

    :param doc: The document (row) from which columns will be removed.
    :param remove_columns: List of column names to be removed, defaults to None.
    :return: The document with specified columns removed.
    """

    if remove_columns is None:
        remove_columns = []

    # Iterating over the list of columns to be removed
    for column_name in remove_columns:
        # Removing the column if it exists in the document
        if column_name in doc:
            del doc[column_name]

    return doc

# Example usage:
remove_columns_list = ["country_id"]

# run it as it is
for row in dummy_source().dummy_data.add_map(lambda doc: remove_columns(doc, remove_columns_list)):
    print(row)
#{'id': 0, 'name': 'Jane Washington 0'}
#{'id': 1, 'name': 'Jane Washington 1'}
#{'id': 2, 'name': 'Jane Washington 2'}


# Or create an instance of the data source, modify the resource and run the source.

# 1. Create an instance of the source so you can edit it.
data_source = dummy_source()
# 2. Modify this source instance's resource
data_source = data_source.dummy_data.add_map(lambda doc: remove_columns(doc, remove_columns_list))
# 3. Inspect your result
for row in data_source:
    print(row)

# Integrating with a DLT pipeline
pipeline = dlt.pipeline(pipeline_name='example', destination='bigquery', dataset_name='filtered_data')
load_info = pipeline.run(data_source)
```