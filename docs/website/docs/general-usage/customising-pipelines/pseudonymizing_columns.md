---
title: Pseudonymizing columns
description: Pseudonymizing (or anonymizing) columns by replacing the special characters
keywords: [pseudonymize, anonymize, columns, special characters]
---

# Pseudonymizing columns

Pseudonymization is a deterministic way to hide personally identifiable information (PII), enabling us to consistently achieve the same mapping. If instead you wish to anonymize, you can delete the data or replace it with a constant. In the example below, we create a dummy source with a PII column called "name", which we replace with deterministic hashes (i.e., replacing the German umlaut).

```py
import dlt
import hashlib

@dlt.source
def dummy_source(prefix: str = None):
    @dlt.resource
    def dummy_data():
        for _ in range(3):
            yield {'id': _, 'name': f'Jane Washington {_}'}
    return dummy_data(),

def pseudonymize_name(doc):
    '''
    Pseudonymization is a deterministic type of PII-obscuring.
    Its role is to allow identifying users by their hash,
    without revealing the underlying info.
    '''
    # add a constant salt to generate
    salt = 'WI@N57%zZrmk#88c'
    salted_string = doc['name'] + salt
    sh = hashlib.sha256()
    sh.update(salted_string.encode())
    hashed_string = sh.digest().hex()
    doc['name'] = hashed_string
    return doc

# run it as is
for row in dummy_source().dummy_data.add_map(pseudonymize_name):
    print(row)

#{'id': 0, 'name': '96259edb2b28b48bebce8278c550e99fbdc4a3fac8189e6b90f183ecff01c442'}
#{'id': 1, 'name': '92d3972b625cbd21f28782fb5c89552ce1aa09281892a2ab32aee8feeb3544a1'}
#{'id': 2, 'name': '443679926a7cff506a3b5d5d094dc7734861352b9e0791af5d39db5a7356d11a'}

# Or create an instance of the data source, modify the resource and run the source.

# 1. Create an instance of the source so you can edit it.
data_source = dummy_source()
# 2. Modify this source instance's resource
data_resource = data_source.dummy_data.add_map(pseudonymize_name)
# 3. Inspect your result
for row in data_resource:
    print(row)

pipeline = dlt.pipeline(pipeline_name='example', destination='bigquery', dataset_name='normalized_data')
load_info = pipeline.run(data_resource)
```

