---
title: Renaming columns
description: Renaming columns by replacing the special characters
keywords: [renaming, columns, special characters]
---

# Renaming columns

## Renaming columns by replacing the special characters

In the example below, we create a dummy source with special characters in the name. We then write a function that we intend to apply to the resource to modify its output (i.e., replacing the German umlaut): `replace_umlauts_in_dict_keys`.

```py
import dlt

# create a dummy source with umlauts (special characters) in key names (um)
@dlt.source
def dummy_source(prefix: str = None):
    @dlt.resource
    def dummy_data():
        for _ in range(100):
            yield {f'Objekt_{_}': {'Größe': _, 'Äquivalenzprüfung': True}}
    return dummy_data(),

def replace_umlauts_in_dict_keys(d):
    """
    Replaces umlauts in dictionary keys with standard characters.
    """
    umlaut_map =  {'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss', 'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue'}
    result = {}
    for k, v in d.items():
        new_key = ''.join(umlaut_map.get(c, c) for c in k)
        if isinstance(v, dict):
            result[new_key] = replace_umlauts_in_dict_keys(v)
        else:
            result[new_key] = v
    return result

# We can add the map function to the resource

# 1. Create an instance of the source so you can edit it.
data_source = dummy_source()

# 2. Modify this source instance's resource
data_resource = data_source.dummy_data().add_map(replace_umlauts_in_dict_keys)

# 3. Inspect your result
for row in data_resource:
    print(row)

# {'Objekt_0': {'Groesse': 0, 'Aequivalenzpruefung': True}}
# ...
```

