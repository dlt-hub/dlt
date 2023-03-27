---
sidebar_position: 1
---

### Rename columns by replacing the special characters (german umlaut)

In the example below, we createa  dummy source with special characters in the name.
We then write a function
```python
import dlt
# create a dummy source with umlauts (special characters) in key names (um)
@dlt.source
def dummy_source(prefix: str = None):
    @dlt.resource
    def dummy_data():
        for _ in range(100):
            yield {f'Objekt_{_}':{'Größe':_, 'Äquivalenzprüfung':True}}
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


# we can add the map function to the resource
for row in dummy_source().dummy_data().add_map(replace_umlauts_in_dict_keys):
    print(row)

# the document is renamed
#{'object0': {'Groesse': 1, 'Aequivalenzpruefung': True}}

```

