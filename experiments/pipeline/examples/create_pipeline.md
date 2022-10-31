
## Example for the simplest ad hoc pipeline without any structure
It is still possible to create "intuitive" pipeline without much knowledge except how to import dlt engine and how to import the destination.

No decorators and secret files, configurations are necessary. We should probably not teach that but I want this kind of super basic and brute code to still work


```python
import requests
import dlt
from dlt.destinations import bigquery

resp = requests.get(
    "https://taktile.com/api/v2/logs?from_log_id=1",
    headers={"Authorization": "98217398ahskj92982173"})
resp.raise_for_status()
data = resp.json()

# if destination or name are not provided, an exception will raise that explains
# 1. where and why to put the name of the table
# 2. how to import the destination and how to configure it with credentials in a proper way
# nevertheless the user decided to pass credentials directly
dlt.run(data["result"], name="logs", destination=bigquery(Service.credentials_from_file("service.json")))
```

## Source extractor function the preferred way
General guidelines:
1. the source extractor is a function decorated with `@dlt.source`. that function yields or returns a list of resources. **it should not access the data itself**. see the example below
2. resources are generator functions that always **yield** data (I think I will enforce that by raising exception). Access to external endpoints, databases etc. should happen from that generator function. Generator functions may be decorated with `@dlt.resource` to provide alternative names, write disposition etc.
3. resource generator functions can be OFC parametrized and resources may be created dynamically
4. the resource generator function may yield a single dict or list of dicts
5. like any other iterator, the @dlt.source and @dlt.resource **can be iterated and thus extracted and loaded only once**, see example below.

> my dilemma here is if I should allow to access data directly in the source function ie. to discover schema or get some configuration for the resources from some endpoint. it is very easy to avoid that but for the non-programmers it will not be intuitive.

## Example for endpoint returning only one resource:

```python
import requests
import dlt

# the `dlt.source` tell the library that the decorated function is a source
# it will use function name `taktile_data` to name the source and the generated schema by default
# in general `@source` should **return** a list of resources or list of generators (function that yield data)
# @source may also **yield** resources or generators - if yielding is more convenient
# if @source returns or yields data - this will generate exception with a proper explanation. dlt user can always load the data directly without any decorators like in the previous example!
@dlt.source
def taktile_data(initial_log_id, taktile_api_key):

    # the `dlt.resource` tells the `dlt.source` that the function defines a resource
    # will use function name `logs` as resource/table name by default
    # the function should **yield** the data items one by one or **yield** a list.
    # here the decorator is optional: there are no parameters to `dlt.resource`
    @dlt.resource
    def logs():
        resp = requests.get(
            "https://taktile.com/api/v2/logs?from_log_id=%i" % initial_log_id,
            headers={"Authorization": taktile_api_key})
        resp.raise_for_status()
        # option 1: yield the whole list
        yield resp.json()["result"]
        # or -> this is useful if you deal with a stream of data and for that you need an API that supports that, for example you could yield lists containing paginated results
        for item in resp.json()["result"]:
            yield item

    # as mentioned we return a resource or a list of resources
    return logs
    # this will also work
    return logs()


# now load the data
taktile_data(1).run(destination=bigquery)
# this below also works
# dlt.run(source=taktile_data(1), destination=bigquery)

# now to illustrate that each source can be loaded only once, if you run this below
data = taktile_data(1)
data.run(destination=bigquery)  # works as expected
data.run(destination=bigquery) # generates empty load package as the data in the iterator is exhausted... maybe I should raise exception instead?

```

**Remarks:**

1. the **@dlt.resource** let's you define the table schema hints: `name`, `write_disposition`, `parent`, `columns`
2. the **@dlt.source** let's you define global schema props: `name` (which is also source name), `schema` which is Schema object if explicit schema is provided `nesting` to set nesting level etc. (I do not have a signature now - I'm still working on it)
3. decorators can also be used as functions ie in case of dlt.resource and `lazy_function` (see one page below)
```python
endpoints = ["songs", "playlist", "albums"]
# return list of resourced
return [dlt.resource(lazy_function(endpoint, name=endpoint) for endpoint in endpoints)]

```

**What if we remove logs() function and get data in source body**

Yeah definitely possible. Just replace `@source` with `@resource` decorator and remove the function

```python
@dlt.resource(name="logs", write_disposition="append")
def taktile_data(initial_log_id, taktile_api_key):

    # yes, this will also work but data will be obtained immediately when taktile_data() is called.
    resp = requests.get(
        "https://taktile.com/api/v2/logs?from_log_id=%i" % initial_log_id,
        headers={"Authorization": taktile_api_key})
    resp.raise_for_status()
    for item in resp.json()["result"]:
        yield item

# this will load the resource into default schema. see `general_usage.md)
dlt.run(source=taktile_data(1), destination=bigquery)

```

**The power of decorators**

With decorators dlt can inspect and modify the code being decorated.
1. it knows what are the sources and resources without running them
2. it knows input arguments so it knows the config values and secret values (see `secrets_and_config`). with those we can generate deployments automatically
3. it can inject config and secret values automatically
4. it wraps the functions into objects that provide additional functionalities
- sources and resources are iterators so you can write
```python
items = list(source())

for item in source()["logs"]:
    ...
```
- you can select which resources to load with `source().select(*names)`
- you can add mappings and filters to resources

## The power of yielding: The preferred way to write resources

The Python function that yields is not a function but magical object that `dlt` can control:
1. it is not executed when you call it! the call just creates a generator (see below). in the example above `taktile_data(1)` will not execute the code inside, it will just return an object composed of function code and input parameters. dlt has control over the object and can execute the code later. this is called `lazy execution`
2. i can control when and how much of the code is executed. the function that yields typically looks like that

```python
def lazy_function(endpoint_name):
    # INIT - this will be executed only once when DLT wants!
    get_configuration()
    from_item = dlt.state.get("last_item", 0)
    l = get_item_list_from_api(api_key, endpoint_name)

    # ITERATOR - this will be executed many times also when DLT wants more data!
    for item in l:
        yield requests.get(url, api_key, "%s?id=%s" % (endpoint_name, item["id"])).json()
    # CLEANUP
    # this will be executed only once after the last item was yielded!
    dlt.state["last_item"] = item["id"]
```

3. dlt will execute this generator in extractor. the whole execution is atomic (including writing to state). if anything fails with exception the whole extract function fails.
4. the execution can be parallelized by using a decorator or a simple modifier function ie:
```python
for item in l:
    yield deferred(requests.get(url, api_key, "%s?id=%s" % (endpoint_name, item["id"])).json())
```

## Python data transformations

```python
from dlt.secrets import anonymize

def transform_user(user_data):
    # anonymize creates nice deterministic hash for any hashable data type
    user_data["user_id"] = anonymize(user_data["user_id"])
    user_data["user_email"] = anonymize(user_data["user_email"])
    return user_data

# usage: can be applied in the source
@dlt.source
def hubspot(...):
    ...

    @dlt.resource(write_disposition="replace")
    def users():
        ...
        users = requests.get(...)
        # option 1: just map and yield from mapping
        users = map(transform_user, users)
        ...
        yield users, deals, customers

    # option 2: return resource with chained transformation
    return users.map(transform_user)

# option 3: user of the pipeline determines if s/he wants the anonymized data or not and does it in pipeline script. so the source may offer also transformations that are easily used
hubspot(...).resources["users"].map(transform_user)
hubspot.run(...)

```

## Multiple resources and resource selection
The source extraction function may contain multiple resources. The resources can be defined as multiple resource functions or created dynamically ie. with parametrized generators.
The user of the pipeline can check what resources are available and select the resources to load.


**each resource has a a separate resource function**
```python
import requests
import dlt

@dlt.source
def hubspot(...):

    @dlt.resource(write_disposition="replace")
    def users():
        # calls to API happens here
        ...
        yield users

    @dlt.resource(write_disposition="append")
    def transactions():
        ...
        yield transactions

    # return a list of resources
    return users, transactions

# load all resources
taktile_data(1).run(destination=bigquery)
# load only decisions
taktile_data(1).with_resources("decisions").run(....)

# alternative form:
source = taktile_data(1)
# select only decisions to be loaded
source.resources.select("decisions")
# see what is selected
print(source.selected_resources)
# same as this
print(source.resources.selected)


```

**resources are created dynamically**
Here we implement a single parametrized function that **yields** data and we call it repeatedly. Mind that the function body won't be executed immediately, only later when generator is consumed in extract stage.

```python

@dlt.source
def spotify():

    endpoints = ["songs", "playlists", "albums"]

    def get_resource(endpoint):
        # here we yield the whole response
        yield requests.get(url + "/" + endpoint).json()

    # here we yield resources because this produces cleaner code
    for endpoint in endpoints:
        # calling get_resource creates generator, the actual code of the function will be executed in extractor
        yield dlt.resource(get_resource(endpoint), name=endpoint)

```

**resources are created dynamically from a single document**
Here we have a list of huge documents and we want to split it into several tables. We do not want to rely on `dlt` normalize stage to do it for us for some reason...

This also provides an example of why getting data in the source function (and not within the resource function) is discouraged.

```python

@dlt.source
def spotify():

    # get the data in source body and the simply return the resources
    # this is discouraged because data access
    list_of_huge_docs = requests.get(...)

    return dlt.resource(list_of_huge_docs["songs"], name="songs"), dlt.resource(list_of_huge_docs["playlists"], name="songs")

# the call to get the resource list happens outside the `dlt` pipeline, this means that if there's
# exception in `list_of_huge_docs = requests.get(...)` I cannot handle or log it (or send slack message)
# user must do it himself or the script will be simply killed. not so much problem during development
# but may be a problem after deployment.
spotify().run(...)
```

How to prevent that:
```python
@dlt.source
def spotify():

    list_of_huge_docs = None

    def get_data(name):
        # regarding intuitiveness and cleanliness of the code this is a hack/trickery IMO
        # this will also have consequences if execution is parallelized
        nonlocal list_of_huge_docs
        docs = list_of_huge_docs or list_of_huge_docs = requests.get(...)
        yield docs[name]

    return dlt.resource(get_data("songs"), name="songs"), dlt.resource(get_data("playlists"), name="songs")
```

The other way to prevent that (see also `multistep_pipelines_and_dependent_resources.md`)

```python
@dlt.source
def spotify():

    @dlt.resource
    def get_huge_doc(name):
        yield requests.get(...)

    # make songs and playlists to be dependent on get_huge_doc
    @dlt.resource(depends_on=huge_doc)
    def songs(huge_doc):
        yield huge_doc["songs"]

    @dlt.resource(depends_on=huge_doc)
    def playlists(huge_doc):
        yield huge_doc["playlists"]

    # as you can see the get_huge_doc is not even returned, nevertheless it will be evaluated (only once)
    # the huge doc will not be extracted and loaded
    return songs, playlists
```

> I could also implement lazy evaluation of the @dlt.source function. this is a lot of trickery in the code but definitely possible. there are consequences though: if someone requests lists of resources or the initial schema in the pipeline script before `run` method the function body will be evaluated. It is really hard to make intuitive code to work properly.

## Pipeline with multiple sources or with same source called twice

Here our source is parametrized or we have several sources to be extracted. This is more or less Ty's twitter case.

```python
@dlt.source
def mongo(from_id, to_id, credentials):
    ...

@dlt.source
def labels():
    ...


# option 1: at some point I may parallelize execution of sources if called this way
dlt.run(source=[mongo(0, 100000), mongo(100001, 200000), labels()], destination=bigquery)

# option 2: be explicit (this has consequences: read the `run` method in `general_usage`)
p = dlt.pipeline(destination=bigquery)
p.extract(mongo(0, 100000))
p.extract(mongo(100001, 200000))
p.extract(labels())
p.normalize()
p.load()
