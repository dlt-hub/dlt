---
sidebar_position: 4
---

# State

> **💡** The “state” of a pipeline refers to metadata that we store about the data.
>
> We can use the state as a persistent python dictionary peristed between pipeline runs in which we can store any information
>
>For incremental pipelines, the state stores information about what was loaded, to know where to resume.

**What is stored in the state?**

- dlt engine configuration and inferred schema (defaults, if you do not modify them)
- variables that you choose to store

For example, the “last id” or “last timestamp” of a loaded data set, so we can make an incremental request. In the case of field renames, we could store the field mapping there.

**Where is state created and maintained?**

The state is stored in the `_dlt_pipeline_state` table at the destination and contains information about the pipeline, pipeline run (that the state belongs to) and state blob.
You can create the state in your python script as such

Example: Track ONE state for a single resource
```python

# Get archives from state. If state does not exist, get the default, in this case empty list []
loaded_archives = dlt.current.state().setdefault("archives", [])
new_archives = get_archives(url)
archives_to_be_loaded = [a for a in new_archives if a not in loaded_archives]
for archive in archives_to_be_loaded:
  yield archives

```
Example: Track multiple states for multiple search terms
```python
# get dlt state to store last values of tweets for each search term we request
last_value_cache = dlt.current.state().setdefault("last_value_cache", {})
# use `last_value` to initialize state on the first run of the pipeline
last_value = last_value_cache.setdefault(search_term, last_value or None)
# if we have a last value, then use it for request params. If not, then we make an unparametrised request and get all data.
if last_value:
  request_params['since_id'] = last_value
# ... get data and set the new state
# the state will be comitted atomically with the data - you do not need to yield it or do anything more
last_value_cache[search_term] = max(last_value_cache[search_term], int(last_id))

```

**What about local state?**

There is also a local state that happens during a run. This state is a temporary cache that supports passing data between resources and buffered loading. You will not interact with this state directly, and it should be cleaned up after every run.

During development, cancelling the running script may cause the state cleanup to fail, and a state conflict to occur on the next run. To resolve this, use `full_refresh=True` in your pipeline to create a new state.

It is important to be aware of this local state, as resources that share state need to be run on the same worker machine.

**How can we share state between loading pipelines?**

The state is identified by pipeline name and destination dataset. To re-use the same state, use the same pipeline name and destination.

To re-use the same local state, for example in airflow, where separate tasks may end up running on separate machines, you can force 2 resources to run on the same machine by putting them in a single task.