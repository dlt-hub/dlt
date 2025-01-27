---
sidebar_label: job_impl
title: destinations.job_impl
---

## FinalizedLoadJob Objects

```python
class FinalizedLoadJob(LoadJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/job_impl.py#L25)

Special Load Job that should never get started and just indicates a job being in a final state.
May also be used to indicate that nothing needs to be done.

## FollowupJobRequestImpl Objects

```python
class FollowupJobRequestImpl(FollowupJobRequest)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/job_impl.py#L57)

Class to create a new loadjob, not stateful and not runnable

### new\_file\_path

```python
def new_file_path() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/job_impl.py#L71)

Path to a newly created temporary job file

### job\_id

```python
def job_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/job_impl.py#L75)

The job id that is derived from the file name and does not changes during job lifecycle

