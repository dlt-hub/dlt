---
sidebar_label: configuration
title: destinations.impl.dummy.configuration
---

## DummyClientConfiguration Objects

```python
@configspec
class DummyClientConfiguration(DestinationClientConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/dummy/configuration.py#L19)

### destination\_type

type: ignore

### fail\_prob

probability of terminal fail

### retry\_prob

probability of job retry

### completed\_prob

probability of successful job completion

### exception\_prob

probability of exception transient exception when running job

### timeout

timeout time

### fail\_terminally\_in\_init

raise terminal exception in job init

### fail\_transiently\_in\_init

raise transient exception in job init

### truncate\_tables\_on\_staging\_destination\_before\_load

truncate tables on staging destination

### create\_followup\_jobs

create followup job for individual jobs

### fail\_followup\_job\_creation

Raise generic exception during followupjob creation

### fail\_table\_chain\_followup\_job\_creation

Raise generic exception during tablechain followupjob creation

### create\_followup\_table\_chain\_sql\_jobs

create a table chain merge job which is guaranteed to fail

### create\_followup\_table\_chain\_reference\_jobs

create table chain jobs which succeed

