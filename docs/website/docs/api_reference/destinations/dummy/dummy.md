---
sidebar_label: dummy
title: destinations.dummy.dummy
---

## DummyClient Objects

```python
class DummyClient(JobClientBase)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/dummy/dummy.py#L72)

dummy client storing jobs in memory

#### create\_table\_chain\_completed\_followup\_jobs

```python
def create_table_chain_completed_followup_jobs(
        table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/dummy/dummy.py#L115)

Creates a list of followup jobs that should be executed after a table chain is completed

