---
sidebar_label: dummy
title: destinations.dummy.dummy
---

## DummyClient Objects

```python
class DummyClient(JobClientBase)
```

dummy client storing jobs in memory

#### create\_table\_chain\_completed\_followup\_jobs

```python
def create_table_chain_completed_followup_jobs(
        table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]
```

Creates a list of followup jobs that should be executed after a table chain is completed

