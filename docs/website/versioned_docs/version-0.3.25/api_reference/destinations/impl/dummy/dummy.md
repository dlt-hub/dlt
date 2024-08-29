---
sidebar_label: dummy
title: destinations.impl.dummy.dummy
---

## DummyClient Objects

```python
class DummyClient(JobClientBase, SupportsStagingDestination,
                  WithStagingDataset)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/dummy/dummy.py#L110)

dummy client storing jobs in memory

### create\_table\_chain\_completed\_followup\_jobs

```python
def create_table_chain_completed_followup_jobs(
        table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/dummy/dummy.py#L158)

Creates a list of followup jobs that should be executed after a table chain is completed

