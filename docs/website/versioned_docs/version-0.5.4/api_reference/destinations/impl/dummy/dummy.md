---
sidebar_label: dummy
title: destinations.impl.dummy.dummy
---

## DummyClient Objects

```python
class DummyClient(JobClientBase, SupportsStagingDestination,
                  WithStagingDataset)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/dummy/dummy.py#L128)

dummy client storing jobs in memory

### create\_table\_chain\_completed\_followup\_jobs

```python
def create_table_chain_completed_followup_jobs(
    table_chain: Sequence[TTableSchema],
    completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None
) -> List[FollowupJobRequest]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/dummy/dummy.py#L179)

Creates a list of followup jobs that should be executed after a table chain is completed

