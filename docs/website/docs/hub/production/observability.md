---
title: Observability
description: Observability tooling
keywords: [observability, monitoring, alerting, callbacks]
---

# Observability

## Custom callbacks

dltHub includes a `PlusLogCollector` interface that can be used to implement custom callbacks.
It is an extension of the dlt [LogCollector](https://github.com/dlt-hub/dlt/blob/273420b2574a518a7488443253ab1e0971b136e8/dlt/common/runtime/collector.py#L77) which accumulates pipeline and system stats and outputs to a python logger or the console.

You have several options to implement your own logic:
- Five different methods related to the different stages of the pipeline, all having the pipeline and the trace as parameters, allowing you, for example, to implement
`after_extract` or `before_load`.
- Three additional callbacks, `on_before`, `on_after` and `on_retry`.
- the `on_log` method, which is called at a customizable interval and has context about the progress of the currently active stage and the system resources.

For an extended description, we recommend you look at the actual implementation of the `PlusLogCollector` interface.
Here is a simple example of how to inherit from the `PlusLogCollector` and use the [slack-hook](../../running-in-production/running#using-slack-to-send-messages) to send a message when a schema change is detected.

```py
from dlthub._runner.plus_log_collector import PlusLogCollector
from dlt.sources.sql_database import sql_database
from dlt.common.schema.typing import TTableSchema
from dlt.common.pipeline import SupportsPipeline
from dlt.pipeline.trace import PipelineTrace
from typing import Dict
import dlt
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")

my_source = sql_database(schema="my_schema")


class MyLogCollector(PlusLogCollector):
    def on_schema_change(
        self,
        pipeline: SupportsPipeline,
        trace: PipelineTrace,
        schema_name: str,
        changes: Dict[str, TTableSchema],
    ) -> None:
        try:
            from dlt.common.runtime.slack import send_slack_message
            # os.environ["RUNTIME__SLACK_INCOMING_HOOK"] = "https://hooks.slack.com/services/..."

            msg = f"schema {schema_name} change in pipeline {pipeline.pipeline_name}**:\n{changes}"
            send_slack_message(pipeline.runtime_config.slack_incoming_hook, msg)  # type: ignore
        except Exception as e:
            # fail without interrupting the pipeline
            print(f"Error trying to send slack message: {e}")


    def on_log(self):
        # override the default behavior if you don't want to see progress log output on stdio
        # super().on_log()
        pass

# attach your collector to the pipeline
pipeline.collector = MyLogCollector()

load_info = pipeline.run(my_source)
print(load_info)
```

## Planned features

There are several other features under development in dltHub to enhance your observability workflows. These include:
* A UI to explore and debug your pipeline runs
* An AI agent to investigate your traces and logs

Interested? Join our [early access program](https://info.dlthub.com/waiting-list).
