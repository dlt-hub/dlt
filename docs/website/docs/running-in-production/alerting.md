---
title: Alerting
description: How to set up alerting for dlt pipelines
keywords: [alerting, alerts, slack]
---

# Alerting

## Alerts

[Monitoring](monitoring.md) and alerting are used together to give a complete picture of the health
of our data product.

An alert is triggered by a specific action:

- What cases do you want to alert?
- Where should the alert be sent?
- How can you create a useful message?

For example, an actionable alert contains information to help take a follow-up action: what, when, and why
the pipeline broke (with a link to the error log):

![Airflow Slack notification](images/airflow_slack_notification.png)

While we may create all kinds of tests and associated alerts, the first ones are usually alerts
about the running status of your pipeline. Unfortunately, the outcome of a pipeline is not binary:
it could succeed, it could fail, it could be late due to extra data, it could be stuck due to a bug,
it could be not started due to a failed dependency, etc. Due to the complexity of the cases, usually,
you alert failures and [monitor](monitoring.md) (lack of) success.

We could also use alerts as a way to deliver tests. For example, a customer support representative
must associate each customer call with a customer. We could test that all tickets have a customer in
our production database. If not, we could alert customer support to collect the necessary
information.

## Sentry

Using `dlt` [tracing](./tracing.md), you can configure [Sentry](https://sentry.io) DSN to start
receiving rich information on executed pipelines, including encountered errors and exceptions.

## Slack

Alerts can be sent to a Slack channel via Slack's incoming webhook URL. The code snippet below demonstrates automated Slack notifications for database table updates using the `send_slack_message` function.

```py
# Import the send_slack_message function from the dlt library
from dlt.common.runtime.slack import send_slack_message

# Define the URL for your Slack webhook
hook = "https://hooks.slack.com/services/xxx/xxx/xxx"

# Iterate over each package in the load_info object
for package in info.load_packages:
    # Iterate over each table in the schema_update of the current package
    for table_name, table in package.schema_update.items():
        # Iterate over each column in the current table
        for column_name, column in table["columns"].items():
            # Send a message to the Slack channel with the table
            # and column update information
            send_slack_message(
                hook,
                message=(
                    f"\tTable updated: {table_name}: "
                    f"Column changed: {column_name}: "
                    f"{column['data_type']}"
                )
            )
```
Refer to this [example](../examples/chess_production/) for a practical application of the method in a production environment.

Similarly, Slack notifications can be extended to include information on pipeline execution times, loading durations, schema modifications, and more. For comprehensive details on configuring and sending messages to Slack, please read [here](./running#using-slack-to-send-messages).

