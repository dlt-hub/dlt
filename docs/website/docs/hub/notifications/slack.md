---
title: Send Slack notifications
description: Connect Slack incoming webhooks to receive dltHub workspace run failure and success alerts, or send custom messages from your pipeline code.
keywords: [slack, incoming webhooks, alerts, notifications, failure alerts, run alerts, hub, dltHub]
---
# Send Slack notifications

dltHub supports native Slack notifications for pipeline status changes, configured directly in the Web UI using Slack incoming webhooks. You can also send custom Slack messages programmatically from within your pipeline code.

---

## dltHub platform alerts

:::tip Recommended
Configuring Slack alerts in the dltHub Web UI is the preferred way to monitor pipeline runs. It requires no code changes or secrets in your repository, catches container and runtime failures that happen before user code executes, formats messages with interactive Slack Block Kit buttons, and allows routing alerts to different channels across your team.
:::

With platform alerts, dltHub dispatches rich notifications to your Slack channels whenever a pipeline run fails or succeeds.

![dltHub workspace Slack alert configuration](https://storage.googleapis.com/dlt-blog-images/dlthub-screenshot-slack-alert-config.png)

### Step 1: Create an incoming webhook in Slack

1. Open the [Slack Incoming Webhooks guide](https://api.slack.com/messaging/webhooks).
2. Create a Slack app (or select an existing one in your workspace).
3. Enable **Incoming Webhooks** and click **Add New Webhook to Workspace**.
4. Select the destination Slack channel (e.g. `#data-alerts` or `#pipeline-monitoring`) and authorize the webhook.
5. Copy the generated Webhook URL (`https://hooks.slack.com/services/T.../B.../...`).

### Step 2: Connect the Slack channel in dltHub

1. Open the dltHub Web UI and navigate to your workspace.
2. In the left navigation, go to **Settings > Alerts** (`/w/<workspace_id>/settings/alerts`).
3. In the **Slack Channel Setup** card, click **+ Add channel**.
4. Enter a friendly **Channel** name (e.g. `#data-alerts`).
5. Paste the **Webhook URL** copied from Slack.
6. Click **Save** in the bottom changes bar to store the channel.

### Step 3: Route alerts to your Slack channels and test

1. In the **Alerts Configuration** section on the same page, locate the trigger:

  - **Job run failures**: Fires whenever a pipeline or job run fails.
  - **Job run successes**: Fires whenever a pipeline or job run finishes successfully.

2. Toggle the alert switch **On**.

3. Click the **Slack Channels** dropdown to open the **Post to** popover:

  - Check the channel(s) that should receive this alert. You can select multiple channels.
  - Click the **Test** button next to any channel to dispatch a test notification immediately and verify delivery for that alert type.

4. Under **Scope**, choose which jobs to monitor:

  - **All jobs**: Delivers alerts for any job run in the workspace.
  - **Specific pipelines**: Filters alerts to a selected list of pipelines.

5. (Optional) Under **Email Recipients**, select **( ) No email recipients** if you want alerts routed exclusively to Slack.

6. Click **Save** in the bottom changes bar.

### What the Slack notification includes

dltHub formats alerts using Slack Block Kit:

- **Status header**: Indicates the pipeline name, environment, and outcome (e.g. `dltHub • Job Run Failed: my_pipeline (prod)`).
- **Color-coded attachment**: Red for failures, green for completions.
- **Run metadata**: Workspace name, exact reported timestamp, and trigger source.
- **Failure reason**: Truncated error message and stack excerpt (for failure alerts).
- **Interactive action buttons**:
  - **View Run Details**: Deep links directly to the run page and execution logs in dltHub.
  - **View Pipeline**: Links to the pipeline overview page.

---

## Custom Slack notifications from code (in-code alternative)

:::note When to use custom in-code notifications
dltHub platform alerts above manage channel webhooks and pipeline run notifications across your workspace with zero code changes.

Use the in-code pattern below only if you need custom notification logic from within Python (for instance, notifying Slack on dlt schema changes or custom load metrics).
:::

dlt ships a helper, `send_slack_message`, that posts to a Slack [incoming webhook](https://api.slack.com/messaging/webhooks). Combined with `pipeline.runtime_config.slack_incoming_hook`, it gives you a way to alert a channel directly from your Python script.

### Store the webhook in your prod profile

Add to `.dlt/prod.secrets.toml`:

```toml
[runtime]
slack_incoming_hook = "https://hooks.slack.com/services/T…/B…/…"
```

dlt picks this up automatically and exposes it at runtime as `pipeline.runtime_config.slack_incoming_hook`. To also get notifications from local runs, mirror the same `[runtime]` block into `.dlt/dev.secrets.toml`.

### Wire it into your pipeline

```python
import time
from datetime import datetime, timezone

import dlt
from dlt.common.runtime.slack import send_slack_message
from dlt.hub import run


@run.pipeline("my_pipeline")
def my_job():
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination="warehouse",
        dataset_name="my_dataset",
    )

    hook = pipeline.runtime_config.slack_incoming_hook
    started = time.time()
    try:
        load_info = pipeline.run(my_source())
        if hook:
            send_slack_message(
                hook,
                "\n".join([
                    f":white_check_mark: *`{pipeline.pipeline_name}` succeeded*",
                    f"*Finished:* {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}",
                    f"*Duration:* {time.time() - started:.1f}s",
                    f"*Load ID:* `{load_info.loads_ids[-1]}`",
                ]),
            )
    except Exception as e:
        if hook:
            send_slack_message(
                hook,
                f":x: *`{pipeline.pipeline_name}` failed*: `{type(e).__name__}: {e}`",
            )
        raise
```

The `if hook:` check skips the Slack call when no webhook is configured. The same script works in any profile, whether you've set up notifications or not.

:::tip Notify on schema changes
You can also notify Slack whenever a load surfaces new tables or columns. The dlt chess pipeline shows this pattern by inspecting `schema_update` on each load package and posting a message when new tables or columns appear.
:::

### Deploy and trigger

```sh
uv run dlthub deploy                            # syncs code + prod secret
uv run dlthub run my_job                        # triggers the job, posts to Slack on completion
```
