---
title: Send Slack notifications
description: Notify Slack when a dltHub job succeeds or fails.
keywords: [slack, notifications, alerting, hub, dltHub]
---

# Send Slack notifications

dlt ships a small helper, `send_slack_message`, that posts to a Slack [incoming webhook](https://api.slack.com/messaging/webhooks). Combined with `pipeline.runtime_config.slack_incoming_hook`, it gives you a one-line way to alert a channel when a job finishes or fails.

## Prerequisites

Create an incoming webhook for the channel that should receive alerts:

1. Open https://api.slack.com/messaging/webhooks.
2. Create a new Slack app (or pick an existing one), enable **Incoming Webhooks**, and add a webhook to your workspace.
3. Pick the destination channel. You'll get a URL of the form `https://hooks.slack.com/services/T…/B…/…`.

The webhook URL itself is the credential, so treat it as a secret.

## Store the webhook in your prod profile

Add to `.dlt/prod.secrets.toml`:

```toml
[runtime]
slack_incoming_hook = "https://hooks.slack.com/services/T…/B…/…"
```

dlt picks this up automatically and exposes it at runtime as `pipeline.runtime_config.slack_incoming_hook`. To also get notifications from local runs, mirror the same `[runtime]` block into `.dlt/dev.secrets.toml`.

## Wire it into your pipeline

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
You can also notify Slack whenever a load surfaces new tables or columns. The [dlt chess pipeline](../../examples/chess_production.md) shows this pattern by inspecting `schema_update` on each load package and posting a message when new tables or columns appear.
:::

## Deploy and trigger

```sh
uv run dlthub deploy                            # syncs code + prod secret
uv run dlthub run my_job                        # triggers the job, posts to Slack on completion
```
