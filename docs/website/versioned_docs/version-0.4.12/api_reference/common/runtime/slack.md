---
sidebar_label: slack
title: common.runtime.slack
---

## send\_slack\_message

```python
def send_slack_message(incoming_hook: str,
                       message: str,
                       is_markdown: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runtime/slack.py#L5)

Sends a `message` to  Slack `incoming_hook`, by default formatted as markdown.

