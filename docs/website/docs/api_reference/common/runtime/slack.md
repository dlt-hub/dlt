---
sidebar_label: slack
title: common.runtime.slack
---

#### send\_slack\_message

```python
def send_slack_message(incoming_hook: str,
                       message: str,
                       is_markdown: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/slack.py#L5)

Sends a `message` to  Slack `incoming_hook`, by default formatted as markdown.

