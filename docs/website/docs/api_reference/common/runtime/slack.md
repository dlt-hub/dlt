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

Sends a `message` to  Slack `incoming_hook`, by default formatted as markdown.

