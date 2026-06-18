---
title: Send email notifications
description: Notify by email when a dltHub job succeeds or fails, using Gmail SMTP.
keywords: [email, smtp, gmail, notifications, alerting, hub, dltHub]
---

# Send email notifications

The pattern below uses Python's standard `smtplib` with Gmail SMTP, but the same shape works for any SMTP server (Outlook, Workspace SMTP relay, or transactional providers like Resend, SendGrid, Mailgun).

## Prerequisites

Generate a Gmail **App Password**, a 16-character credential that lets SMTP authenticate without your real password:

1. Make sure **2-Step Verification** is enabled on the Google Account.
2. Open https://myaccount.google.com/apppasswords.
3. Create a new password and name it, e.g. "dltHub pipeline".
4. Copy the 16 characters. Google displays them with spaces (`abcd efgh ijkl mnop`); the spaces are decorative, so strip them.

App Passwords don't affect normal sign-in: your password, 2FA, and existing sessions are unchanged. You can revoke the App Password from the same page without touching the account.

If you're on a **Google Workspace** domain, an administrator may have disabled App Passwords org-wide. In that case use a transactional provider (Resend, SendGrid, Mailgun). The wiring is the same; just point `smtplib` at their SMTP server and use their API key as the password.

## Store credentials in your prod profile

Add to `.dlt/prod.secrets.toml`:

```toml
[notifications.email]
host = "smtp.gmail.com"
port = 587
sender = "you@example.com"
recipient = "you@example.com"
password = "abcdefghijklmnop"     # 16 chars, no spaces, no angle brackets
```

`sender` must be the **same Google Account** the App Password was generated on.

:::tip Allowlist outbound IPs
If your SMTP server requires IP allowlisting, enable [static egress IPs](../pipeline-operations/job-configuration.md#static-egress-ips) so the job's outbound traffic uses a known set of source IPs.
:::

## Wire it into your pipeline

```python
import smtplib
import time
from datetime import datetime, timezone
from email.message import EmailMessage

import dlt
from dlt.hub import run


def send_email(subject: str, body: str) -> None:
    host = dlt.secrets["notifications.email.host"]
    port = int(dlt.secrets["notifications.email.port"])
    sender = dlt.secrets["notifications.email.sender"]
    recipient = dlt.secrets["notifications.email.recipient"]
    password = dlt.secrets["notifications.email.password"]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.set_content(body)

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(sender, password)
        s.send_message(msg)


@run.pipeline("my_pipeline")
def my_job():
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination="warehouse",
        dataset_name="my_dataset",
    )

    started = time.time()
    try:
        load_info = pipeline.run(my_source())
        send_email(
            f"[dltHub] {pipeline.pipeline_name} succeeded",
            "\n".join([
                f"Pipeline:  {pipeline.pipeline_name}",
                f"Status:    SUCCESS",
                f"Finished:  {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}",
                f"Duration:  {time.time() - started:.1f}s",
                f"Load ID:   {load_info.loads_ids[-1]}",
            ]),
        )
    except Exception as e:
        try:
            send_email(
                f"[dltHub] {pipeline.pipeline_name} FAILED",
                f"Pipeline:  {pipeline.pipeline_name}\nError:     {type(e).__name__}: {e}",
            )
        except Exception as mail_err:
            print(f"Failed to send failure email: {mail_err}")
        raise
```

Wrap the failure-path `send_email` in its own try/except: a broken alerting channel shouldn't mask the underlying pipeline error.

## Deploy and trigger

```sh
uv run dlthub deploy                            # syncs code + SMTP credentials
uv run dlthub run my_job                        # triggers the job, email lands on completion
```
