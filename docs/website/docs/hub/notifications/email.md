---
title: Send email notifications
description: Subscribe to dltHub workspace failure and success alerts, or send custom emails from your pipeline code with SMTP.
keywords: [email, alerts, notifications, failure alerts, run alerts, hub, dltHub, smtp]
---
# Send email notifications

dltHub provides native email alerting managed directly from the Web UI, with no pipeline code changes required. You can also send custom emails directly from your pipeline code using SMTP.

---

## dltHub platform alerts

:::tip Recommended
Configuring alerts in the dltHub Web UI is the preferred way to monitor your workloads. It requires no code or secrets in your repository and captures platform-level failures (such as container crashes, timeouts, memory limits, and dependency issues) that occur outside your pipeline code.
:::

With platform alerts, dltHub sends transactional email notifications when pipeline runs fail or complete successfully.

### How to configure

1. Open the dltHub Web UI and navigate to your workspace.

2. In the left navigation, go to **Settings > Alerts** (`/w/<workspace_id>/settings/alerts`).

3. Under **Alerts Configuration**, locate the trigger you want to configure:

  - **Job run failures**: Fires whenever a job or pipeline run transitions to failed status.
  - **Job run successes**: Fires whenever a job or pipeline run transitions to completed status.

4. Toggle the alert switch **On**.

5. Under **Email Recipients**, select who should receive the email:

  - **Workspace owners only**: Default for failure alerts. Delivers to users with the Workspace Owner or Org Owner role.
  - **All workspace members**: Sends notifications to all human members belonging to the workspace.
  - **( ) No email recipients**: Disables email delivery for this trigger (useful when routing notifications exclusively to Slack).

6. Under **Scope**, choose which jobs to monitor:

  - **All jobs**: Delivers alerts for any job run in the workspace.
  - **Specific pipelines**: Filters alerts to a selected list of pipelines.

7. Click **Save** in the bottom changes bar to apply your configuration.

### What the email includes

Platform alert emails are pre-formatted and sent via high-deliverability infrastructure. Each email includes:

- The name of the pipeline and workspace.
- The failure reason and error excerpt (for failure alerts).
- The exact UTC timestamp of the run.
- A direct link to open the run details and logs in the dltHub Web UI.

---

## Custom email notification on pipeline failure (in-code alternative)

:::note When to use custom in-code emails
Platform alerts above notify you when a **job run** fails, whatever the cause. The custom notification below covers **pipeline failures** only: it runs inside your job, so it cannot report a job that fails before your code runs or is killed mid-run by the container runtime.

Use this custom approach only if you need custom email templates, attachments, or need to send alerts to external recipients outside your dltHub workspace.
:::

If you need a different channel, recipient list, or message body than the platform alerts provide, send the email from the job itself. The pattern below uses Python's standard `smtplib` with Gmail SMTP, but the same shape works for any SMTP server (Outlook, Workspace SMTP relay, or transactional providers like Resend, SendGrid, Mailgun).

### Prerequisites

Generate a Gmail **App Password**, a 16-character credential that lets SMTP authenticate without your real password:

1. Make sure **2-Step Verification** is enabled on the Google Account.
2. Open [https://myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords).
3. Create a new password and name it, e.g. "dltHub pipeline".
4. Copy the 16 characters. Google displays them with spaces (`abcd efgh ijkl mnop`); the spaces are decorative, so strip them.

App Passwords don't affect normal sign-in: your password, 2FA, and existing sessions are unchanged. You can revoke the App Password from the same page without touching the account.

If you're on a **Google Workspace** domain, an administrator may have disabled App Passwords org-wide. In that case use a transactional provider (Resend, SendGrid, Mailgun). The wiring is the same; just point `smtplib` at their SMTP server and use their API key as the password.

### Store credentials in your prod profile

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

### Wire it into your pipeline

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

### Deploy and trigger

```sh
uv run dlthub deploy                            # syncs code + SMTP credentials
uv run dlthub run my_job                        # triggers the job, email lands on completion
```
