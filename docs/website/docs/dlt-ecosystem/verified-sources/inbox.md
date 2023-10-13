---
title: Inbox
description: dlt verified source for Mail Inbox
keywords: [inbox, inbox verified source, inbox mail, email]
---

# Inbox

:::info Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our Slack community](https://dlthub-community.slack.com/join/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g)
or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::

This source collects inbox emails, retrieves attachments, and stores relevant email data. It uses the imaplib library for IMAP interactions and the dlt library for data processing.

This Inbox `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/inbox_pipeline.py)
loads data using “Inbox” verified source to the destination of your choice.

Sources and resources that can be loaded using this verified source are:

| Name              | Description                                        |
|-------------------|----------------------------------------------------|
| inbox_source      | Gathers inbox emails and saves attachments locally |
| get_messages_uids | Retrieves messages UUIDs from the mailbox          |
| get_messages      | Retrieves emails from the mailbox using given UIDs |
| get_attachments   | Downloads attachments from emails using given UIDs |

## Setup Guide

### Grab credentials

1. For verified source configuration, you need:
   - "host": IMAP server hostname (e.g., Gmail: imap.gmail.com, Outlook: imap.gmail.com).
   - "email_account": Associated email account name.
   - "password": APP password (for third-party clients) from the email provider.

1. Host addresses and APP password procedures vary by provider and can be found via a quick Google search. For Google Mail's app password, read [here](https://support.google.com/mail/answer/185833?hl=en#:~:text=An%20app%20password%20is%20a,2%2DStep%20Verification%20turned%20on).

1. However, this guide covers Gmail inbox configuration; similar steps apply to other providers.

### Accessing Gmail Inbox

1. SMTP server DNS: 'imap.gmail.com' for Gmail.
1. Port: 993 (for internet messaging access protocol over TLS/SSL).

### Grab App password for Gmail

1. An app password is a 16-digit code allowing less secure apps/devices to access your Google Account, available only with 2-Step Verification activated.

#### Steps to Create and Use App Passwords:

1. Visit your Google Account > Security.
1. Under "How you sign in to Google", enable 2-Step Verification.
1. Choose App passwords at the bottom.
1. Name the device for reference.
1. Click Generate.
1. Input the generated 16-character app password as prompted.
1. Click Done.

Read more in [this article](https://pythoncircle.com/post/727/accessing-gmail-inbox-using-python-imaplib-module/) or [Google official documentation.](https://support.google.com/mail/answer/185833#zippy=%2Cwhy-you-may-need-an-app-password)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```bash
   dlt init inbox duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/inbox_pipeline.py)
   with Inbox as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the
[Walkthrough: Add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credential

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can
   securely store your access tokens and other sensitive information. It's important to handle this
   file with care and keep it safe. Here's what the file looks like:

   ```toml
   # put your secret values and credentials here
   # do not share this file and do not push it to github
   [sources.inbox]
   host = "Please set me up!" # The host address of the email service provider.
   email_account = "Please set me up!" # Email account associated with the service.
   password = "Please set me up!" # # APP Password for the above email account.
   ```

1. Replace the host, email and password value with the [previously copied one](#grab-credentials)
   to ensure secure access to your Inbox resources.
   > When adding the App Password, remove any spaces. For instance, "abcd efgh ijkl mnop" should be "abcdefghijklmnop".

1. Next, follow the [destination documentation](../../dlt-ecosystem/destinations) instructions to
   add credentials for your chosen destination, ensuring proper routing of your data to the final
   destination.

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```bash
   pip install -r requirements.txt
   ```

   Prerequisites for fetching messages differ by provider. For Gmail:

   - Python 3.x
   - dlt library: pip install dlt
   - PyPDF2: pip install PyPDF2
   - Specific destinations, e.g., duckdb: pip install duckdb
   - (Note: Confirm based on your service provider.)

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```bash
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `standard_inbox`, you may also
   use any custom name instead.

For more information, read the [Walkthrough: Run a pipeline.](../../walkthroughs/run-a-pipeline)

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `inbox_source`

This function fetches inbox emails, saves attachments locally, and returns uids, messages, and attachments as resources.

```python
@dlt.source
def inbox_source(
    host: str = dlt.secrets.value,
    email_account: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    folder: str = "INBOX",
    gmail_group: Optional[str] = GMAIL_GROUP,
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
    filter_emails: Sequence[str] = None,
    filter_by_mime_type: Sequence[str] = None,
    chunksize: int = DEFAULT_CHUNK_SIZE,
) -> Sequence[DltResource]:
```

`host` : IMAP server hostname. Default: 'dlt.secrets.value'.

`email_account`: Email login. Default: 'dlt.secrets.value'.

`password`:  Email App password. Default: 'dlt.secrets.value'.

`folder`: Mailbox folder for collecting emails. Default: 'INBOX'.

`gmail_group`: Google Group email for filtering. Default: settings 'GMAIL_GROUP'.

`start_date`: Start date to collect emails. Default: `/inbox/settings.py` 'DEFAULT_START_DATE'.

`filter_emails`:Email addresses for 'FROM' filtering. Default: settings 'FILTER_EMAILS'.

`filter_by_mime_type`: MIME types for attachment filtering. Default: [].

`chunksize`: UIDs collected per batch. Default: settings 'DEFAULT_CHUNK_SIZE'.

### Resource `get_messages_uids`

This function retrieves email message UIDs (Unique IDs) from the mailbox.

```python
    @dlt.resource(name="uids")
    def get_messages_uids(
        initial_message_num: Optional[
            dlt.sources.incremental[int]
        ] = dlt.sources.incremental("message_uid", initial_value=1),
    ) -> TDataItem:
```

`initial_message_num`: provides incremental loading on UID.

### Resource `get_messages`

This function reads mailbox emails using the given UIDs.

```python
@dlt.transformer(name="messages", primary_key="message_uid")
def get_messages(
    items: TDataItems,
    include_body: bool = True,
) -> TDataItem:
```

`items` (TDataItems): An iterable of dictionaries with 'message_uid' for email UIDs.

`include_body` (bool, optional): Includes email body if True. Default: True.

Similar to the previous resources, resource `get_attachments` downloads email attachments using the provided UIDs.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```python
   pipeline = dlt.pipeline(
       pipeline_name="standard_inbox",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="standard_inbox_data"  # Use a custom name if desired
       full_refresh=True,
   )
   ```
   To read more about pipeline configuration, please refer to our
   [documentation](../../general-usage/pipeline).

1. To load messages from "mycreditcard@bank.com" starting "2023-10-1":

    - Set DEFAULT_START_DATE = pendulum.datetime(2023, 10, 1) in "./inbox/settings.py".
    - Use the following code:
      ```python
      # Retrieve messages from the specified email address.
      messages = inbox_source(filter_emails=("mycreditcard@bank.com",)).messages
      # Configure messages to exclude body and name the result "my_inbox".
      messages = messages(include_body=False).with_name("my_inbox")
      # Execute the pipeline and load messages to the "my_inbox" table.
      load_info = pipeline.run(messages)
      # Print the loading details.
      print(load_info)
      # Return the configured pipeline.
      return pipeline
      ```
1. To load messages from multiple emails, including "community@dlthub.com":

   ```python
   messages = inbox_source(filter_emails=("mycreditcard@bank.com", "community@dlthub.com.")).messages

   --- rest of the code
   ```

1. In "inbox_pipeline.py", the "pdf_to_text" transformer extracts text from PDFs, treating each page as a separate data item.
   Here is the code for the "pdf_to_text" transformer function:
   ```python
   @dlt.transformer(primary_key="file_hash", write_disposition="merge")
   def pdf_to_text(file_items: Sequence[FileItemDict]) -> Iterator[Dict[str, Any]]:
      # extract data from PDF page by page
       for file_item in file_items:
           with file_item.open() as file:
               reader = PdfReader(file)
               for page_no in range(len(reader.pages)):
                   # add page content to file item
                   page_item = {}
                   page_item["file_hash"] = file_item["file_hash"]
                     page_item["text"] = reader.pages[page_no].extract_text()
                   page_item["subject"] = file_item["message"]["Subject"]
                   page_item["page_id"] = file_item["file_name"] + "_" + str(page_no)
                   # TODO: copy more info from file_item
                 yield page_item
     ```
1. Using the "pdf_to_text" function to load parsed pdfs from mail to the database:

   ```python
   filter_emails = ["mycreditcard@bank.com", "community@dlthub.com."] # Email senders
   attachments = inbox_source(
        filter_emails=filter_emails, filter_by_mime_type=["application/pdf"]
   ).attachments

   # Process attachments through PDF parser and save to 'my_pages' table.
   load_info = pipeline.run((attachments | pdf_to_text).with_name("my_pages"))
   # Display loaded data details.
   print(load_info)
   return pipeline
   ```