---
sidebar_position: 13
---

# Command Line Interface

## `dlt init`

```
dlt init <source> <destination>
```

If you want to start from a [generic template](https://github.com/dlt-hub/python-dlt-init-template),
then run `dlt init` with a [source](./glossary.md#source) name of your choice and one of the three
[destination](./destinations.md) options. The optional `--generic` flag will provide a more complex
example, which can be used to speed up implementation if you have built `dlt` pipelines before.

If you don't want to start with a generic template, then try out the chess.com API to Google BigQuery
complete example by running `dlt init chess bigquery`.

Follow the [Create a pipeline](./walkthroughs/create-a-pipeline.md) walkthrough to learn more.

## `dlt deploy`

```
dlt deploy <script>.py github-action --schedule "*/30 * * * *"
```

[GitHub Actions](https://github.com/features/actions) are the only supported deployment method at the moment.
You need to specify when the GitHub Action should run using a [cron schedule expression](https://crontab.guru/). The command also takes additional flags: `--run-on-push` (default is False) and `--run-manually` (default is True). Remember to put the cron schedule into quotation marks as in the example above. 

For the chess.com API example above, you could deploy it with `dlt deploy chess.py github-action --schedule "*/30 * * * *"`.

Follow the [Deploy a pipeline](./walkthroughs/deploy-a-pipeline.md) walkthrough to learn more.

## `dlt pipeline show`

```
dlt pipeline <pipeline name> show
```

Generates and launches a simple [Streamlit](https://streamlit.io/) app that you can use to inspect the schemas and data in the destination as well as your pipeline state and loading status / stats. Requires `streamlit` to be installed.