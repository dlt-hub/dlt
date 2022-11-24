---
sidebar_position: 7
---

# Command Line Interface

## `dlt init`

- `dlt init --help`
- chess example: `dlt init chess bigquery`
- generic template: `dlt init twitter bigquery`
- what dlt init does: https://github.com/dlt-hub/python-dlt-init-template
- See `create a pipeline` for more info

## `dlt deploy`

- dlt deploy twitter.py <how> --schedule * * * *
- <how> is "github-action" and it takes additional flags: --run-on-push [default False] --run-manually [default True]
- See `deploy a pipeline` for more info