---
title: Overview
description: Overview of the dltHub Runtime REST API, its base URLs and authentication
keywords: [dlthub, runtime, rest api, openapi, api key, authentication]
---
# Overview

:::info
The REST API and the [Python SDK](../python-sdk/getting-started.md) are in public preview. Their interfaces will
remain stable from this point onward and will only be extended.
:::

The dltHub Runtime has a REST API for everything the `dlthub` [CLI](../../command-line-interface.md) and the
[dltHub web app](https://app.dlthub.com) do: manage workspaces, deploy code, trigger and
cancel jobs, read run logs and pipeline telemetry.

It is split across two planes:

- the **control plane** at `https://api.dlthub.com` holds organizations, workspaces,
  deployments, jobs and runs
- the **data plane** of your workspace's region holds its data: workspace variables,
  deployment files, run logs and telemetry

| API                               | What it covers                                                         | Base URL                  | Credential       |
| --------------------------------- | ---------------------------------------------------------------------- | ------------------------- | ---------------- |
| [Runtime API](runtime-api.md)     | Organizations, workspaces, deployments, configurations, jobs, job runs | `https://api.dlthub.com`  | API key          |
| [Dataplane API](dataplane-api.md) | Workspace variables, deployment and configuration files                | Workspace `dataplane_url` | Data plane token |
| [Logs API](logs-api.md)           | Logs of job runs, stored or live                                       | Workspace `dataplane_url` | Data plane token |
| [Telemetry API](telemetry-api.md) | Pipeline runs, traces, load results, dataset and pipeline statistics   | Workspace `dataplane_url` | Data plane token |

The pages are generated from the OpenAPI specs that the `dlthub` CLI and its Python SDK are
built from, so they always match a released client.

## Authentication

Every request carries a bearer token in the `Authorization` header. Use an API key for the
control plane:

```sh
curl https://api.dlthub.com/api/v1/me \
  -H "Authorization: Bearer $DLTHUB_API_KEY"
```

Create API keys in the [dltHub web app](https://app.dlthub.com). Each key is shown once, when
you create it. Keys expire after 90 days unless another lifetime, up to 365 days, is set when the key is created.
Deleting a key revokes it immediately.

| Key           | Prefix    | Where to create it | Acts as                                                                |
| ------------- | --------- | ------------------ | ---------------------------------------------------------------------- |
| Personal key  | `dlt_u_`  | Account settings   | You, with your roles in every organization and workspace you belong to |
| Workspace key | `dlt_sa_` | Workspace settings | A service account of one workspace with a fixed role                   |

A workspace key has the DEVELOPER role, which can deploy and run jobs, or the VIEWER role,
which can read and run jobs that use the read-only `access` profile, such as interactive jobs.
Use workspace keys for CI and other automation, so access does not depend on a person.
Workspace keys cannot call `GET /api/v1/me`, which answers only for people.

## Calling the data plane

The data plane services do not accept API keys. Exchange your API key for a short-lived data
plane token, then call the workspace's data plane with it:

1. Get the data plane address of the workspace from
   [`GET /api/v1/workspaces/{workspace_id}`](runtime-api.md):

   ```sh
   curl https://api.dlthub.com/api/v1/workspaces/$WORKSPACE_ID \
     -H "Authorization: Bearer $DLTHUB_API_KEY"
   ```

   The response contains `dataplane_url`, for example `https://eu1.dlthub.com`.

2. Issue a data plane token with
   [`POST /api/v1/workspaces/{workspace_id}/-issue-dataplane-token`](runtime-api.md):

   ```sh
   curl -X POST https://api.dlthub.com/api/v1/workspaces/$WORKSPACE_ID/-issue-dataplane-token \
     -H "Authorization: Bearer $DLTHUB_API_KEY"
   ```

   The response contains `token` and `expires_at`, a Unix timestamp. The token is valid for
   this workspace only, currently for 10 minutes. Request a new one before it expires.

3. Call a data plane service with the token, for example the logs of a job run:

   ```sh
   curl $DATAPLANE_URL/logs/v1/workspaces/$WORKSPACE_ID/job-runs/$JOB_RUN_ID/logs \
     -H "Authorization: Bearer $DATAPLANE_TOKEN"
   ```

The data planes today:

| Region                       | `dataplane_url`          |
| ---------------------------- | ------------------------ |
| Europe (`eu1`, europe-west4) | `https://eu1.dlthub.com` |
| US East (`us1`, us-east4)    | `https://us1.dlthub.com` |

Read `dataplane_url` from the workspace instead of hard-coding a region, as new regions can be
added.

Every workspace member can read logs, telemetry and file lists. Only workspace owners can read
or change [variables](dataplane-api.md), so workspace API keys, which are DEVELOPER or VIEWER,
cannot.

Uploading a deployment or configuration takes its own token: the `-issue-upload` operations of
the [Runtime API](runtime-api.md) return an `upload_token` and the `upload_url` to send the
files to. The token is valid for 10 minutes and only for the deployment or configuration it
was issued for.

## Lists and pagination

List endpoints take the same query parameters to page through their results:

| Parameter | Meaning                                                                                                                               |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `limit`   | Maximum number of items to return, at most 1000                                                                                       |
| `offset`  | Number of items to skip, at most 10000; narrow with filters instead of paging deeper                                                  |
| `cursor`  | The `next_cursor` of the previous page; returns the rows after it under the same `sort` and `order`. Cannot be combined with `offset` |
| `order`   | Sort directions, `asc` or `desc`, one per `sort` key and in the same order. Required whenever `sort` is given                         |

The keys `sort` accepts differ per endpoint and are listed with it. List endpoints return a page
of results:

| Field         | Meaning                                                                        |
| ------------- | ------------------------------------------------------------------------------ |
| `items`       | The rows of this page                                                          |
| `total`       | Rows matching the filters, counted up to 10001                                 |
| `limit`       | The page size, set with the `limit` parameter (at most 1000)                   |
| `offset`      | Rows skipped before this page, set with the `offset` parameter (at most 10000) |
| `next_cursor` | Pass as `cursor` to get the next page; `null` on the last page                 |

Prefer `cursor` over `offset` for walking through long lists. `cursor` and `offset` cannot be
combined.

## Errors

A failed request answers with a 4xx or 5xx status and an error body:

```json
{
  "status_code": 401,
  "code": "token_expired",
  "detail": "<human-readable message>"
}
```

| Field         | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| `status_code` | The HTTP status code                                         |
| `code`        | Machine-readable error code, when the error has one          |
| `detail`      | Human-readable description of the error                      |
| `extra`       | Additional error details, free-form; omitted for most errors |

`detail` is meant for people. Check `code` in programs: most 4xx errors carry one, for example
`token_expired` when a data plane token ran out or `permission_denied` when your role does
not allow the call. New codes can be added at any time, so treat a code you do not know like
any other error of its status. Errors without a code, such as a 404 for an unknown path or a
path parameter that is not a valid ID, and all 5xx errors, are told apart by their status only.

## Versioning

The API version is part of the path. Endpoints under `/v1` are stable: breaking changes ship
only under a new version prefix. New endpoints, optional parameters and response fields can be
added to `/v1` at any time, so ignore fields and enum values you do not know.

## URL structure

Paths are built from a few kinds of segments, so a URL tells you what a request does:

| Kind       | Example                                     | Meaning                                                                |
| ---------- | ------------------------------------------- | ---------------------------------------------------------------------- |
| Collection | `GET …/workspaces/{workspace_id}/jobs`      | A set of resources. `GET` lists them, `POST` creates one.              |
| Item       | `GET …/jobs/{job_id}`                       | One resource of a collection, addressed by its ID. `PATCH` changes it. |
| Singleton  | `GET …/workspaces/{workspace_id}/variables` | Exactly one resource at a fixed name, such as `me` or `variables`.     |
| Alias      | `GET …/deployments/latest`                  | A reserved name in place of an ID: `latest` is the newest item.        |
| Report     | `GET …/pipelines/-/stats`                   | An aggregate over the collection, behind the `-` separator.            |
| Verb       | `POST …/jobs/{job_id}/-pause`               | An action, spelled with a leading `-`.                                 |

A segment that starts with `-` followed by a name is a verb, such as `-pause`, `-trigger` or
`-issue-upload`. A bare `-` only separates a report from its collection. Verbs are always called
with `POST`. They are used only where a call does more than store a field: it starts work
(`-trigger`, `-cancel`), runs asynchronously, or is a state change with side effects (`-pause`,
`-resume`). Changing a plain field is a `PATCH` on the item instead. A reversible state change is
a pair of verbs, such as `-archive` and `-unarchive`.

A verb on an item acts on that item. The same verb on the collection acts in bulk on the items
whose IDs the request body lists, for example `POST …/job-runs/-cancel`. A few verbs act on the
collection itself: `POST …/deployments/-issue-upload` issues a grant to upload a new deployment.

IDs in paths are UUIDs. To find an item by another key, filter its collection, for example
`GET …/jobs?ref=my-job`.
