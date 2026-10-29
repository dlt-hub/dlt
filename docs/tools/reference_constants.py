"""Constants and text of the generated CLI, REST API and Python SDK references.

Templates are filled with `str.format`.
"""

import pathlib

DOCS_DIR = pathlib.Path(__file__).resolve().parents[1]

CLI_DEFAULT_TITLE = "dlt CLI reference"

# `title` is the page title, `sidebar_label` keeps the navigation label stable
CLI_HEADER_TEMPLATE = """---
title: {title}
sidebar_label: Command Line Interface
description: Command line interface (CLI) full reference of dlt
keywords: [command line interface, cli, dlt init]
---


# Command Line Interface Reference

<!-- this page is fully generated from the argparse object of dlt, do not edit -->

This page contains all commands available in the dlt CLI and is generated
automatically from the fully populated python argparse object of dlt.
:::note
Flags and positional commands are inherited from the parent command. Position within the command string
is important. For example if you want to enable debug mode on the pipeline command, you need to add the
debug flag to the base dlt command:

```sh
dlt --debug pipeline
```

Adding the flag after the pipeline keyword will not work.
:::

"""

CLI_HEADER = CLI_HEADER_TEMPLATE.format(title=CLI_DEFAULT_TITLE)

CLI_INHERITS_TEMPLATE = "Inherits arguments from [`{parent}`](#{anchor})."
CLI_VERSIONS_TEMPLATE = "This page is generated from {versions}.\n\n"

API_DEFAULT_OUTPUT_DIR = (
    DOCS_DIR / "website" / "docs_processed" / "hub" / "reference" / "api"
)

#: spec file stem -> (page slug, sidebar label, base URL and token in curl examples); also the
#: page order
API_SERVICES = {
    "api": ("runtime-api", "Runtime API", "https://api.dlthub.com", "$DLTHUB_API_KEY"),
    "dataplane_api": (
        "dataplane-api",
        "Dataplane API",
        "$DATAPLANE_URL",
        "$DATAPLANE_TOKEN",
    ),
    "telemetry": (
        "telemetry-api",
        "Telemetry API",
        "$DATAPLANE_URL",
        "$DATAPLANE_TOKEN",
    ),
    "logs": ("logs-api", "Logs API", "$DATAPLANE_URL", "$DATAPLANE_TOKEN"),
}

API_HTTP_METHODS = ("get", "post", "put", "patch", "delete")

#: tag of operations that have none
API_DEFAULT_TAG = "endpoints"

#: framework default for timezone-aware datetimes, says nothing the type does not
API_DATETIME_BOILERPLATE = (
    "datetime with the constraint that the value must have timezone info"
)

#: nesting depth to which bodies are expanded, deeper objects show as `object`
API_MAX_DEPTH = 3

#: schemas shown as `object` without their fields: dlt's own job definition, documented with dlt
API_OPAQUE_SCHEMAS = ("TJobDefinition",)

#: values of string formats in example responses
API_FORMAT_EXAMPLES = {
    "date-time": "2026-01-01T00:00:00Z",
    "date": "2026-01-01",
    "uuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "uri": "https://example.com",
    "email": "user@example.com",
}

#: query parameters every list operation takes, described once on the API overview page
API_PAGINATION_PARAMS = ("limit", "offset", "cursor", "order")

#: uploads take the token returned by the matching `-issue-upload` operation
API_UPLOAD_TOKEN = "$UPLOAD_TOKEN"

#: response media types the Python example reads line by line
API_STREAM_MEDIA_TYPES = ("text/event-stream", "application/x-ndjson")

API_KEYWORDS = ["dlthub", "runtime", "rest api", "openapi"]
API_DESCRIPTION_TEMPLATE = "{label} reference of the dltHub Runtime"
API_GENERATED_TEMPLATE = "<!-- generated from the {title} OpenAPI spec, do not edit -->"

#: marks required parameters and request fields at the start of their description
API_REQUIRED = "**Required.**"
API_ID_DESCRIPTION_TEMPLATE = "ID of the {name}"
API_VALUES_TEMPLATE = "One of: {values}."
API_DEFAULT_TEMPLATE = "Default: `{default}`."
API_PAGINATION_TEMPLATE = "**Pagination:** {params}, see [Lists and pagination](overview.md#lists-and-pagination)"
API_OPTIONAL_BODY = "The body is optional."
API_MEDIA_TYPE_TEMPLATE = "Sent as `{media_type}`."
API_RETURNS_TEMPLATE = "`{status}` returns {media_types}."
API_RETURNS_NOTHING_TEMPLATE = "`{status}` returns no body."
#: the error body every service shares, documented once on the API overview page
API_ERROR_BODY = "Each error returns the [error body](overview.md#errors)."
API_DEPRECATED = ":::warning\nThis endpoint is deprecated.\n:::"
API_DEFAULT_STATUS = "Any other status"
API_STATUS_RANGE_TEMPLATE = "Any {status} status"

SDK_PACKAGE = "dlthub_sdk"
SDK_DISTRIBUTION = "dlthub-client"

#: module (relative to the package) -> section label; also the section order
SDK_SECTIONS = {
    "": "Connecting",
    "domain.runtime": "Runtime",
    "domain.organizations": "Organizations",
    "domain.workspaces": "Workspaces",
    "domain.caller": "Caller",
    "domain.dataplanes": "Data planes",
    "domain.deployments": "Deployments",
    "domain.configurations": "Configurations",
    "domain.files": "Files",
    "domain.jobs": "Jobs",
    "domain.job_runs": "Job runs",
    "domain.logs": "Logs",
    "domain.variables": "Variables",
    "domain.telemetry": "Telemetry",
    "credentials": "Credentials",
    "errors": "Errors",
}

#: section of the private modules
SDK_CONVENTIONS = "Conventions"

#: section ordered by the class tree instead of by role
SDK_ERRORS = "Errors"

#: page slug -> (title, section labels); also the page order. A section of a new module gets a
#: page of its own
SDK_PAGES = {
    "getting-started": ("Getting started", []),
    "client": ("Client", ["Connecting", "Runtime", "Credentials", SDK_CONVENTIONS]),
    "organizations-and-workspaces": (
        "Organizations and workspaces",
        ["Organizations", "Workspaces", "Caller", "Data planes"],
    ),
    "deployments": (
        "Deployments and configurations",
        ["Deployments", "Configurations", "Files"],
    ),
    "jobs": ("Jobs", ["Jobs"]),
    "job-runs": ("Job runs and logs", ["Job runs", "Logs"]),
    "variables": ("Variables", ["Variables"]),
    "telemetry": ("Telemetry", ["Telemetry"]),
    "errors": ("Errors", [SDK_ERRORS]),
}

SDK_GETTING_STARTED = "getting-started"

#: section label -> text above its objects
SDK_SECTION_INTROS = {
    SDK_CONVENTIONS: "Update methods leave a field as it is when you pass `KEEP`, the default of"
    " every optional update argument, clear it when you pass `None` and set it when you pass a"
    " value.",
}

#: base class -> rank of its subclasses within a section: how you reach an object, the object,
#: what it returns and takes, then enums
SDK_ROLES = {
    "Namespace": 0,
    "Collection": 1,
    "Entity": 2,
    "TypedDict": 4,
    "Protocol": 4,
    "StrEnum": 5,
}

#: doc id of the folder the pages are served from
SDK_DOC_ID = "hub/reference/python-sdk"
SDK_TITLE = "Python SDK reference"
SDK_KEYWORDS = ["dlthub", "runtime", "python sdk", "dlthub_sdk", "api"]
SDK_SIDEBAR_DESCRIPTION = f"Reference of the dltHub Runtime Python SDK ({SDK_PACKAGE})"
SDK_PAGE_DESCRIPTION_TEMPLATE = (
    "{title} {preposition} the dltHub Runtime Python SDK (" + SDK_PACKAGE + ")"
)
SDK_GENERATED_TEMPLATE = (
    "<!-- generated from " + SDK_DISTRIBUTION + " {version}, do not edit -->"
)
SDK_VERSION_TEMPLATE = (
    "This reference is generated from `" + SDK_DISTRIBUTION + "` {version}."
)
SDK_SUBCLASS_TEMPLATE = "Subclass of {bases}."
SDK_COLLECTIONS_TITLE = "[Collections](getting-started.md#collections-and-attributes)"

SDK_INTRO = f"""\
:::info
The Python SDK and the [REST API](../api/overview.md) are in public preview. Their interfaces will
remain stable from this point onward and will only be extended. The packaging of the SDK may
still change: it can move to another PyPI package and another top-level package name than
`{SDK_PACKAGE}`.
:::

The Python SDK is the `{SDK_PACKAGE}` package. It ships with `{SDK_DISTRIBUTION}`, which `dlt[hub]`
installs:

```sh
pip install "dlt[hub]"
```

Connect with an API key. The client closes when the `with` block ends:

```python
import dlthub_sdk

with dlthub_sdk.connect(token="dlt_u_...") as runtime:
    for organization in runtime.organizations.list():
        for workspace in organization.workspaces.list():
            print(organization.name, workspace.id, workspace.name)
```

Every object leads to the objects that belong to it, so you go from the client to a workspace,
from the workspace to a job, and from the job to its runs and their logs:

```python
with dlthub_sdk.connect(token="dlt_u_...") as runtime:
    workspace = runtime.workspaces.get(id="<workspace id>")
    job = workspace.jobs.get_by_ref("jobs.load")

    run = job.start_run()
    for line in run.stream_logs():
        print(line.content)
    run = run.wait()
    print(run.status)

    for past_run in job.runs.list(limit=5):
        print(past_run.number, past_run.status, past_run.duration_seconds)
```

A workspace also reaches its variables, deployments, configurations and pipeline telemetry:

```python
with dlthub_sdk.connect(token="dlt_u_...") as runtime:
    workspace = runtime.workspaces.get(id="<workspace id>")

    workspace.variables.set(
        "SOURCES__API__TOKEN", "...", profile=dlthub_sdk.WORKSPACE_PROFILE, secret=True
    )
    print(workspace.deployments.latest().version)
    for pipeline in workspace.telemetry.pipelines():
        print(pipeline.pipeline_name, pipeline.latest_status, pipeline.total_rows_loaded)
```

[`connect_async()`](client.md#connect_async) gives the same objects for `asyncio`. Methods that call the
platform return awaitables and lists are async iterables:

```python
async with dlthub_sdk.connect_async(token="dlt_u_...") as runtime:
    workspace = await runtime.workspaces.get(id="<workspace id>")
    job = await workspace.jobs.get_by_ref("jobs.load")
    run = await job.start_run()
    async for line in run.stream_logs():
        print(line.content)
```

## Collections and attributes

Each object in this reference lists its collections and its attributes:

- **Collections** lead to the objects that belong to it, such as `workspace.jobs` or `job.runs`.
  `list()` iterates over their objects and `get()` fetches one by its ID; most can also
  `count()` them. A few, such as `workspace.variables` and `workspace.telemetry`, group operations
  instead of holding objects.
- **Attributes** are the data the object carries. `to_dict()` returns them as a plain dict.

Types in this reference are those of the blocking client. API keys, base URLs and the data plane
are explained in the [REST API reference](../api/overview.md), which the SDK is built on."""

#: shown at the top of every page but the getting started page
SDK_START_HERE = "New to the SDK? [Getting started](getting-started.md) shows how to connect and use it."
