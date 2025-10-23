---
title: Overview
---

# Workspace

1. install dlt with workspace support
```sh
[uv] pip install "dlt[workspace]"
```

2. do a regular dlt init, for example
```sh
dlt init dlthub:fal duckdb
```

3. At this moment "new workspace" is hidden behind feature flag
```sh
dlt --help
```
just returns regular set of commands

4. Enable new workspace by creating `.dlt/.workspace` file
```sh
touch .dlt/.workspace
```

5. Now a new set of commands is available, try
```sh
dlt workspace info
```

to get basic information of the workspace. Now you can see basic file layout:

```sh
.
├── .dlt/
│   ├── config.toml
|   ├── secrets.toml
|   ├── .workspace     # feature flag
│   └── .var/dev/    # working dir for pipelines for `dev` (default profile)
├── _local/dev                # locally loaded data: ducklake, duckdb databases etc will go there
├── .gitignore
├── requirements.txt
```

Now let's run a simple pipeline
```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name="foo",
    destination="duckdb",
    dataset_name="lake_schema",
    dev_mode=True,
)

info = pipeline.run(
    [{"foo": 1}, {"foo": 2}],
    table_name="table_foo",
)
print(info)
print(pipeline.dataset().table_foo["foo"].df())
```

From the output we see that data got loaded into `_local/dev/foo.duckdb` database and `dlt pipeline foo info`
tells us that pipelines working dir is in `.dlt/.var/dev/pipelines`. Further `dlt pipeline -l` shows just one pipeline belonging to current workspace.
**New Workspace fully isolates pipelines across different workspace on configuration and working directory level**.

6. Now we can access data.

* `dlt workspace show` will launch Workspace Dashboard
* `dlt workspace mcp` will launch Workspace MCP (Thierry's OSS MCP) in sse mode.
* `dlt pipeline foo mcp` will launch pipeline MCP (old Marcin's MCP) in sse mode.
* `dlt pipeline foo show` will launch Workspace Dashboard and open pipeline `foo`

## Profiles

New workspace adds concept of profiles that are used to:
1. secure access to data in different environments (ie. dev, tests and prod, access)
2. isolate pipelines from different workspaces and across profiles: pipeline may share code but they have
separate working directories and they store locally loaded data in separate locations.

After initialization, default **dev** profile is activated and from OSS user POV, everything works like they used to.

Profiles are to a large degree compatible with `project` profiles:
1. profile pinning works the same
2. configuring secrets and config toml for profiles works the same
3. `dlt profile` works +- the same

New Workspace is opinionated on several profiles
```sh
dlt profile list
Available profiles:
* dev - dev profile, workspace default
* prod - production profile, assumed by pipelines deployed in Runtime
* tests - profile assumed when running tests
* access - production profile, assumed by interactive notebooks in Runtime, typically with limited access rights
```

right now we plan to automatically assign profiles to Runtime jobs ie. batch jobs work on `prod` profile by default, interactive (notebooks) on `access` (read only profile.). But we'll see.

Now let's use profile to switch to production:

1. Add new named destination

First let's use another feature of new workspace: **named destinations**. We'll be able to easily switch and test pipelines without changing code. Our new destination has a name **warehouse**. Let's configure duckdb warehouse in `secrets.toml` (or `dev.secrets.toml` to fully split profiles).
```toml
[destination.warehouse]
destination_type="duckdb"
```
and change pipeline code (`destination="warehouse"`):
```py
pipeline = dlt.pipeline(
    pipeline_name="foo",
    destination="warehouse",
    dataset_name="lake_schema",
    dev_mode=True,
)
```
run the script again: you data got loaded to `_local/dev/warehouse.duckdb` now!

2. Add motherduck secrets to `prod` profile.

Now create `prod.secrets.toml` file:
```toml
[destination.warehouse]
destination_type="motherduck"
credentials="md:///dlt_data?motherduck_token=...."
```

and pin the **prod** profile to start testing in production 🤯
```sh
dlt profile prod pin
dlt profile
dlt workspace
```
Now you see that your new toml file will be read when pipeline runs.

Before we run pipeline script let's test connection to destination:
```sh
dlt --debug pipeline foo sync --destination warehouse --dataset-name lake_schema
```
(not ideal - we'll do a way better dry run soon). If your credentials are invalid or there's any other problem you'll get a detailed stack trace with an exception.

If connection is successful but there's no dataset on the Motherduck side you should get:
```sh
ERROR: Pipeline foo was not found in dataset lake_schema in warehouse
```

Now you can run pipeline script and observe your data getting into Motherduck. Now when you run Workspace Dashboard you'll see it connecting to remote dataset.


## Manage and configure workspace

You can cleanup workspace from all local files. This is intended to `dev` profile to easily start over:

```sh
dlt workspace clean
```

Workspace can be configured. You can change workspace name. **config.toml**:
```toml
[workspace.settings]
name="name_override"
```

You can also override local and working directories (not recommended). For example to have **dev** profile behaving exactly like OSS: **dev.config.toml**
```toml
[workspace.settings]
local_dir="."
working_dir="~/.dlt/"
```
Now `dlt pipeline -l` shows all OSS pipelines but `workspace clean` will refuse to work.

You can also configure dashboard and mcp (coming soon) on workspace and pipeline level:

```toml
[workspace.dashboard]
set="set"

[pipelines.foo.dashboard]
set="set"
```

Workspace also has runtime configuration that derives from OSS but will soon have dlthub Runtime settings:

```toml
[workspace.runtime]
log_level="DEBUG"
```