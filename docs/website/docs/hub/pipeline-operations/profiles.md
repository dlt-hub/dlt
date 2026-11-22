---
title: Profiles and environments
description: Profiles control access to your data with write, read and none data access levels that dltHub assigns to pipelines and jobs. Environments deploy the same workspace to separate remote workspaces.
keywords: [dltHub, profiles, environments, workspace, configuration, secrets, data access, staging, deployment]
---
# Profiles and environments

A **profile** controls access to your data. It holds the credentials that a pipeline or job runs with and declares what those credentials can do with the data: **write** it, only **read** it, or not touch it at all (**none**).

Every job, in turn, declares the data access that it needs. When you deploy, dltHub matches the two: each pipeline and job runs with the profile that gives the data access it declares, without any code on your side. In a new workspace, pipelines load with the `prod` profile and notebooks read with the `access` profile once it has settings files. Until then, notebooks also run with `prod`.

:::caution
You bring your own destination, so set up the rights of each credential there. dlt trusts the declaration and never checks what a credential can do. For example, give `access` a read-only database role, or a storage key without write permission.
:::

Profiles also carry settings, so the same code loads into a local DuckDB file with `dev` and into your warehouse with `prod`, unchanged.

An **environment** is what you deploy the workspace to. In each environment, the workspace deploys to a remote workspace of its own, with its own set of profiles. The built-in `prod` environment is meant for production. With a `staging` environment, you can review your changes on a small slice of data before they reach production.

In short, a profile answers *what can this code do with your data?* and an environment answers *where does this workspace run?* An environment groups several profiles, so the jobs of one deployment can run with different data access.

## How profiles work

A new workspace declares five profiles:

| Profile     | Location | Data access | Used by                                           |
| ----------- | -------- | ----------- | ------------------------------------------------- |
| `dev`       | local    | write       | your machine, the default profile                 |
| `tests`     | local    | write       | automated tests, with state and data of their own |
| `prod`      | remote   | write       | batch jobs on the dltHub platform                 |
| `access`    | remote   | read        | interactive jobs on the dltHub platform           |
| `no-access` | remote   | none        | agent jobs that declare no data access            |

### Data access

Each profile declares its **data access**: `write`, `read` or `none`. Write includes read. A local profile always has write data access. A deployment reads the data access to give each job a profile, see [How a job gets its profile](#how-a-job-gets-its-profile).

### Settings files

The `.dlt` folder holds `config.toml` and `secrets.toml`, the files you know from OSS `dlt`. Everything in them applies to every profile. A profile adds `<profile>.config.toml` and `<profile>.secrets.toml` on top. A value in the settings file of a profile wins over the same value in the shared file. So the destination credentials of a profile go in its secrets file, and the settings that are the same everywhere go in `config.toml`.

```tree
.dlt/
├── .workspace           # declares the profiles and environments
├── config.toml          # settings of every profile
├── secrets.toml         # credentials of every profile (gitignored)
├── dev.config.toml      # settings of dev
├── prod.config.toml     # settings of prod
├── prod.secrets.toml    # credentials of prod (gitignored)
└── access.secrets.toml  # credentials of access (gitignored)
```

Each profile has a state folder, `.dlt/state/<profile>/`, and a data folder, `.dlt/data/<profile>/`. Pipeline working directories and local DuckDB files live there. Two profiles never share state. `dlthub local clean` removes the folders of the active profile.

### Local and remote profiles

A **local profile** never leaves your machine. A deployment never reads or includes its settings files. `dev` and `tests` are local profiles. A **remote profile** belongs to an environment and deploys with it: its settings files go to the dltHub platform with the deployment. `prod`, `access` and `no-access` are the remote profiles of the `prod` environment.

### Select a profile

A command that works on your machine, such as `dlthub local run`, runs with one profile, the **active profile**. dlt picks it in this order:

1. The profile named with `--profile` after the command name, as in `dlthub local show --profile tests`. Commands that never read a profile, such as `dlthub init`, `dlthub profile` and `dlthub ai toolkit`, refuse it.
2. The profile in the `WORKSPACE__PROFILE` environment variable. Runners set it for deployed jobs.
3. The profile that `dlthub local profile use <name>` recorded in `.dlt/profile-name`.
4. The default profile, `dev`.

`dlthub local profile use prod` makes `prod` the active profile for every local command that follows. To return to the default profile, remove `.dlt/profile-name`. `--profile` and `WORKSPACE__PROFILE` override the recorded profile for one command or process and do not change it.

To get more local profiles, or more profiles in an environment, add them with `dlthub profile add`, see [Declare profiles and environments](#declare-profiles-and-environments).

`dlthub profile info` shows the active profile with its location, data access, environment and folders. `dlthub -v local info` lists every file that dlt reads settings from.

In code, the workspace run context holds the active profile:

```py
import dlt

workspace = dlt.current.workspace()
print(workspace.profile)
```

## How environments work

:::note
Environments are available from dlt 1.33.0.
:::

A deployment is made to one environment and runs in the remote workspace of that environment. An environment declares:

- its **profiles**, which deploy with it. `prod`, `access` and `no-access` deploy with the `prod` environment.
- one **default profile** for each data access that its profiles have. A job that declares no profile gets the default profile of the data access that it declares. In `prod`, the default is `prod` for write, `access` for read and `no-access` for none.
- whether it is **strict** about access.
- whether it is the **default environment**.

Every environment has a default profile with write data access, its **default write profile**. `dlthub deploy` runs with it. The first profile of a data access that you add to an environment becomes the default for that data access.

Only the settings files of the profiles of an environment deploy with it, together with the shared `config.toml`, `secrets.toml` and `.workspace`. The credentials of `prod` never reach `staging`. The workspace dashboard is deployed with the deployment module of the default environment only, see [Choose the deployment module of an environment](#choose-the-deployment-module-of-an-environment).

### How a job gets its profile

Every job declares the **access** that it needs to run. The `data` axis of the declaration is the data access of the job: write, read or none. The dltHub platform grants a job no more than it declares. The profile gives the credentials for it.

Declare access with `access=` on the job:

```py notype
from dlt.hub import run

# a batch job that only reads
@run.job(access={"data": ["read"]})
def check_freshness() -> None:
    ...
```

A job without `access` declares the data access of its job type:

| Job                                                  | Data access it declares                                     |
| ---------------------------------------------------- | ----------------------------------------------------------- |
| batch and stream jobs: `@run.pipeline`, `@run.job`   | write                                                       |
| interactive jobs: notebooks, dashboards, MCP servers | read                                                        |
| agent jobs without `access`                          | none                                                        |
| any job with `access`                                | what `data` says: write or read. Without `data` it is none. |

A deployment gives every job the default profile of the data access it declares. A job that declares a profile with `require={"profile": ...}` gets that profile. If the job declares no `data` axis, the deployment gives it the data access of that profile.

A **strict** environment accepts only a profile that gives exactly the data access a job declares and has settings files, `<profile>.config.toml` or `<profile>.secrets.toml`. Otherwise `dlthub deploy` fails and its message names the fix. A profile without data access holds no credentials and needs no settings files. Every environment that `dlthub profile add-environment` creates is strict.

The `prod` environment of a new workspace is not strict. There, a job that declares read data access runs with `prod` as long as `access` has no settings files, and `dlthub deploy` warns about it. This keeps existing workspaces working and lets a small team start with one set of credentials. When you add `.dlt/access.secrets.toml` with read-only credentials, the next deployment gives these jobs `access`. A future release will deprecate this leniency.

On the dltHub platform, a job run makes the profile of the job the active profile before it imports the job module. `dlthub local run` runs with the active profile, or with the one named with `--profile`. A pipeline created at module level therefore uses the active profile of the run. Other jobs and notebooks can import the pipeline to read its dataset, see [Access a dataset from another script, job or notebook](../../general-usage/dataset-access/dataset.md#access-a-dataset-from-another-script-job-or-notebook).

### Select an environment

A command that deploys or works with a deployment, its jobs, job runs, logs and traces, takes `--environment` after the command name. Local commands such as `dlthub local` and `dlthub profile` refuse it. dlt picks the environment in this order:

1. The environment named with `--environment`. The commands that also take `--profile`, such as `dlthub variable`, also take `--workspace`. `--workspace` works as `--environment` without a name: dlt picks the environment in the order below.
2. The environment of the active profile, when that profile is a remote one.
3. The default environment, `prod` in a new workspace.

The command then runs with the default write profile of that environment. `dlthub deploy --environment staging` generates the manifest and deploys with `staging-write`. `dlthub local profile use staging-read` makes `staging` the environment of these commands, until another profile is active. A plain `dlthub deploy` then deploys to `staging`, not to `prod`.

These commands take no `--profile`, except the ones that work with one profile of the environment, such as `dlthub variable`, and the MCP tools that restore a pipeline, which take a `profile` argument. They run with the profile named with `--profile`, which must belong to the environment. Without `--environment`, dlt uses the environment of the profile. Without `--profile`, they run with the active profile if it belongs to the environment, else with the default write profile. `--workspace` names no environment, so it cannot go with `--profile`. `dlthub variable set` and `dlthub variable delete` need one of them: `--profile` for the variables of one profile, `--environment` or `--workspace` for the variables of the whole environment.

`dlthub <command> --help` shows the options that a command takes. A command group shows the options that all of its commands take.

| Commands                                                                                                                                     | Options                                     |
| -------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| `dlthub local`, `dlthub ai secrets`, `dlthub ai mcp`                                                                                         | `--profile`                                 |
| `dlthub deploy`, `dlthub run`, `dlthub show`, `dlthub job`, `dlthub workspace`                                                               | `--environment`                             |
| `dlthub variable`                                                                                                                            | `--environment`, `--workspace`, `--profile` |
| `dlthub init`, `dlthub profile`, `dlthub local profile`, `dlthub local schema`, `dlthub ai toolkit`, `dlthub ai mcp install`, `dlthub login` | none                                        |

The MCP tools take the same options as arguments. The tools that read data or secrets, such as `execute_sql_query` and `list_tables`, take `profile`: each call runs with the profile that it names, else with the active profile.

In code, the environment of the active profile is on the workspace run context. It is `None` for a local profile:

```py
import dlt

if dlt.current.workspace().environment == "staging":
    print("deployed to staging")
```

A deployed job reads the environment that the deployment was made to. The runner sets `WORKSPACE__PROFILE` to the profile that the deployment gave the job.

## Declare profiles and environments

dlt stores the declarations in `.dlt/.workspace`, a file that it writes for you. A new workspace declares the five profiles in [How profiles work](#how-profiles-work), with `prod`, `access` and `no-access` in the `prod` environment.

:::note Workspaces from before dlt 1.33.0
dlt declares the profiles of such a workspace the first time it loads it. Each profile that has settings files or is recorded in `.dlt/profile-name`, other than the built-in ones, gets an environment of its own. That environment is named after the profile, and deployments to it use this profile alone. This is the only time that dlt declares a profile for you. A profile that only `--profile` or `WORKSPACE__PROFILE` names, as in CI, is not declared: declare it with `dlthub profile add` before you upgrade.

A job that declares such a profile no longer deploys to `prod`, because the profile belongs to another environment. To use the profile in `prod`, edit `.dlt/.workspace` and move its declaration to the `prod` environment. dlt validates the file and names what else to fix. To keep the profile in its own environment, deploy with `--environment <profile>`.

An agent job that declares no data access and no profile now gets `no-access`, where it got `access` before. If such a job reads settings from `access.secrets.toml`, move them to the shared `secrets.toml` or declare `require={"profile": "access"}` on the job.
:::

Add profiles and environments with the CLI. Both commands show what they add to `.dlt/.workspace` and which files they create, then ask before they change anything:

```cli
# the staging environment, deployed with staging-write
dlthub profile add-environment staging
# a profile with read data access in staging
dlthub profile add staging-read --access read --environment staging
# a profile with read data access in the default environment
dlthub profile add sensitive-access --access read
# a profile without data access in staging
dlthub profile add staging-none --access none --environment staging
# a local profile
dlthub profile add lab --local
```

`profile add-environment <name>` declares a strict environment with one profile, `<name>-write`, its default write profile. Then it prints the command that adds the profile with read data access that interactive jobs need. `profile add` declares one profile in the default environment, or in the environment named with `--environment`, with write data access unless `--access` says otherwise. The first profile of a data access in an environment becomes the default for that data access. With `--local`, it declares a local profile.

Both commands create `<profile>.config.toml` and `<profile>.secrets.toml` for each new profile with data access. A profile without data access takes the shared `config.toml` and `secrets.toml`. The commands keep existing files. For a declared profile or environment, the command creates the settings files that are missing. When none is missing, it shows the declaration and fails.

For anything else, edit `.dlt/.workspace`, for example to make another profile the default of its data access. dlt validates the file on every command and explains what to fix.

## Connect to a remote workspace in each environment

In each environment, the local workspace is connected to a remote workspace of its own and deploys there. So `prod` and `staging` never share deployments, jobs or job runs. All remote workspaces of a local workspace belong to one organization.

dlt keeps the connections in the shared `config.toml`, one section for each environment:

```toml
# .dlt/config.toml
[runtime]
organization_id = "..."           # one organization for the whole workspace

[workspace.environment.prod]
workspace_id = "..."

[workspace.environment.staging]
workspace_id = "..."
```

dlt writes these sections for you. `dlthub workspace connect` connects the local workspace in the environment of the active profile, else in the default environment. Name another one with `--environment`. The first deployment to another environment connects the local workspace in that environment.

The remote workspace is named after the local workspace and the environment: the workspace `ingest` deploys to `ingest-staging` in `staging`. The default environment keeps the plain workspace name. To use another name, set `name` in the section of the environment, as `name` under `[workspace.settings]` names the local workspace.

CI sets the same values with environment variables, `WORKSPACE__ENVIRONMENT__STAGING__WORKSPACE_ID` for example.

`dlthub profile list` shows the remote workspace of each environment. `dlthub profile info` shows the one of the active profile.

:::note Workspaces connected before dlt 1.33.0
Such a workspace keeps the id of its remote workspace as `workspace_id` under `[runtime]` in `config.toml`. dlt still reads it as the remote workspace of the default environment, and nothing needs to change. `dlthub workspace connect` moves it to `[workspace.environment.prod]`, and `dlthub workspace disconnect` removes it. If both are set, even to the same id, dlt stops with an error: remove `workspace_id` under `[runtime]`.

The same key in the settings file of a profile of another environment, `stg.config.toml` for example, connects the local workspace in that environment until its section has a `workspace_id`. In the settings file of a profile of the default environment, it counts as the one in `config.toml`. `RUNTIME__WORKSPACE_ID`, which the platform and CI set, always wins.
:::

## Choose the deployment module of an environment

dlt imports the deployment module to find the jobs of a deployment. The deployment module is `__deployment__.py` by default. An environment can name another module with `deployment_module` in its section:

```toml
# .dlt/config.toml
[workspace.environment.staging]
deployment_module = "__staging__.py"
```

`dlthub deploy --environment staging` then imports `__staging__.py`. Give a `.py` path relative to the workspace, or a module name. A local profile uses the deployment module of the default environment, for example in `dlthub local run`. `dlthub profile list` shows the deployment module of each environment. `dlthub local info` shows the deployment module that it reads.

The workspace dashboard is deployed with the deployment module of the default environment. A module that you name in `dlthub deploy` gets no dashboard, and neither does another environment. To deploy without the dashboard in the default environment too, set `deploy_dashboard = false` in its section.

## How to use profiles and environments

This section shows four use cases, from the simplest to the most advanced.

### Run on DuckDB locally and on a warehouse in production

Give the destination a name in your pipeline. Each profile then says what the name means. `dev` points `warehouse` at a local DuckDB file. `prod` points it at MotherDuck, or at [any cloud destination](../../dlt-ecosystem/destinations/index.md). The code does not change.

1. In `.dlt/dev.config.toml`, define the local destination:

```toml
[destination.warehouse]
destination_type = "duckdb"
```

2. In `.dlt/prod.secrets.toml`, define the production destination:

```toml
[destination.warehouse]
destination_type = "motherduck"
credentials = "md:///dlt_data?motherduck_token=...."
```

3. In the pipeline, use `destination="warehouse"`. A job reads its settings from the `[jobs.<module>]` section of the active profile, so the `limit` of the load can differ per profile:

```py notype
# pokemon_api_pipeline.py
import dlt
from dlt.hub import run
from dlt.hub.run import trigger

from pokemon_api import pokemon_source


@run.pipeline("pokemon_api_pipeline", trigger=trigger.every("1h"))
def load_pokemon(limit: int = dlt.config.value) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="pokemon_api_pipeline",
        destination="warehouse",
        dataset_name="pokemon_api_data",
    )
    pipeline.run(pokemon_source().add_limit(limit))
```

4. Set the limit of each profile. In `.dlt/dev.config.toml`:

```toml
[jobs.pokemon_api_pipeline]
limit = 10
```

In `.dlt/prod.config.toml`:

```toml
[jobs.pokemon_api_pipeline]
limit = 10000
```

5. Run the job on your machine. The data lands in `.dlt/data/dev/warehouse.duckdb` and the state in `.dlt/state/dev/`:

```cli
dlthub local run pokemon_api_pipeline.py
```

6. Deploy. The deployment is made with `prod`, so the job loads into MotherDuck on the dltHub platform:

```cli
dlthub deploy
```

To reach the production data from your machine, make `prod` the active profile. Then local commands run with the production credentials:

```cli
dlthub local profile use prod
dlthub local show
```

Before the first deployment, you can test the production credentials the same way. Restore the pipeline state from the destination:

```cli
dlthub --debug local pipeline sync pokemon_api_pipeline --destination warehouse --dataset-name pokemon_api_data
```

:::caution
Run `sync` only on a fresh state of the `prod` profile, as here. `sync` drops the local working directory of the pipeline and restores it from the destination.
:::

With wrong credentials, dlt raises an exception with the full stack trace. When the dataset does not exist yet, dlt reports `Pipeline pokemon_api_pipeline was not found in dataset pokemon_api_data in warehouse`. This message is expected before the first load. When you are done, make `dev` the active profile again:

```cli
dlthub local profile use dev
```

### Run tests on the `tests` profile

`tests` is a local profile with state and data folders of its own. Tests that run with it never touch the pipelines that you work on with `dev`. A test run and your notebook can run at the same time.

1. Define `warehouse` for `tests` in `.dlt/tests.config.toml`, as for `dev`:

```toml
[destination.warehouse]
destination_type = "duckdb"
```

2. When the test session starts, make `tests` the active profile in `conftest.py`:

```py
import dlt
import pytest


def pytest_sessionstart(session: pytest.Session) -> None:
    dlt.current.workspace().switch_profile("tests")
```

pytest calls the hook before it imports the test modules. So the jobs that the tests import, and the pipelines that they create at the module level, run with `tests`. A fixture runs only after the imports.

Or override the profile for one `pytest` run from the shell:

```sh
WORKSPACE__PROFILE=tests uv run pytest
```

3. If a test fails, inspect the pipeline that it ran with `--profile tests`:

```cli
dlthub local pipeline trace pokemon_api_pipeline --profile tests
dlthub local pipeline failed-jobs pokemon_api_pipeline --profile tests
dlthub local show --profile tests
```

`dlthub local clean --profile tests` removes the data and state of the test runs.

### Limit the data access of jobs

A profile with read data access holds credentials that can only read. On the dltHub platform, a job that declares read data access gets the default profile with read data access: `access` in the `prod` environment. Interactive jobs declare read data access without any code. Put the read-only credentials in `.dlt/access.secrets.toml`:

```toml
[destination.warehouse]
destination_type = "motherduck"
# a token of a user that can only read
credentials = "md:///dlt_data?motherduck_token=...."
```

Until `access` has settings files, these jobs run with `prod`, and `dlthub deploy` warns once and names them.

A batch job declares write data access unless it says otherwise. Declare what the job needs with `access=`, and the deployment gives it `access` and not `prod`:

```py notype
# freshness_checks.py
from dlt.hub import run
from dlt.hub.run import trigger


@run.job(access={"data": ["read"]}, trigger=trigger.every("1h"))
def check_freshness() -> None:
    ...
```

Some jobs need a profile of their own. An agent job that reads personal data to answer customer care tickets must not share `access` with every notebook. Add a profile with read data access to the environment. Then declare the profile on the job:

```cli
dlthub profile add sensitive-access --access read
```

```py notype
# __deployment__.py
from dlt.hub import run

care_inspector = run.agent(
    "agents/customer_care",
    trigger="job.fail:tag:care",
    require={"profile": "sensitive-access"},
)
```

`sensitive-access` deploys with the `prod` environment, together with its other profiles, and only this job runs with its credentials. The agent definition declares `data: read`, so the profile gives exactly the data access that the job declares. See [Profile of an agent job](../agents/index.md#profile-of-an-agent-job).

A profile without data access has no destination credentials and needs no settings files. The `prod` environment has one: `no-access`. Agent jobs that declare no data access, such as those that only read logs and job runs, get it, so no data credential reaches the model. Declare one in another environment with `--access none`.

### Create a staging environment with its own remote workspace

In a staging environment, the same local workspace deploys to a second remote workspace, with credentials and settings of its own. Reviewers see your changes run on small data before the changes reach `prod`. This use case builds on the `sensitive-access` profile from the use case above.

1. Declare the environment and its profiles:

```cli
dlthub profile add-environment staging
dlthub profile add staging-read --access read --environment staging
dlthub profile add staging-sensitive-access --access read --environment staging
```

`staging` is strict, and deployments to it use `staging-write`, its default write profile. `staging-read` is the first profile with read data access, so it is the default that interactive jobs get. Without it, a deployment with a notebook fails. `staging-sensitive-access` is not a default: only a job that declares it gets it.

2. Put the staging credentials in `.dlt/staging-write.secrets.toml`, `.dlt/staging-read.secrets.toml` and `.dlt/staging-sensitive-access.secrets.toml`. Put the staging limit in `.dlt/staging-write.config.toml`:

```toml
[jobs.pokemon_api_pipeline]
limit = 100
```

3. Vary the deployment by environment. The deployment module reads the environment, then decides which jobs to deploy and which profile a job declares:

```py notype
# __deployment__.py
import dlt
from dlt.hub import run

from pokemon_api_pipeline import load_pokemon
import pokemon_notebook

__all__ = ["load_pokemon", "pokemon_notebook", "care_inspector"]

environment = dlt.current.workspace().environment

# the nightly report runs in production only
if environment == "prod":
    from reports import nightly_report

    __all__.append("nightly_report")

# the profile with the personal data: `environment` is None on a local profile
SENSITIVE_PROFILE = {"staging": "staging-sensitive-access"}

care_inspector = run.agent(
    "agents/customer_care",
    trigger="job.fail:tag:care",
    require={"profile": SENSITIVE_PROFILE.get(environment, "sensitive-access")},
)
```

4. Deploy to staging:

```cli
dlthub deploy --environment staging
```

dlt generates the deployment manifest with `staging-write` as the active profile, so `environment` in the module is `staging`. `staging-write`, `staging-read` and `staging-sensitive-access` deploy with the `staging` environment. The first deployment connects the local workspace in `staging` to the remote workspace `ingest-staging` and writes `[workspace.environment.staging]` to `config.toml`. `dlthub deploy` without `--environment` deploys to `prod` as before.

5. Work with the staging deployment. Name the environment on each command, or make a profile of `staging` the active profile:

```cli
dlthub job list --environment staging
dlthub local profile use staging-read
dlthub job list
```

While `staging-read` is the active profile, a plain `dlthub deploy` deploys to `staging`. When you are done, make `dev` the active profile again with `dlthub local profile use dev`.

Credentials and limits live in the settings files of each profile. The deployment module only changes which jobs deploy and which profile a job declares. To keep the jobs of `staging` in a module of their own, make it the [deployment module of `staging`](#choose-the-deployment-module-of-an-environment). The [CI/CD blueprint](../../examples/dlthub_cicd.md) deploys to staging from a GitHub pull request with this layout.

## Commands

| Command                                 | What it does                                                                                                                                                  |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dlthub profile list`                   | Lists the declared profiles, with the remote workspace of each environment.                                                                                   |
| `dlthub profile info`                   | Shows the active profile: location, data access, environment and folders.                                                                                     |
| `dlthub profile add <name>`             | Declares a profile in an environment (`--environment`, `--access`) or a local profile (`--local`).                                                            |
| `dlthub profile add-environment <name>` | Declares a strict environment with `<name>-write` as its default write profile.                                                                               |
| `dlthub local profile use <name>`       | Makes a profile the active profile.                                                                                                                           |
| `dlthub local clean`                    | Removes the state and data folders of the active profile.                                                                                                     |
| `dlthub <command> --profile <name>`     | Overrides the recorded profile for one local command.                                                                                                         |
| `dlthub <command> --environment <name>` | Names the environment for a command that deploys or works with a deployment.                                                                                  |
| `dlthub variable --workspace`           | `--environment` without a name: the environment of the active profile, else the default one. Only commands that take `--profile` and `--environment` take it. |

## Next steps

* [Workspace setup](workspace-setup.md)
* [Deployments](deployments.md)
* [Profile of an agent job](../agents/index.md#profile-of-an-agent-job)
* [CI/CD blueprint](../../examples/dlthub_cicd.md)
* [Environment variables](environment-variables.md)
