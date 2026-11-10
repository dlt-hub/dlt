---
title: Profiles and environments
description: Profiles control access to your data with write, read and none data access levels that dltHub assigns to pipelines and jobs. Environments deploy the same workspace to separate remote workspaces.
keywords: [dltHub, profiles, environments, workspace, configuration, secrets, data access, staging, deployment]
---
# Profiles and environments

A **profile** controls access to your data. It holds the credentials that a pipeline or job runs with, and it declares what those credentials can do with the data: write, read, or nothing.

dltHub builds on that declaration. Each profile has one of three data access levels: `write`, `read` or `none`. Each job declares the access that it needs to run. A deployment gives every pipeline and job the profile with the data access that the job declares, and you write no code for it. A pipeline writes with the `prod` profile. A notebook reads with the `access` profile.

:::warning
You are responsible for the credentials. You bring your own destination, and dlt never checks what a credential can do. Set up the rights of each credential at the destination. A profile with read data access gets a read-only database role, or a storage key without write permission.
:::

A profile also holds settings. This is its second function: the same code loads into a local DuckDB file with `dev` and into your warehouse with `prod`, without changes.

An **environment** is what a deployment is made to. Each environment deploys the same workspace to a remote workspace of its own, with profiles of its own. `prod` is for production, `staging` for a review of your changes on small data.

A profile answers one question: what can this code do with your data? An environment answers another: where is this workspace deployed? One environment has several profiles, so the jobs of one deployment run with different data access.

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

The `.dlt` folder holds `config.toml` and `secrets.toml`, the files you know from OSS `dlt`. Everything in them applies to every profile. A profile adds `<profile>.config.toml` and `<profile>.secrets.toml` on top. A value in a profile file wins over the same value in the shared file. Put the destination credentials of a profile in its secrets file, and the settings that are the same everywhere in `config.toml`.

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

A **local profile** runs only on your machine. A deployment never reads or includes its settings files. `dev` and `tests` are local profiles. A **remote profile** belongs to an environment and deploys with it: its settings files go to the dltHub platform with the deployment. `prod`, `access` and `no-access` are the remote profiles of the `prod` environment.

### Select a profile

A command that works on your machine, such as `dlthub local run`, runs with one profile, the **active profile**. dlt picks it in this order:

1. The profile named with `--profile`, before or after the command.
2. The profile in the `WORKSPACE__PROFILE` environment variable. Runners set it for deployed jobs.
3. The profile that `dlthub local profile use <name>` recorded in `.dlt/profile-name`.
4. The default profile, `dev`.

`dlthub local profile use prod` makes `prod` the active profile for every local command that follows. To go back to the default profile, remove `.dlt/profile-name`. `--profile` and `WORKSPACE__PROFILE` override the recorded profile for one command or process and do not change it.

Every profile must be declared. dlt stops when the workspace loads with an undeclared profile, from `--profile`, from `WORKSPACE__PROFILE` or from `.dlt/profile-name`. The message names the source and the fix: declare the profile with `dlthub profile add`, unset the variable, or remove `.dlt/profile-name`. An environment name is not a profile name.

`dlthub profile info` shows the active profile with its location, data access, environment and folders. `dlthub -v local info` lists every file that dlt reads settings from.

In code, the workspace run context knows the profile:

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

- its **profiles**. They deploy together. A deployment to `prod` ships `prod`, `access` and `no-access`.
- one **default profile** for each data access that its profiles have. Jobs that declare no profile get the default profiles. In `prod`, the default is `prod` for write, `access` for read and `no-access` for none.
- whether it is **strict** about access.
- whether it is the **default environment**.

Every environment has a default profile with write data access, its **default write profile**. `dlthub deploy` runs with it. The first profile of a data access that you add to an environment becomes the default for that data access.

A deployment ships only the settings files of the profiles of its environment, with the shared `config.toml` and `secrets.toml`. The credentials of `prod` never reach `staging`. The workspace dashboard is deployed to the default environment only.

### How a job gets its profile

Every job declares the **access** that it needs to run. The `data` part of the declaration is the data access of the job: write, read or none. The dltHub platform grants a job no more than it declares. The profile gives the credentials for it.

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

A deployment then gives every job one profile of the environment. A job without a profile gets the default profile of its data access. A job that declares `require={"profile": ...}` keeps that profile. A strict environment requires that the profile gives exactly the data access that the job declares:

| The job                                                                        | Strict environment    | Environment that is not strict                         |
| ------------------------------------------------------------------------------ | --------------------- | ------------------------------------------------------ |
| declares no profile, the default profile of its data access has settings files | gets that profile     | gets that profile                                      |
| declares no profile, that default profile is missing or has no settings files  | `dlthub deploy` fails | gets the closest default profile with more data access |
| declares a profile with exactly its data access                                | keeps it              | keeps it                                               |
| declares a profile with more data access                                       | `dlthub deploy` fails | keeps it                                               |
| declares a profile with less data access                                       | `dlthub deploy` fails | keeps it, `dlthub deploy` warns                        |
| declares a profile without settings files                                      | `dlthub deploy` fails | keeps it, `dlthub deploy` warns                        |
| declares a profile that is not one of the environment                          | `dlthub deploy` fails | `dlthub deploy` fails                                  |

A declared profile is not enough: the profile must have settings files, `<profile>.config.toml` or `<profile>.secrets.toml`. A profile without data access is the exception. It needs no credentials, so it needs no settings files.

In an environment that is not strict, a job falls back when its default profile is missing or has no settings files. It gets the closest default profile with more data access that has settings files. `none` falls to `read` and then to `write`, `read` falls to `write`. The default write profile is the last one and is always taken, with or without settings files. `dlthub deploy` warns about every job that gets a profile with more data access than it declares. The access that the job declares does not change.

This is how a new workspace runs with one set of credentials. `prod` is not strict, and `access` has no settings files until you create them. A notebook then gets `prod` and `dlthub deploy` warns about it: the notebook declares read data access and holds credentials that can write. Add `.dlt/access.secrets.toml` with read-only credentials, and the next deployment gives the notebook `access`.

`prod` is not strict. Every environment that `dlthub profile add-environment` declares is strict. When a job in a strict environment gets no profile, `dlthub deploy` fails. The message names the fix: add the profile or its settings files with `dlthub profile add`, declare the access that the job needs with `access=`, or set `strict_access: false` on the environment in `.dlt/.workspace`.

### Select an environment

A command that deploys or works with a deployment, its jobs, runs, logs and traces, takes `--environment` and no `--profile`. dlt picks the environment in this order:

1. The environment named with `--environment`.
2. The environment of the active profile, when that profile is a remote one.
3. The default environment, `prod` in a new workspace.

The command then runs with the default write profile of that environment. `dlthub deploy --environment staging` generates the manifest and deploys with `staging-write`. `dlthub local profile use staging-read` makes `staging` the environment of these commands, until another profile is active. A plain `dlthub deploy` then deploys to `staging`, not to `prod`.

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
dlt declares the profiles of such a workspace the first time it loads it. Each profile that has settings files or is recorded in `.dlt/profile-name` becomes an environment of its own, deployed with that profile alone. This is the only time that dlt declares a profile for you.

A job that requires such a profile no longer deploys to `prod`, because the profile belongs to another environment. To use the profile in `prod`, edit `.dlt/.workspace`: move its declaration to the `prod` environment, remove `default: true` from it, and remove the environment that is left without profiles. To keep it apart, deploy with `--environment <profile>`.

An agent job that declares no data access and no profile now gets `no-access`, where it got `access` before. If such a job reads settings from `access.secrets.toml`, move them to the shared `secrets.toml` or declare `require={"profile": "access"}` on the job.
:::

Add profiles and environments with the CLI. Both commands show what they add to `.dlt/.workspace` and which files they create, then ask before they change anything:

```cli
# the staging environment, deployed with staging-write
dlthub profile add-environment staging
# a profile with read data access in staging
dlthub profile add staging-read --access read --env staging
# a profile with read data access in the default environment
dlthub profile add sensitive-access --access read
# a profile without data access in staging
dlthub profile add staging-none --access none --env staging
# a local profile
dlthub profile add lab --local
```

`profile add-environment <name>` declares a strict environment with one profile, `<name>-write`, its default write profile. Then it prints the command that adds the profile with read data access that interactive jobs need. `profile add` declares one profile in the default environment, or in the environment named with `--env`, with write data access unless `--access` says otherwise. The first profile of a data access in an environment becomes the default for that data access. With `--local`, it declares a local profile.

Both commands create `<profile>.config.toml` and `<profile>.secrets.toml` for each new profile with data access. A profile without data access takes the shared `config.toml` and `secrets.toml`. Existing files are kept. For a declared profile or environment, the command creates the settings files that are missing. When none is missing, it shows the declaration and fails.

For anything else, edit `.dlt/.workspace`. To make another profile the default of its data access, move `default: true` to it. dlt validates the file on every command and explains what to fix.

## Connect to a remote workspace in each environment

Each environment deploys to a remote workspace of its own. The local workspace is connected to one remote workspace in each environment, so `prod` and `staging` never share deployments, jobs or runs. All remote workspaces of a local workspace belong to one organization.

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

dlt writes these sections for you. `dlthub workspace connect` connects the local workspace in the default environment. The first deployment to another environment connects the local workspace in that environment. The remote workspace is named after the local workspace and the environment: the workspace `ingest` deploys to `ingest-staging` in `staging`. The default environment keeps the plain workspace name. To use another name, set `workspace_name` in the section of the environment.

CI sets the same values with environment variables, `WORKSPACE__ENVIRONMENT__STAGING__WORKSPACE_ID` for example.

`dlthub profile list` shows the remote workspace of each environment. `dlthub profile info` shows the one of the active profile.

:::note Workspaces connected before dlt 1.33.0
Such a workspace keeps the id of its remote workspace as `workspace_id` under `[runtime]` in `config.toml`. dlt still reads it as the remote workspace of the default environment, and nothing needs to change. The same key in the settings file of a profile, `stg.config.toml` for example, connects the local workspace in the environment of that profile.
:::

## How to use profiles and environments

Four use cases, from the simplest to the most advanced.

### Run on DuckDB locally and on a warehouse in production

Name the destination in your pipeline and let each profile say what the name means. `dev` points `warehouse` at a local DuckDB file. `prod` points it at MotherDuck, or at [any cloud destination](../../dlt-ecosystem/destinations/index.md). The code does not change.

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

:::warning
`sync` drops the local working directory of the pipeline and restores it from the destination. Run it only on a fresh state of the `prod` profile, as here.
:::

With wrong credentials, dlt raises an exception with the full stack trace. When the dataset does not exist yet, dlt reports `Pipeline pokemon_api_pipeline was not found in dataset pokemon_api_data in warehouse`. This message is expected before the first load. Make `dev` the active profile again when you are done:

```cli
dlthub local profile use dev
```

### Run tests on the `tests` profile

`tests` is a local profile with state and data folders of its own. Tests that run on it never touch the pipelines that you work on with `dev`. A test run and your notebook can run at the same time.

1. Define `warehouse` for `tests` in `.dlt/tests.config.toml`, as for `dev`:

```toml
[destination.warehouse]
destination_type = "duckdb"
```

2. Make `tests` the active profile for every test with a fixture in `conftest.py`:

```py
import pytest
import dlt


@pytest.fixture(autouse=True, scope="session")
def tests_profile() -> None:
    dlt.current.workspace().switch_profile("tests")
```

Or override the profile for one `pytest` run from the shell:

```sh
WORKSPACE__PROFILE=tests uv run pytest
```

3. If a test fails, inspect the pipeline that it ran with `--profile tests`:

```cli
dlthub --profile tests local pipeline trace pokemon_api_pipeline
dlthub --profile tests local pipeline failed-jobs pokemon_api_pipeline
dlthub --profile tests local show
```

`dlthub --profile tests local clean` removes the data and state of the test runs.

### Limit the data access of jobs

A profile with read data access holds credentials that can only read. On the dltHub platform, a job that declares read data access gets the default profile with read data access: `access` in the `prod` environment. Interactive jobs declare read data access without any code. Put the read-only credentials in `.dlt/access.secrets.toml`:

```toml
[destination.warehouse]
destination_type = "motherduck"
# a token of a user that can only read
credentials = "md:///dlt_data?motherduck_token=...."
```

Until `access` has settings files, these jobs run with `prod` and `dlthub deploy` warns about each of them.

A batch job declares write data access unless it says otherwise. Declare what the job needs with `access=`, and the deployment gives it `access` and not `prod`:

```py notype
# freshness_checks.py
from dlt.hub import run
from dlt.hub.run import trigger


@run.job(access={"data": ["read"]}, trigger=trigger.every("1h"))
def check_freshness() -> None:
    ...
```

Some jobs need a profile of their own. An agent job that reads personal data to answer customer care tickets must not share `access` with every notebook. Declare a profile with read data access in the environment, and declare it on the job:

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

The deployment ships `sensitive-access` with the other profiles of `prod`, and only this job runs with its credentials. The agent definition declares `data: read`, so the profile gives exactly the data access that the job declares. See [Profile of an agent job](../agents/index.md#profile-of-an-agent-job).

A profile without data access has no destination credentials and needs no settings files. `prod` has one, `no-access`. Agent jobs that declare no data access, such as those that only read logs and run records, get it, so no data credential reaches the model. Declare one in another environment with `--access none`.

### Create a staging environment with its own remote workspace

A staging environment deploys the same local workspace to a second remote workspace, with credentials and settings of its own. Reviewers see your changes run on small data before the changes reach `prod`. This use case builds on the `sensitive-access` profile from the use case above.

1. Declare the environment and its profiles:

```cli
dlthub profile add-environment staging
dlthub profile add staging-read --access read --env staging
dlthub profile add staging-sensitive-access --access read --env staging
```

`staging` is strict and deployed with `staging-write`, its default write profile. `staging-read` is the first profile with read data access, so it is the default that interactive jobs get. Without it, a deployment with a notebook fails. `staging-sensitive-access` is not a default: only a job that declares it gets it.

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

The manifest is generated with `staging-write` as the active profile, so `environment` in the module is `staging`. The deployment ships `staging-write`, `staging-read` and `staging-sensitive-access`. The first deployment connects the local workspace in `staging` to the remote workspace `ingest-staging` and writes `[workspace.environment.staging]` to `config.toml`. `dlthub deploy` without `--environment` deploys to `prod` as before.

5. Work with the staging deployment. Name the environment on each command, or make a profile of `staging` the active profile:

```cli
dlthub job list --environment staging
dlthub local profile use staging-read
dlthub job list
```

While `staging-read` is the active profile, a plain `dlthub deploy` deploys to `staging`. Make `dev` the active profile again with `dlthub local profile use dev` when you are done.

Credentials and limits live in the settings files of each profile. The deployment module only changes which jobs deploy and which profile a job declares. The [CI/CD blueprint](../../examples/dlthub_cicd.md) deploys to staging from a GitHub pull request with this layout.

## Commands

| Command                                 | What it does                                                                               |
| --------------------------------------- | ------------------------------------------------------------------------------------------ |
| `dlthub profile list`                   | Lists the declared profiles, with the remote workspace of each environment.                |
| `dlthub profile info`                   | Shows the active profile: location, data access, environment and folders.                  |
| `dlthub profile add <name>`             | Declares a profile in an environment (`--env`, `--access`) or a local profile (`--local`). |
| `dlthub profile add-environment <name>` | Declares a strict environment with `<name>-write` as its default write profile.            |
| `dlthub local profile use <name>`       | Makes a profile the active profile.                                                        |
| `dlthub local clean`                    | Removes the state and data folders of the active profile.                                  |
| `dlthub <command> --profile <name>`     | Overrides the recorded profile for one local command.                                      |
| `dlthub <command> --environment <name>` | Names the environment for a command that deploys or works with a deployment.               |

## Next steps

* [Workspace setup](workspace-setup.md)
* [Deployments](deployments.md)
* [Profile of an agent job](../agents/index.md#profile-of-an-agent-job)
* [CI/CD blueprint](../../examples/dlthub_cicd.md)
* [Environment variables](environment-variables.md)
