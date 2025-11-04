---
title: Profiles
description: Manage environment-specific configurations and secrets in dltHub Workspace
keywords: [dltHub, profiles, workspace, configuration, secrets, environments]
---

# Profiles

Profiles in `dlt` define **environment-specific configurations and secrets**.
They allow you to manage separate settings for development, testing, and production using the same codebase.

Each profile provides isolated configuration, credentials, and working directories, ensuring your pipelines are secure and environment-aware.

## Overview

A **profile** is a named configuration context that controls how and where your pipelines run.
Profiles are defined and managed through [**TOML files**](../../general-usage/credentials) located in the `.dlt` directory.

Profiles let you:

* Securely manage credentials for multiple environments.
* Isolate pipeline state, configuration, and local data storage.
* Switch between environments without changing code.

## Enable the workspace and profiles

Before you start, make sure that you followed [installation instructions](../getting-started/installation.md) and enabled [additional Workspace features](../getting-started/installation.md#enable-dlthub-free-tier-features) (which also include Profiles)

**dltHub Workspace** is a unified environment for developing, running, and maintaining data pipelines — from local development to production.

[More about dlt Workspace ->](../workspace/overview.md)

[Initialize](../workspace/init) a project:

```sh
dlt init dlthub:pokemon_api duckdb
```

Once initialized, the Workspace automatically activates **profile support** and adds new commands such as:

```sh
dlt profile
dlt workspace
```

## Default profiles

When you initialize a project with `dlt init`, it creates a complete project structure — including configuration and secrets directories (`.dlt/`), a sample pipeline script, and a default `dev` profile.
This setup lets you start developing and running pipelines immediately, with environment-specific configurations ready to extend or customize.

The **dltHub Workspace** adds predefined profiles that isolate environments and simplify transitions between them:

| Profile | Description                                                                                                 |
|---------|-------------------------------------------------------------------------------------------------------------|
| **`dev`** | Default profile for local development. Pipelines store data in `_local/dev/` and state in `.dlt/.var/dev/`. |
| **`prod`** | Production profile, used by pipelines deployed in Runtime.                                                  |
| **`tests`** | Profile for automated test runs and CI/CD.                                                                  |
| **`access`** | Read-only production profile for interactive notebooks in Runtime.                                          |

:::note
Only the `dev` profile is active by default when you create a workspace.
The others become active when pinned or automatically selected by Runtime.
:::

View available profiles:

```sh
dlt profile list
```

Output:

```text
Available profiles:
* dev - dev profile, workspace default
* prod - production profile, assumed by pipelines deployed in Runtime
* tests - profile assumed when running tests
* access - production profile, assumed by interactive notebooks
```

## Switching profiles

To change environments, **pin the desired profile**.
This makes it the default for all commands and runs:

```sh
dlt profile prod pin
```

You can verify your current profile:

```sh
dlt profile
```

To unpin:

```sh
rm .dlt/profile-name
```

Once pinned, you can simply run your pipeline as usual:

```sh
python pokemon_api_pipeline.py
```

The workspace automatically uses the active profile’s configuration and secrets.

## Example: Switching between environments

Let's walk through a setup that switches between **local DuckDB** (`dev`) and **MotherDuck** (`prod`).

### Step 1. Configure the development profile

In `.dlt/dev.secrets.toml` (to fully split profiles), define your local destination:

```toml
[destination.warehouse]
destination_type = "duckdb"
```

Then, in your pipeline script, change the code `(destination="warehouse")`:

```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name='pokemon_api_pipeline',
    destination='warehouse',
    dataset_name='pokemon_api_data',
)
```

Run it locally:

```sh
python pokemon_api_pipeline.py
```

Data will be stored in `_local/dev/warehouse.duckdb`.


### Step 2. Configure the production profile

Create `.dlt/prod.secrets.toml`:

```toml
[destination.warehouse]
destination_type = "motherduck"
credentials = "md:///dlt_data?motherduck_token=...."
```

Pin and activate the profile:

```sh
dlt profile prod pin
```

#### Test the connection (optional)

Before running your pipeline in production, you can verify that the credentials and dataset configuration work correctly:

```sh
dlt --debug pipeline pokemon_api_pipeline sync --destination warehouse --dataset-name pokemon_api_data
```

This command performs a **dry run**, checking the connection to your destination and validating credentials without loading any data.
If your credentials are invalid or there’s another configuration issue, `dlt` will raise a detailed exception with a full stack trace — helping you debug before deployment.

If the connection succeeds but the dataset doesn’t yet exist in **MotherDuck**, you’ll see a message like:

```text
ERROR: Pipeline pokemon_api_pipeline was not found in dataset pokemon_api_data in warehouse
```

This simply means the target dataset hasn’t been created yet — no action is required.
Now, run your pipeline script to load data into MotherDuck:

```sh
python pokemon_api_pipeline.py
```

Once the pipeline completes, open the **Workspace Dashboard** with:

```sh
dlt workspace show
```

You’ll see your pipeline connected to the remote MotherDuck dataset and ready for further exploration.


## Inspecting and managing profiles

* **List profiles**

  ```sh
  dlt profile list
  ```

* **Show the current profile**

  ```sh
  dlt profile
  ```

* **Clean workspace (useful in dev)**

  ```sh
  dlt workspace clean
  ```

## Best practices

* Use **`dev`** for local testing and experimentation.
* Use **`prod`** for production jobs and runtime environments.
* Keep secrets in separate `<profile>.secrets.toml` files — never in code.
* Use **named destinations** (like `warehouse`) to simplify switching.
* Commit `config.toml`, but exclude all `.secrets.toml` files.


## Next steps

* [Configure the workspace.](../workspace/overview.md)
* [Deploy your pipeline.](../../walkthroughs/deploy-a-pipeline)
* [Monitor and debug pipelines.](../../general-usage/pipeline#monitor-the-loading-progress)
