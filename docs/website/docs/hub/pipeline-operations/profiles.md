---
title: Profiles
description: Manage environment-specific configurations and secrets in dltHub Workspace
keywords: [dltHub, profiles, workspace, configuration, secrets, environments]
---

# Profiles

Profiles in `dlt` define **environment-specific configurations and secrets**.
They allow you to manage separate settings for development, testing, and production using the same codebase.

Each profile provides isolated configuration, credentials, and working directories for dlt pipelines, datasets, transformations, and notebooks. You don't need to write any additional code to benefit from profiles.

Profiles are defined and managed through [**TOML files**](../../general-usage/credentials) located in the `.dlt` directory.
They are compatible with the `secrets.toml` and `config.toml` files you may already know from OSS dlt.

The [dltHub platform](https://app.dlthub.com) automatically uses certain profiles to deploy and run pipelines and notebooks.


## Enable the workspace and profiles

Before you start, make sure you have followed the [installation instructions](../getting-started/installation.md) and enabled [additional Workspace features](../getting-started/installation.md#enable-workspace-mode) (which also include Profiles).

**dltHub Workspace** is a unified environment for developing, running, and maintaining data pipelines—from local development to production.

[More about dlt Workspace →](../getting-started/installation.md#what-is-a-dlthub-workspace)

[Scaffold](../ingestion/init.md) a workspace with `uvx dlthub-start@latest`, then add a pipeline to it from inside the workspace:

```sh
dlthub pipeline init pokemon_api duckdb
```

Once initialized, the workspace exposes the extended CLI surface, including profile-aware commands:

```sh
dlthub profile
dlthub local
```

## Define profiles

Once your workspace is scaffolded, you'll have two familiar `toml` files in `.dlt`: `secrets.toml` and `config.toml`. They work exactly the same way as in OSS `dlt`. You can run your OSS dlt code without modifications.

**Anything you place in those files is visible to all profiles**. For example, if you place
`log_level="INFO"` in `config.toml`, it applies to all profiles. Only when you want certain settings to vary across profiles (for example, `INFO` level for development, `WARNING` for production) do you need to create profile-specific `toml` files.

**dltHub Workspace** predefines several profiles, and together with the **dltHub platform**, assigns them specific functions:

| Profile      | Description                                                                                                                          |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| **`dev`**    | Default profile for local development.                                                                                               |
| **`prod`**   | Production profile, [used by the dltHub platform to run pipelines](./workspace-setup.md#understanding-workspace-profiles).         |
| **`tests`**  | Profile for automated test runs and CI/CD.                                                                                           |
| **`access`** | Read-only production profile [for interactive notebooks on the dltHub platform](./workspace-setup.md#understanding-workspace-profiles). |

:::note
The `dev` profile is active by default when you create a workspace. The others become active when pinned or automatically selected by the dltHub platform.
:::

View available profiles:

```sh
dlthub profile list
```


## Switching profiles

To change environments locally, **pin the desired profile**.
This makes it the default for subsequent `dlthub local …` commands:

```sh
dlthub local profile use prod
```

You can verify your current profile:

```sh
dlthub profile info
```

To unpin:

```sh
rm .dlt/profile-name
```

:::tip
You can pin a profile with any name, not just those from the predefined list. This allows you to create as many profiles as you need.
You can also pin a profile that doesn't yet have profile-specific TOML files and add those files later.
```sh
dlthub -v local info
```
This command lists all expected file locations from which `dlt` reads profile settings.
:::

Once pinned, you can run your pipeline as usual through the local runner:

```sh
dlthub local pipeline run pokemon_api_pipeline
```

The workspace automatically uses the active profile's configuration, secrets, and data locations to run the pipeline.

:::tip
Profiles isolate not only configuration but also pipeline runs. Each profile has a separate working directory (`.dlt/state/<profile>/`) and
local data directory (`.dlt/data/<profile>/`). This makes it easy to:
1. Clean up your workspace and start over (`dlthub local clean`)
2. Switch to the `tests` profile when running `pytest` (for example, using a fixture) so you can develop on the `dev` profile interactively while running tests in parallel in isolation
:::

### Switching profiles in code

You can interact with the workspace run context, switch profiles, and inspect workspace configuration using code:

```py
import dlt

workspace = dlt.current.workspace()

workspace.switch_profile("test")
```

## Example: Switch destinations using profiles

Let's walk through a setup that switches between **local DuckDB** (`dev`) and **MotherDuck** (`prod`).

### Step 1. Configure the development profile

In `.dlt/dev.secrets.toml` (to fully separate profiles), define your local destination:

```toml
[destination.warehouse]
destination_type = "duckdb"
```

Then, in your pipeline script, use `destination="warehouse"`:

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
dlthub local pipeline run pokemon_api_pipeline
```

Data will be stored in `.dlt/data/dev/warehouse.duckdb`.
Pipeline state will be stored in `.dlt/state/dev/`.


### Step 2. Configure the production profile

Create `.dlt/prod.secrets.toml`:

```toml
[destination.warehouse]
destination_type = "motherduck"
credentials = "md:///dlt_data?motherduck_token=...."
```

Pin and activate the profile:

```sh
dlthub local profile use prod
```

#### Test the connection (optional)

Before running your pipeline in production, you can verify that the credentials and dataset configuration work correctly:

```sh
dlthub --debug local pipeline sync pokemon_api_pipeline --destination warehouse --dataset-name pokemon_api_data
```

:::warning
`sync` drops the local pipeline working directory and restores it from the destination. Only run this on a fresh local state for the `prod` profile (the case here, since you just switched profiles).
:::

This command connects to your destination, validates credentials, and bootstraps a local copy of pipeline state from the destination. If your credentials are invalid or the configuration is wrong, `dlt` will raise a detailed exception with a full stack trace—helping you debug before deployment.

If the connection succeeds but the dataset doesn't yet exist in **MotherDuck**, you'll see a message like:

```text
ERROR: Pipeline pokemon_api_pipeline was not found in dataset pokemon_api_data in warehouse
```

This simply means the target dataset hasn't been created yet—no action is required.
Now run your pipeline script to load data into MotherDuck:

#### Run the pipeline with the `prod` profile

```sh
dlthub local pipeline run pokemon_api_pipeline
```

Data will be stored in MotherDuck.
Pipeline state will be stored in `.dlt/state/prod/`.

Once the pipeline completes, open the **Workspace Dashboard** with:

```sh
dlthub local show
```

You'll see your pipeline connected to the remote MotherDuck dataset and ready for further exploration.

#### Schedule the pipeline to run on the dltHub platform

Now you're ready to deploy your Workspace to the dltHub platform and [schedule your pipeline to run](../getting-started/platform-tutorial.md#7-schedule-a-pipeline).
Note that the dltHub platform will automatically use the `prod` profile you just created.

## Inspecting and managing profiles

* **List profiles**

  ```sh
  dlthub profile list
  ```

* **Show the current profile**

  ```sh
  dlthub profile info
  ```

* **Clean the workspace (useful in dev)**

  ```sh
  dlthub local clean
  ```

## Best practices

* Use **`dev`** for local testing and experimentation.
* Use **`prod`** for production jobs and runtime environments.
* Keep secrets in separate `<profile>.secrets.toml` files—never in code.
* Use **named destinations** (like `warehouse`) to simplify switching.
* Commit `config.toml`, but exclude all `.secrets.toml` files.


## Next steps

* [Configure the workspace](../getting-started/installation.md#what-is-a-dlthub-workspace)
* [Deploy your pipeline](../getting-started/platform-tutorial.md#5-run-your-first-pipeline)
* [Monitor and debug pipelines](../../general-usage/pipeline#monitor-the-loading-progress)
