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

The managed dltHub Platform automatically uses certain profiles to deploy and run pipelines and notebooks.


## Enable the workspace and profiles

Before you start, make sure you have followed the [installation instructions](../getting-started/installation.md) and initialized a workspace (`dlthub init`). The `.dlt/.workspace` marker file activates Workspace mode, which includes profile support.

**dltHub Workspace** is a unified environment for developing, running, and maintaining data pipelines—from local development to production.

[More about dlt Workspace →](../workspace/overview.md)

[Initialize](../workspace/init.md) a workspace and add a pipeline to it:

```sh
dlthub init
dlthub pipeline init pokemon_api duckdb
```

Once initialized, the workspace exposes the extended CLI surface, including profile-aware commands:

```sh
dlthub profile
dlthub local
```

## Define profiles

After `dlthub init`, you'll have two familiar `toml` files in `.dlt`: `secrets.toml` and `config.toml`. They work exactly the same way as in OSS `dlt`. You can run your OSS dlt code without modifications.

**Anything you place in those files is visible to all profiles**. For example, if you place
`log_level="INFO"` in `config.toml`, it applies to all profiles. Only when you want certain settings to vary across profiles (e.g., `INFO` level for development, `WARNING` for production) do you need to create profile-specific `toml` files.

**dltHub Workspace** predefines several profiles. `dev` and `tests` are local-only and never uploaded; `prod`, `access`, and any custom profile referenced in a job decorator are synchronized to the cloud configuration on every `dlthub deploy`.

| Profile      | Scope            | Description                                                                                                                   |
| ------------ | ---------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| **`dev`**    | Local only       | Default profile for local development.                                                                                        |
| **`tests`**  | Local only       | Profile for automated test runs and CI/CD.                                                                                    |
| **`prod`**   | Synced to cloud  | Production profile, [used by the cloud to run batch pipelines](../runtime/overview.md#workspace-profiles).                    |
| **`access`** | Synced to cloud  | Read-only production profile [for interactive notebooks served on the cloud](../runtime/overview.md#workspace-profiles).      |

:::note
The `dev` profile is active by default when you create a workspace. The others become active when pinned locally or automatically selected by the cloud (`prod` for batch jobs, `access` for interactive ones).
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
2. Switch to the `tests` profile when running `pytest` (e.g., using a fixture) so you can develop on the `dev` profile interactively while running tests in parallel in isolation
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

Before running your pipeline against MotherDuck, you can verify that the credentials and dataset configuration work correctly:

```sh
dlthub --debug local pipeline pokemon_api_pipeline sync --destination warehouse --dataset-name pokemon_api_data
```

This command performs a **dry run**, checking the connection to your destination and validating credentials without loading any data.
If your credentials are invalid or there's another configuration issue, `dlt` will raise a detailed exception with a full stack trace—helping you debug before deployment.

If the connection succeeds but the dataset doesn't yet exist in **MotherDuck**, you'll see a message like:

```text
ERROR: Pipeline pokemon_api_pipeline was not found in dataset pokemon_api_data in warehouse
```

This simply means the target dataset hasn't been created yet — no action is required.
Now run your pipeline to load data into MotherDuck:

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

#### Deploy the pipeline to dltHub

Now you're ready to deploy your workspace to dltHub and [run your pipeline in the cloud](../getting-started/runtime-tutorial.md#5-run-your-first-pipeline).
Note that the cloud will automatically use the `prod` profile you just created.

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
* Use **`prod`** for production jobs and the managed dltHub Platform.
* Keep secrets in separate `<profile>.secrets.toml` files—never in code.
* Use **named destinations** (like `warehouse`) to simplify switching.
* Commit `config.toml`, but exclude all `.secrets.toml` files.


## Next steps

* [Configure the workspace](../workspace/overview.md)
* [Deploy your pipeline](../getting-started/runtime-tutorial.md#5-run-your-first-pipeline)
* [Monitor and debug pipelines](../../general-usage/pipeline#monitor-the-loading-progress)
