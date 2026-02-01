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

dltHub Runtime automatically uses certain profiles to deploy and run pipelines and notebooks.


## Enable the workspace and profiles

Before you start, make sure you have followed the [installation instructions](../getting-started/installation.md) and enabled [additional Workspace features](../getting-started/installation.md#enable-dlthub-free-and-paid-features) (which also include Profiles).

**dltHub Workspace** is a unified environment for developing, running, and maintaining data pipelines—from local development to production.

[More about dlt Workspace →](../workspace/overview.md)

[Initialize](../workspace/init) a project:

```sh
dlt init dlthub:pokemon_api duckdb
```

Once initialized, the Workspace automatically activates **profile support** and adds new commands such as:

```sh
dlt profile
dlt workspace
```

## Define profiles

If you use `dlt init`, you'll have two familiar `toml` files in `.dlt`: `secrets.toml` and `config.toml`. They work exactly the same way as in OSS `dlt`. You can run your OSS dlt code without modifications.

**Anything you place in those files is visible to all profiles**. For example, if you place
`log_level="INFO"` in `config.toml`, it applies to all profiles. Only when you want certain settings to vary across profiles (e.g., `INFO` level for development, `WARNING` for production) do you need to create profile-specific `toml` files.

**dltHub Workspace** predefines several profiles, and together with **dltHub Runtime**, assigns them specific functions:

| Profile      | Description                                                                                                                   |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| **`dev`**    | Default profile for local development.                                                                                        |
| **`prod`**   | Production profile, [used by Runtime to run pipelines](../runtime/overview.md#understanding-workspace-profiles).              |
| **`tests`**  | Profile for automated test runs and CI/CD.                                                                                    |
| **`access`** | Read-only production profile [for interactive notebooks in Runtime](../runtime/overview.md#understanding-workspace-profiles). |

:::note
The `dev` profile is active by default when you create a workspace. The others become active when pinned or automatically selected by Runtime.
:::

View available profiles:

```sh
dlt profile list
```


## Switching profiles

To change environments, **pin the desired profile**.
This makes it the default for all dlt commands:

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

:::tip
You can pin a profile with any name, not just those from the predefined list. This allows you to create as many profiles as you need.
You can also pin a profile that doesn't yet have profile-specific TOML files and add those files later.
```sh
dlt workspace -v info
```
This command lists all expected file locations from which `dlt` reads profile settings.
:::

Once pinned, you can simply run your pipeline as usual:

```sh
python pokemon_api_pipeline.py
```

The workspace automatically uses the active profile's configuration, secrets, and data locations to run the pipeline.

:::tip
Profiles isolate not only configuration but also pipeline runs. Each profile has a separate pipeline directory (`.dlt/var/$profile/pipelines`) and
storage location for locally stored data (e.g., local `filesystem`, `ducklake`, or `duckdb`). This makes it easy to:
1. Clean up your workspace and start over (`dlt workspace clean`)
2. Switch to the `test` profile when running `pytest` (e.g., using a fixture) so you can develop on the `dev` profile interactively while running tests in parallel in isolation
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
python pokemon_api_pipeline.py
```

Data will be stored in `_local/dev/warehouse.duckdb`.
Pipeline state will be stored in `.dlt/.var/dev/`.


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
If your credentials are invalid or there's another configuration issue, `dlt` will raise a detailed exception with a full stack trace—helping you debug before deployment.

If the connection succeeds but the dataset doesn't yet exist in **MotherDuck**, you'll see a message like:

```text
ERROR: Pipeline pokemon_api_pipeline was not found in dataset pokemon_api_data in warehouse
```

This simply means the target dataset hasn't been created yet—no action is required.
Now run your pipeline script to load data into MotherDuck:

#### Run the pipeline with the `prod` profile

```sh
python pokemon_api_pipeline.py
```

Data will be stored in MotherDuck.
Pipeline state will be stored in `.dlt/.var/prod/`.

Once the pipeline completes, open the **Workspace Dashboard** with:

```sh
dlt workspace show
```

You'll see your pipeline connected to the remote MotherDuck dataset and ready for further exploration.

#### Schedule the pipeline to run on Runtime

Now you're ready to deploy your Workspace to Runtime and [schedule your pipeline to run](../getting-started/runtime-tutorial.md#7-schedule-a-pipeline).
Note that Runtime will automatically use the `prod` profile you just created.

## Inspecting and managing profiles

* **List profiles**

  ```sh
  dlt profile list
  ```

* **Show the current profile**

  ```sh
  dlt profile
  ```

* **Clean the workspace (useful in dev)**

  ```sh
  dlt workspace clean
  ```

## Best practices

* Use **`dev`** for local testing and experimentation.
* Use **`prod`** for production jobs and runtime environments.
* Keep secrets in separate `<profile>.secrets.toml` files—never in code.
* Use **named destinations** (like `warehouse`) to simplify switching.
* Commit `config.toml`, but exclude all `.secrets.toml` files.


## Next steps

* [Configure the workspace](../workspace/overview.md)
* [Deploy your pipeline](../getting-started/runtime-tutorial.md#5-run-your-first-pipeline-on-runtime)
* [Monitor and debug pipelines](../../general-usage/pipeline#monitor-the-loading-progress)
