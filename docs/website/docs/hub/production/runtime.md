---
title: Runtime
description: Deploy and run your Workspace remotely in dltHub Runtime
keywords: [dltHub, runtime, workspace, deployment, configuration, scripts, runs, production]
---

# Runtime

The dltHub Runtime lets you deploy your local Workspace, run scripts remotely, and inspect deployments, configurations, scripts, and runs — all from the CLI.

## Overview

Runtime operates on a few core entities and their lifecycle:

- **Workspace**: Your project context, a remote representation of your local [Workspace](https://dlthub.com/docs/hub/workspace/overview). In Runtime, a workspace combines a code artifact (**deployment**) with its **configuration** (configs and secrets).
- **Deployment**: A snapshot of your Workspace files (excluding `.dlt/` internals and ignored files). Deployment is updated only when the code changes.
- **Configuration**: A snapshot of `.dlt/*config.toml` and `.dlt/*secrets.toml`. Configuration is updated only when config/secrets content changes.
- **Script**: A named entry point (e.g. `pipeline.py`) stored in Runtime, keeps the information about schedule, profile and whether it's an interactive script such as a notebook (exposed via a URL) or a batch script (not exposed).
- **Run**: An execution of a script (manual or scheduled). Runs have IDs, status, logs, and metadata.

Typical workflow in Runtime:

1. You authenticate to Runtime and connect your local Workspace to the remote one.
2. You deploy your local Workspace to the remote one in one of two ways:
    - using a single convenience command `dlt workspace deploy`
    - using a finely controlled multi-command workflow via `dlt runtime ...` subcommands
3. You inspect run results (status, logs, etc.) and resources (deployments, configurations, scripts, runs).

## Enable the Workspace and Runtime

::::info
Runtime features are part of the Workspace experience and may be experimental.
You need to enable Workspace mode in your project. All commands are to be executed within the Workspace directory.
::::

Install `dlt` with Workspace support:

```sh
pip install "dlt[workspace]"
```

[Initialize](../workspace/init) a project, for example:

```sh
dlt init dlthub:pokemon_api duckdb
```

Enable Workspace mode by creating the feature flag file:

```sh
touch .dlt/.workspace
```

Once enabled, you can use the Runtime commands via:

```sh
dlt workspace ...
dlt runtime ...
```

## Authenticate: login and logout

- **Login**: authenticates via GitHub Device Flow, stores the auth token and connects your local workspace to the remote one.

  ```sh
  dlt workspace login
  ```

  During login you’ll see a verification URL and code to enter in the browser. If your local `.dlt/runtime.config.toml` has no `workspace_id` or it differs from the remote default workspace, you’ll be prompted to sync it.

- **Logout**: removes the stored auth token.

  ```sh
  dlt workspace logout
  ```

## Deploy and run (single command)

For the fastest path that “just works” in most cases, use a single command that uploads your code and configuration if they changed, creates/updates the script, and triggers a run:

```sh
dlt workspace deploy path/to/your_script.py [-p <profile>] [-i]
```

- **`-p, --profile`**: run under a specific profile (e.g. `prod`), `dev` by default. See [Profiles](../core-concepts/profiles-dlthub.md).
- **`-i, --interactive`**: mark the script as interactive (e.g. notebooks). When interactive, the command prints a URL to access the app once running.

Behind the scenes this performs:

1. `deployment sync` (upload code if content changed).
2. `configuration sync` (upload `.dlt/*config.toml` and `.dlt/*secrets.toml` if content changed).
3. Create/update the script, then run it.

## Deploy and run (multi‑command advanced control)

If you want fine-grained control over each step, use `dlt runtime ...` subcommands.
Use `--help` on any subcommand to see all options.

1. Create a new deployment or update it if your Workspace content changed:

  ```sh
  dlt runtime deployment sync
  ```

2. Create a new configuration or update it if `.dlt/*config.toml` or `.dlt/*secrets.toml` changed:

  ```sh
  dlt runtime configuration sync
  ```

3. Create/update a script and trigger a run:

  ```sh
  dlt runtime run path/to/your_script.py [-p <profile>] [-i]
  ```

  `-p` and `-i` options are the same as in the single command deploy (`dlt workspace deploy`).

## Inspect run results

As the script has been launched, `dlt workspace deploy` or `dlt runtime run` returns a run ID. You can use this ID to inspect the run results. Alternatively, you can use the script path to inspect the latest run of this script.

- Show the status and metadata of a specific run (identified by run ID) or latest script run (identified by script path):

  ```sh
  dlt runtime runs <run_id|script_name> info
  ```

- Show the logs of a specific run (identified by run ID) or latest script run (identified by script path):

  ```sh
  dlt runtime runs <run_id|script_name> logs
  ```

- List the run history of a specific script:

  ```sh
  dlt runtime runs <script_name> list
  ```

- List the run history of all scripts in the workspace:

  ```sh
  dlt runtime runs --list
  ```


## Inspect resources

All `dlt runtime ...` subcommands can be used to inspect resources. To list resources, use the `-l` or `--list` option. To show a specific resource, use the `info` subcommand.

### Deployments

- List deployments:

  ```sh
  dlt runtime deployment --list
  ```

- Show the metadata of a specific deployment:

  ```sh
  dlt runtime deployment <deployment_id> info
  ```

### Configurations

- Create a new configuration or update it if `.dlt/*config.toml` or `.dlt/*secrets.toml` changed:

  ```sh
  dlt runtime configuration sync
  ```

- List configurations:

  ```sh
  dlt runtime configuration --list
  ```

- Show the metadata of a specific configuration:

  ```sh
  dlt runtime configuration <configuration_id> info
  ```

### Scripts and runs

- Create/update a script and trigger a run:

  ```sh
  dlt runtime run path/to/your_script.py [-p <profile>] [-i]
  ```

  `-p` and `-i` options are the same as in the single command deploy (`dlt workspace deploy`).

- List the scripts created in the workspace:

  ```sh
  dlt runtime script --list
  ```

- Show the metadata of a specific script:

  ```sh
  dlt runtime script <script_id_or_name> info
  ```

## Resource inspection cheatsheet

- **Deployments**: `dlt runtime deployment --list`, `dlt runtime deployment info <deployment_id>`
- **Configurations**: `dlt runtime configuration --list`, `dlt runtime configuration info <configuration_id>`
- **Scripts**: `dlt runtime script --list`, `dlt runtime script info <script_id_or_name>`
- **Runs**: `dlt runtime runs --list`, `dlt runtime runs <run_id|script_name> list|info|logs|cancel `

## Best practices

- **Use profiles**: Keep `dev` and `prod` separate. See [Profiles](../core-concepts/profiles-dlthub.md).
- **Use single-command deploy to start**: It reduces friction and ensures deployment/configuration stays in sync before each run.
- **Switch to multi‑command when needed**: For CI/CD or controlled rollouts, use `deployment sync`, `configuration sync`, and separate `script`/`runs` operations.
