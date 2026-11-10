---
title: Workspace setup
description: Convert a Python project into a dltHub platform workspace and configure credentials for dev, prod, and access profiles
keywords: [dlthub platform, workspace, setup, login, profiles, credentials, configuration]
---
# Workspace setup

A workspace ready for the dltHub platform is a regular Python project with a few additions. You can easily convert any existing dlt project into a dltHub workspace.

Starting from scratch instead? Use `uvx dlthub-start@latest`, which scaffolds a workspace and runs a first pipeline for you — see [Deploy your first pipeline](../getting-started/onboarding.md).

## 1. Enable dltHub platform features

Initialize the workspace:

```cli
uvx dlthub-init@latest
```

`uvx dlthub-init@latest` scaffolds a ready-to-run workspace—the `.dlt/.workspace` marker that turns on workspace mode, local config and secrets, a managed `pyproject.toml`, and the dltHub AI skills your coding agent uses—then installs dependencies with `uv sync`.

The `.dlt/.workspace` marker activates [profile support](./profiles.md) and enables the `dlthub` CLI command (including `dlthub profile` and `dlthub local`). Run `uvx dlthub-init@latest --help` for options like `--no-sync`. If you'd rather flip the toggle by hand, see [Enable workspace mode](../getting-started/installation.md#enable-workspace-mode).

## 2. Log in to the dltHub platform

```cli
dlthub login
```

This opens a GitHub OAuth device flow and authenticates the current user. Then connect the local workspace to a remote workspace:

```cli
dlthub workspace connect [<workspace>] [--org-id <org_id>]
```

With no argument, an interactive picker is shown, grouped by organization. The chosen `workspace_id` (and `organization_id`, on the first connect) is persisted to `.dlt/config.toml`. The command connects the local workspace in the default environment, `prod`. The first deployment to another environment [connects the local workspace in that environment](profiles.md#connect-to-a-remote-workspace-in-each-environment). To list workspaces you have access to, use `dlthub workspace list`.

:::note
The first time you run `dlthub deploy`, `dlthub run`, or `dlthub serve`, the CLI walks you through GitHub OAuth and then prompts you to pick (or create) a remote workspace — so you can skip this step entirely.

`organization_id` is write-once. To switch organizations later, remove the line from `.dlt/config.toml` by hand and run `dlthub workspace connect` again.
:::

:::caution
In each environment, a local workspace is connected to only one remote workspace at a time. You connect with `dlthub workspace connect`. If you point the same repo at a different remote workspace, jobs deployed under the previous connection are deactivated — run history is preserved but their triggers no longer fire.

Connecting multiple local repositories to the same remote workspace is not yet supported.
:::

## 3. Add pipelines

```cli
dlthub pipeline init <source> <destination>
```

This reuses the same machinery as `dlt init`, so verified sources and templates work as you'd expect. See [Initialize a pipeline](../ingestion/init.md) for templates, verified sources, and the agentic setup.

## Credentials and configs

### Understanding workspace profiles

A **profile** controls access to your data. It holds the credentials that a pipeline or job runs with and declares their data access: write, read or none. It also holds settings. You set up the rights of each credential at the destination, dlt does not check them. A local profile runs only on your machine. A remote profile belongs to an environment and deploys with it. The built-in profiles are:

| Profile     | Location | Data access | Used by                                                      |
| ----------- | -------- | ----------- | ------------------------------------------------------------ |
| `dev`       | local    | write       | your machine, the default profile                            |
| `tests`     | local    | write       | automated tests                                              |
| `prod`      | remote   | write       | batch jobs on the dltHub platform                            |
| `access`    | remote   | read        | interactive jobs on the dltHub platform                      |
| `no-access` | remote   | none        | [agent jobs](../agents/index.md) that declare no data access |

A deployment is made to an **environment**, `prod` by default, and gives every job the profile of the environment with the data access that the job declares. See [Profiles and environments](./profiles.md) for the full reference.

### Setting up configuration files

Configuration files live in the `.dlt/` directory:

```tree
.dlt/
├── .workspace              # Declares the profiles and environments, enables the `dlthub` CLI
├── config.toml             # Workspace-wide config (all profiles)
├── secrets.toml            # Workspace-wide secrets (gitignored)
├── dev.config.toml         # Dev profile config
├── prod.config.toml        # Production profile config
├── prod.secrets.toml       # Production secrets (gitignored)
├── access.config.toml      # Access profile config
└── access.secrets.toml     # Access secrets (gitignored)
```

Settings in profile-scoped files override workspace-scoped files. Below is an example using **named destinations** so the same `destination="warehouse"` resolves to DuckDB locally and MotherDuck in production. You can swap MotherDuck for any cloud destination — see for example [BigQuery](../../dlt-ecosystem/destinations/bigquery.md), [Snowflake](../../dlt-ecosystem/destinations/snowflake.md), or [filesystem/S3](../../dlt-ecosystem/destinations/filesystem.md).

**`config.toml`** (defaults shared by all profiles):

```toml
[runtime]
log_level = "WARNING"
dlthub_telemetry = true

# One organization for the whole workspace
organization_id = "your-organization-id"

# Written by `dlthub workspace connect`
[workspace.environment.prod]
workspace_id = "your-workspace-id"

# Written by the first deployment to `staging`
[workspace.environment.staging]
workspace_id = "your-staging-workspace-id"
```

`api_base_url` defaults to `https://api.dlthub.com` and only needs to be set when targeting a self-hosted control plane. For non-interactive auth (CI, scripts), set `api_key` in `secrets.toml` under `[runtime]` instead of relying on the OAuth flow.

**`dev.config.toml`** (local DuckDB):

```toml
[destination.warehouse]
destination_type = "duckdb"
```

**`prod.config.toml`** (production destination):

```toml
[destination.warehouse]
destination_type = "motherduck"
```

**`prod.secrets.toml`** (read/write credentials for batch jobs):

```toml
[destination.warehouse.credentials]
database = "your_database"
password = "your-motherduck-service-token"
```

**`access.config.toml`** + **`access.secrets.toml`** (read-only credentials for interactive jobs):

```toml
[destination.warehouse]
destination_type = "motherduck"
```

```toml
[destination.warehouse.credentials]
database = "your_database"
password = "your-motherduck-read-only-token"
```

:::warning Security
Files matching `*.secrets.toml` and `secrets.toml` are gitignored by default. Never commit secrets to version control. The dltHub platform stores your secrets securely when you sync your configuration.
:::

## Next steps

- [Deployments](deployments.md) — quick deploys and the full deployment workflow
- [Triggers and scheduling](triggers.md) — schedule jobs, chain follow-ups, and backfill with intervals
- [Monitoring and debugging](monitoring.md) — watch runs, stream logs, diagnose failures
