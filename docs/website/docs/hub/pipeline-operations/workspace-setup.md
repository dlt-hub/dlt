---
title: Workspace setup
description: Convert a Python project into a dltHub platform workspace and configure credentials for dev, prod, and access profiles
keywords: [dlthub platform, workspace, setup, login, profiles, credentials, configuration]
---

# Workspace setup

A workspace ready for the dltHub platform is a regular Python project with a few additions. You can easily convert any existing dlt project into a dltHub workspace.

## 1. Initialize a Python project

If your project doesn't have a `pyproject.toml` yet, create one:

```sh
uv init
```

The dltHub platform uses `pyproject.toml` to install dependencies remotely.

## 2. Enable dltHub platform features

Install `dlt[hub]`:

```sh
uv add "dlt[hub]"
touch .dlt/.workspace
```

The `.dlt/.workspace` file activates [profile support](./profiles.md) and enables the `dlthub` CLI command (including `dlthub profile` and `dlthub local`).

## 3. Log in to the dltHub platform

```sh
dlthub login
```

This opens a GitHub OAuth flow. After authentication, the CLI prompts you to select or create a remote workspace. The workspace ID is stored in `.dlt/config.toml` under `[runtime] workspace_id`.

To list workspaces you have access to, use `dlthub workspace list`. To switch workspaces later without logging out, use `dlthub workspace connect [name_or_id]` (omit the argument to pick interactively).

:::caution
Each GitHub account can have only one remote workspace. When you run `dlthub login`, it connects your current local workspace to that remote workspace. If you later connect a different local repository and deploy, it will replace your existing **deployment** and **configuration**, making any previously scheduled jobs defunct.

Support for multiple remote workspaces (mirroring multiple local repositories) is planned.
:::

## Credentials and configs

### Understanding workspace profiles

The dltHub platform uses **profiles** to manage different configurations for different environments. The relevant profiles are:

| Profile | Purpose | Credentials |
|---------|---------|-------------|
| `dev` | Local development (default when running on your machine) | Local DuckDB / test credentials |
| `prod` | Production batch jobs running on the dltHub platform | Read/write access to your destination |
| `access` | Interactive notebooks and dashboards on the dltHub platform | Read-only access (for safe data exploration) |

When you run a script locally, dlt uses `dev`. When the dltHub platform executes a **batch job**, it uses `prod`. When the dltHub platform serves an **interactive job** (notebook, dashboard, MCP), it uses `access`. If `access` is not configured, interactive jobs fall back to `prod`.

See [profiles in dltHub](./profiles.md) for the full reference.

### Setting up configuration files

Configuration files live in the `.dlt/` directory:

```text
.dlt/
├── .workspace              # Marker file enabling profiles + runtime CLI
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

# dltHub platform connection settings (set automatically by `dlthub login`)
auth_base_url = "https://app.dlthub.com/api/auth"
api_base_url = "https://app.dlthub.com/api/api"
workspace_id = "your-workspace-id"
```

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
