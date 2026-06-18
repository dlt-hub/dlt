---
title: Workspace setup
description: Convert a Python project into a dltHub platform workspace and configure credentials for dev, prod, and access profiles
keywords: [dlthub platform, workspace, setup, login, profiles, credentials, configuration]
---

# Workspace setup

A workspace ready for the dltHub platform is a regular Python project with a few additions. You can easily convert any existing dlt project into a dltHub workspace.

## 1. Enable dltHub platform features

Initialize the workspace:

```sh
uvx dlthub-init@latest
```

`uvx dlthub-init@latest` scaffolds a ready-to-run workspace—the `.dlt/.workspace` marker that turns on workspace mode, local config and secrets, a managed `pyproject.toml`, and the dltHub AI skills your coding agent uses—then installs dependencies with `uv sync`.

The `.dlt/.workspace` marker activates [profile support](./profiles.md) and enables the `dlthub` CLI command (including `dlthub profile` and `dlthub local`). Run `uvx dlthub-init@latest --help` for options like `--no-sync`. If you'd rather flip the toggle by hand, see [Enable workspace mode](../getting-started/installation.md#enable-workspace-mode).

## 2. Log in to the dltHub platform

```sh
dlthub login
```

This opens a GitHub OAuth device flow and authenticates the current user. Then bind this repo to a remote workspace:

```sh
dlthub workspace connect [<name_or_id>] [--org-id <id>]
```

With no argument, an interactive picker is shown, grouped by organization. The chosen `workspace_id` (and `organization_id`, on the first connect) is persisted to `.dlt/config.toml`. To list workspaces you have access to, use `dlthub workspace list`.

:::note
The first time you run `dlthub deploy`, `dlthub run`, or `dlthub serve`, the CLI walks you through GitHub OAuth and then prompts you to pick (or create) a remote workspace — so you can skip this step entirely.

`organization_id` is write-once. To switch organizations later, remove the line from `.dlt/config.toml` by hand and run `dlthub workspace connect` again.
:::

:::caution
A single GitHub repository can be connected to only one remote workspace at a time. You connect with `dlthub workspace connect`. If you point the same repo at a different remote workspace, jobs deployed under the previous binding are deactivated — run history is preserved but their triggers no longer fire.

Connecting multiple local repositories to the same remote workspace is not yet supported.
:::

## 3. Add pipelines

```sh
dlthub pipeline init <source> <destination>
```

This reuses the same machinery as `dlt init`, so verified sources and templates work as you'd expect. See [Initialize a pipeline](../ingestion/init.md) for templates, verified sources, and the agentic setup.

## Credentials and configs

### Understanding workspace profiles

The dltHub platform uses **profiles** to manage different configurations for different environments. **Some profiles stay local; others are synchronized with the backend.** Local-only profiles live in your repo and are never uploaded. Synced profiles are pushed to the dltHub platform on every deploy so the cloud runtime can use the same configuration when it executes your jobs.

The built-in profiles are:

| Profile | Scope | Purpose | Credentials |
|---------|-------|---------|-------------|
| `dev` | Local only | Local development (default when running on your machine) | Local DuckDB / test credentials |
| `tests` | Local only | Automated tests | Test credentials |
| `prod` | Synced with backend | Production batch jobs running on the dltHub platform | Read/write access to your destination |
| `access` | Synced with backend | Interactive notebooks and dashboards on the dltHub platform | Read-only access (for safe data exploration) |

Any custom profile you reference in a job decorator (e.g. `require={"profile": "analytics"}`) is also synced to the cloud configuration.

When you run a script locally, dlt uses `dev`. When the dltHub platform executes a **batch job**, it uses `prod`. When the dltHub platform serves an **interactive job** (notebook, dashboard, MCP), it uses `access`. If `access` is not configured, interactive jobs fall back to `prod`.

See [profiles in dltHub](./profiles.md) for the full reference.

### Setting up configuration files

Configuration files live in the `.dlt/` directory:

```text
.dlt/
├── .workspace              # Marker file enabling profiles + the `dlthub` CLI
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

# Set automatically by `dlthub workspace connect`
workspace_id = "your-workspace-id"
organization_id = "your-organization-id"
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
