---
title: What is a workspace
description: A dltHub workspace bundles pipelines, configuration, and AI toolkit into a single deployable unit that runs locally and in the cloud.
keywords: [workspace, dlthub, workspace mode, dlt project, profiles, deployment]
---

# What is a workspace?

A workspace is a Python project layout that bundles your dlt pipelines, transformations, configuration, and AI toolkit setup into a single deployable unit. The same folder runs on your local machine, in CI, and — when you deploy — on the managed dltHub platform, so what you build locally is what runs in production.


A workspace is where you:

- Connect a local repo to a remote workspace on dltHub (`dlthub workspace connect`)
- Configure [destinations](../../dlt-ecosystem/destinations/index.md), config, and secrets (often via [profiles](profiles.md) like `dev`, `prod`, `access`)
- Create and manage pipelines, [deployments](deployments.md), jobs/runs, logs, and notebooks
- Control access via [workspace roles](../platform-capabilities/users-and-roles.md) (owner, developer, viewer)

When you create an account, dltHub automatically creates a personal [Playground workspace](playground-workspace.md) so you can try things without any setup.


Every workspace contains:

- **`.dlt/.workspace`** — a marker file that activates the `dlthub` CLI, [profile support](profiles.md), and the managed-platform commands. Without this file you're using plain OSS `dlt`.
- **`.dlt/config.toml`** and **`.dlt/secrets.toml`** — settings and credentials, with optional per-profile overrides (`dev`, `prod`, `tests`, `access`).
- **`pyproject.toml`** (or `requirements.txt`) — workspace-level dependencies like `dlt[hub]`, `duckdb`, `marimo`.
- **Pipeline files** and an optional **`__deployment__.py`** manifest — the code you run, and the description of how it's deployed.
- **AI toolkit configuration** — skills, rules, and MCP wiring for Claude Code, Cursor, or Codex (added when you opt in during scaffolding).


## Creating a workspace

```sh
uv run dlthub login
uv run dlthub workspace connect "<name>" --create
```

## Next steps

* [Workspace setup](workspace-setup.md): convert a Python project into a workspace and configure credentials.
* [The Playground workspace](playground-workspace.md): the auto-created workspace for trying dltHub.
* [Profiles](profiles.md): target different destinations from the same code.
