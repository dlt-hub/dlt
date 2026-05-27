---
title: Installation
description: Install dlt[hub], create a workspace, and license paid features
keywords: [installation, dlthub, dlthub init, workspace mode, license]
---

:::info Supported Python versions

dltHub currently supports Python versions 3.10-3.13.

:::

## What is a dltHub workspace?

A workspace is a Python project layout that bundles your `dlt` pipelines, transformations, configuration, and AI toolkit setup into a single deployable unit. The same folder runs on your local machine, in CI, and — when you deploy — on the managed dltHub platform, so what you build locally is what runs in production.

Every workspace contains:

- **`.dlt/.workspace`** — a marker file that activates the `dlthub` CLI, [profile support](../pipeline-operations/profiles.md), and the managed-platform commands. Without this file you're using plain OSS `dlt`.
- **`.dlt/config.toml`** and **`.dlt/secrets.toml`** — settings and credentials, with optional per-profile overrides (`dev`, `prod`, `tests`, `access`).
- **`pyproject.toml`** (or `requirements.txt`) — workspace-level dependencies like `dlt[hub]`, `duckdb`, `marimo`.
- **Pipeline files** and an optional **`__deployment__.py`** manifest — the code you run, and the description of how it's deployed.
- **AI toolkit configuration** — skills, rules, and MCP wiring for Claude Code, Cursor, or Codex (added when you opt in during scaffolding).

For the wider feature surface that a workspace unlocks — [profiles](../pipeline-operations/profiles.md), [data quality](../data-quality/index.md), [transformations](../transformations/index.md), the [managed platform](../pipeline-operations/overview.md), the [dashboard](../ingestion/dashboard.md) — see the [introduction](introduction.md).

## Quickstart

If you already have `uv` installed:

```sh
uvx dlthub-start@latest
```

If you don't have `uv` yet, either [install it first](#setting-up-your-environment) or run via `pipx` — the CLI will offer to install `uv` for you before syncing dependencies:

```sh
pipx run dlthub-start
```

Either way, it prompts for a workspace name, scaffold, and which AI agents to wire up (Claude / Cursor / Codex), scaffolds a workspace with `.dlt/.workspace` already set, vendors the AI toolkits (`rest-api-pipeline`, `transformations`, `dlthub-platform`, `data-exploration`), and runs `uv sync` so `dlt[hub]` and all workspace dependencies are installed.

For the recommended defaults non-interactively, pass a name explicitly:

```sh
uvx dlthub-start@latest my-workspace --yes
```

## Setting up your environment

### Configuration of the Python environment

In this documentation, we use `uv` (a modern package manager) to install Python versions, manage virtual environments, and manage project dependencies.
To install `uv`, you can use `pip` or follow [the OS-specific installation instructions](https://docs.astral.sh/uv/getting-started/installation/).

Once you have `uv` installed you can pick any Python version supported by it:

```sh
uv python install 3.13
```

or use any Python version you have installed on your system.

### Virtual environment

We recommend working within a [virtual environment](https://docs.python.org/3/library/venv.html) when creating Python projects.
This way, all the dependencies for your current project will be isolated from packages in other projects. With `uv`, run:
```sh
uv venv
```
This will create a virtual environment in the `.venv` folder using the default system Python version.

```sh
uv venv --python 3.13
```
This will use `Python 3.13` for your virtual environment.


Activate the virtual environment using the instructions displayed by `uv`, i.e.:

```sh
source .venv/bin/activate
```

## Add dltHub to an existing project

To install `dlt[hub]` into an existing project, activate its virtual environment and run:
```sh
uv pip install "dlt[hub]"
```
This installs `dlt` plus two plugin packages pulled in by the `hub` extra:
* `dlthub`—enables the **dlthub** command and features like AI toolkits and transformations
* `dlthub-client`—enables access to the [managed dltHub Platform](../pipeline-operations/overview.md) (login, deploy, run, serve, ...)

Workspace-level dependencies (destinations like `duckdb`, plus tools like `marimo` or `fastmcp` used by notebooks and MCP jobs) are managed in your workspace's `pyproject.toml`, not via `dlt` extras. Run `dlthub init` (see [below](#enable-workspace-mode))—it scaffolds a `pyproject.toml` you can extend with `uv add <package>`.

## Upgrade existing installation

To upgrade just the `hub` extra without upgrading `dlt` itself run:
```sh
uv pip install -U "dlt[hub]==1.27.0"
```
This keeps the current `1.27.0` `dlt` and upgrades `dlthub` and `dlthub-client` to their newest matching versions.

:::tip
A particular `dlt` version expects `dlthub` and `dlthub-client` versions in a matching range. For example: `1.27.x` expects
`0.27.x` of each plugin. This is enforced via dependencies in the `hub` extra and at import time. Installing a plugin directly will not change the
installed `dlt` version (to prevent unwanted upgrades). For example if you run:
```sh
uv pip install dlthub
```
and it downloads `0.28.0` of the plugin, `dlt` `1.27.0` will still be installed but it will report a wrong plugin version on import (with instructions
how to install a compatible plugin version).
:::

## Enable workspace mode

The full dltHub feature surface—profiles, the `dlthub` CLI host, and [managed-platform commands](../pipeline-operations/overview.md)—is gated behind **Workspace mode**, signaled by a `.dlt/.workspace` marker file. The simplest way to turn it on is:

```sh
dlthub init
```

This scaffolds a fresh dltHub workspace—it creates the `.dlt/.workspace` marker plus `config.toml`, `secrets.toml`, `.gitignore`, and a `pyproject.toml` (or `requirements.txt` if `uv` isn't on `PATH`). See [Initialize a pipeline](../ingestion/init.md) for the next steps.

If you'd rather flip the toggle by hand in an existing project, create the empty marker file yourself:

<Tabs values={[{"label": "Ubuntu", "value": "ubuntu"}, {"label": "macOS", "value": "macos"}, {"label": "Windows", "value": "windows"}]} groupId="operating-systems" defaultValue="ubuntu">
<TabItem value="ubuntu">

```sh
mkdir -p .dlt && touch .dlt/.workspace
```

  </TabItem>
  <TabItem value="macos">

```sh
mkdir -p .dlt && touch .dlt/.workspace
```

  </TabItem>
  <TabItem value="windows">

```sh
mkdir .dlt
type nul > .dlt\.workspace
```

  </TabItem>
</Tabs>
