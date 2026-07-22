---
title: Installation
description: Install the dltHub AI Harness in a new or existing workspace, and add feature toolkits by intent or by command.
keywords: [ai harness, install, dlthub-init, dlthub-start, dlthub ai init, toolkit install]
---

# Installation

You need Python 3.10 or later, [uv](https://docs.astral.sh/uv/) on your PATH, and one of the supported coding agents (Claude Code, Cursor, or Codex) already installed.

## Add the AI Harness to a project

Run the following command in the directory where you want the workspace:

```sh
uvx dlthub-init@latest
```

This writes `.dlt/.workspace`, config and secrets skeletons, `pyproject.toml`, an empty `__deployment__.py`, and the base `init` toolkit into `.agents/skills/`, then symlinks those skills into `.claude/skills/` for Claude Code.

To wire up Cursor or Codex instead, run:

```sh
uv run dlthub ai init --agent cursor
```

Swap `cursor` for `codex` as needed.

## What you get

Your workspace now contains the base `init` toolkit, which ships an MCP server (`dlt-workspace-mcp`), a workspace-setup rule, and three skills:

- `dlthub-router`: routes user intent to the right toolkit and installs it if missing.
- `setup-secrets`: safely manages `.dlt/secrets.toml` without exposing values to the agent.
- `improve-skills`: captures new patterns learned in a session so skills stay lean.

Feature toolkits (REST API pipelines, SQL database, transformations, deployment, and so on) are **not** installed by default. `dlthub-router` installs them automatically when it matches your intent to one, or you can install them manually from the CLI.

## Adding feature toolkits

Most of the time you don't install feature toolkits manually. You talk to your agent about what you want to build, and `dlthub-router` picks the right toolkit and installs it for you.

For example, if you tell the agent:

> I want to ingest pull requests and issues from the GitHub REST API.

`dlthub-router` matches that intent to the `rest-api-pipeline` toolkit, runs `dlthub ai toolkit install rest-api-pipeline` under the hood, and hands off to that toolkit's entry skill (`find-source`).

If you prefer explicit installs, the same three CLI commands are always available. List everything:

```sh
uv run dlthub ai toolkit list
```

Inspect a specific toolkit before installing:

```sh
uv run dlthub ai toolkit info rest-api-pipeline
```

Install it:

```sh
uv run dlthub ai toolkit install rest-api-pipeline
```

By default `dlthub ai toolkit install` installs for the agent already wired up in the workspace. Pass `--agent claude|cursor|codex` to install for a different one, or `--overwrite` to replace files the agent already has.

## Verify

Check that the workspace is fully wired:

```sh
uv run dlthub ai status
```

Output includes the dlt version, the configured agent, the installed toolkits, and warnings when the MCP server or any dependency is missing. Fix the warnings before you start working with your agent.

## What's next

- [Deployment](deployment.md) shows how to use the `dlthub-platform` toolkit end-to-end.
