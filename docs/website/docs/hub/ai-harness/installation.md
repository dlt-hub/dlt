---
title: Install the AI harness
sidebar_label: Installation
description: Install the dltHub AI Harness in a new or existing workspace, and add feature toolkits by intent or by command.
keywords: [ai harness, install, dlthub-init, dlthub init, dlthub ai init, toolkit install, mcp server, cortex code, codex]
---
# Installation

To install the [AI Harness](https://dlthub.com/features/ai-harness), you need:

- Python 3.10 or later
- [uv](https://docs.astral.sh/uv/) on your PATH
- one of the supported coding agents: Claude Code, Cursor, Codex, or Snowflake Cortex Code

## Add the AI Harness to a project

Run this command in the directory of the workspace:

```cli
uvx dlthub-init@latest
```

The command creates a dltHub workspace and installs the base `init` toolkit for Claude Code:

```tree
your-project/
├── .dlt/
│   ├── .workspace              # marks this directory as a dltHub workspace
│   ├── config.toml             # workspace-wide config
│   └── secrets.toml            # workspace-wide secrets (gitignored)
├── .claude/
│   ├── rules/                  # rules of the `init` toolkit
│   └── skills/                 # skills of the `init` toolkit
├── .mcp.json                   # registers `dlt-workspace-mcp`
└── pyproject.toml
```

The other agents get the skills, rules, and MCP server in their own folders. See [The MCP server](#the-mcp-server).

To use Cursor, Codex, or Cortex Code in an existing workspace, run:

```cli
uv run dlthub ai init --agent cursor
```

Replace `cursor` with `codex` or `cortex`.

### Create the workspace with `dlthub init`

You can also create the workspace with `dlthub init`. The command adds the dependencies that your agent needs, then installs the `init` toolkit for this agent:

```cli
uvx --from dlt@latest dlthub init --agent cortex
```

Without a name, `--agent` detects the agent. Without `--agent`, `dlthub init` creates the workspace and installs no toolkit. Run `uv sync` afterwards to install the dependencies. In an existing workspace, `dlthub init` changes nothing: run `dlthub ai init` instead.

## What you get

The base `init` toolkit has an MCP server (`dlt-workspace-mcp`), a rule for the workspace, and three skills:

- `dlthub-router`: matches your intent to a toolkit, and installs the toolkit when it is missing.
- `setup-secrets`: manages `.dlt/secrets.toml` and does not show the secret values to the agent.
- `improve-skills`: adds the patterns that a session found to the skills, and keeps the skills short.

Feature toolkits for REST API pipelines, SQL databases, transformations, deployment, and more are not installed by default. `dlthub-router` installs them when it matches your intent to one. You can also install them with the CLI.

The toolkits write what they produce, such as schemas, data models and exploration plans, into `.dlthub/` in your workspace. See [Artifact locations](artifact-locations.md).

### The MCP server

dlt installs one MCP server, `dlt-workspace-mcp`. Your agent starts it with `uv run dlthub ai mcp --stdio` and uses the stdio transport. A feature toolkit does not add a second server. It can add feature groups to the `--features` argument of `dlt-workspace-mcp` or remove them from it. Each feature group gives more tools.

Each agent reads the server from its own config file:

| Agent       | Config file                                                                   |
| ----------- | ----------------------------------------------------------------------------- |
| Claude Code | `.mcp.json`                                                                   |
| Cursor      | `.cursor/mcp.json`                                                            |
| Codex       | `.codex/config.toml`                                                          |
| Cortex Code | `.cortex/mcp.json` for Desktop, `$SNOWFLAKE_HOME/cortex/mcp.json` for the CLI |

The Cortex Code CLI reads MCP servers only from your user config, and `$SNOWFLAKE_HOME` is `~/.snowflake` by default. For Codex, dlt also sets `mcp_optional_startup_grace_ms = 5000` when your config does not set it. Codex then waits up to 5 seconds for the server before the first turn of a session.

If your agent does not show the tools of `dlt-workspace-mcp`:

1. Run `uv run dlthub ai status`. The output tells you when the MCP server cannot start, and why.
2. If a toolkit install changed the server, reconnect the server in your agent, or start a new session.
3. In Cursor, enable the server in Cursor Settings > MCP.
4. In Codex, trust the project. Codex reads `.codex/config.toml` only in a trusted project.
5. In Cortex Code, run `cortex mcp list` to see the state of the server. Then run `cortex mcp reconnect dlt-workspace-mcp`.
6. In Cortex Code Desktop, trust the workspace. Desktop loads project MCP servers only in a trusted workspace.

## Adding feature toolkits

Usually, your agent installs feature toolkits for you. Tell the agent what you want to build, and `dlthub-router` picks the toolkit and installs it. For example, you tell the agent:

> I want to ingest pull requests and issues from the GitHub REST API.

`dlthub-router` matches this intent to the `rest-api-pipeline` toolkit. It runs `dlthub ai toolkit install rest-api-pipeline`, then starts the entry skill of the toolkit (`find-source`).

You can also use the CLI. List the toolkits:

```cli
uv run dlthub ai toolkit list
```

Show the contents of a toolkit:

```cli
uv run dlthub ai toolkit info rest-api-pipeline
```

Install the toolkit:

```cli
uv run dlthub ai toolkit install rest-api-pipeline
```

`toolkit install` installs for the agent of the workspace. To install for a different agent, pass `--agent claude|cursor|codex|cortex`. To update an installed toolkit, pass `--overwrite`. The update replaces the files that the toolkit ships, and removes the files that the new version does not ship. This includes the files that you edited. The files that you added stay.

A toolkit that ships agent definitions installs each one to `.claude/dlthub/agents/<toolkit>/<agent>/`. For Cursor, Codex, and Cortex Code, the folder starts with `.cursor/`, `.agents/`, or `.cortex/`. There, `run.agent("<toolkit>:<agent>")` finds it. See [Background agents](../agents/index.md).

### Toolkits for specific agents

The `toolkit.json` file of a toolkit can name the agents that see the toolkit, or that see one of its skills, rules, or MCP servers. Your agent sees and installs only what is made for it. To see and install everything, pass `--see-all` to `toolkit list`, `toolkit info`, or `toolkit install`.

For example, the `snowflake-platform` toolkit is made for Cortex Code. To install it for Claude Code, run:

```cli
uv run dlthub ai toolkit install snowflake-platform --agent claude --see-all
```

A toolkit that you installed with `--see-all` keeps all its parts when you update it.

### Rules for Codex and Cortex Code

Codex and Cortex Code do not read a folder of rules, so dlt gives them the rules in a different way:

- **Codex**: dlt installs each rule as a skill in `.agents/skills/`. A session start hook in `.codex/hooks.json` runs `uv run dlthub ai rules --agent codex`. This command prints a link to each rule skill and tells Codex to obey these skills. Codex runs the hook only in a trusted project, after you approve it once with `/hooks`. You can delete the skill list that older dlt versions wrote to `AGENTS.md`.
- **Cortex Code**: dlt installs each rule to `.cortex/rules/`. After each toolkit install, dlt writes all the rules into the `# dltHub rules` section of `CORTEX.md`. To change a rule, edit its file, because dlt replaces the section. The text outside the section stays. Cortex Code reads skills and `CORTEX.md` when a session starts, so start a new session after an install.

## Verify

Make sure that the workspace is ready:

```cli
uv run dlthub ai status
```

The output shows the dlt version, the configured agent, the installed toolkits, and a warning for each missing MCP server or dependency. Fix the warnings before you start to work with your agent.

## What's next

- [Toolkits](toolkits.md) shows all the toolkits and where each one fits in the development cycle.
- [Deploy with the dltHub AI Harness](../pipeline-operations/deployments.md#deploy-with-the-dlthub-ai-harness) shows the `dlthub-platform` toolkit from start to end.
