---
title: MCP server
description: Install the dlt MCP with your preferred LLM-enabled IDE.
keywords: [mcp, llm, agents, ai]
---

## Overview

dltHub Workspace comes with an MCP server you can run locally and integrate with your preferred IDE. It provides a set of tools for interacting with pipelines and datasets:
- Explore and describe pipeline schemas
- Access and explore data in destination tables
- Write SQL queries, models, and transformations
- Combining all the above, it provides efficient help with writing reports, notebook code, and dlt pipelines themselves

🚧 (in development) We are adding a set of tools that help drill down into pipeline run traces to find possible problems and root causes for incidents.

The MCP server can be started in:
- Workspace context, where it will see all the pipelines in it
- Pipeline context, where it is attached to a single pipeline

Users can start as many MCP servers as necessary. The default configurations and examples below assume that workspace and pipeline MCP servers can work side by side.

## Launch MCP server

Since all MCP clients work with `sse` transport now, it is the default when running the server. The MCP will attach to the workspace context
and pipelines in it. It must be able to start in the same Python environment and see the same workspace as `dlt` when running pipelines.
There were also issues with `stdio` pollution from `print` statements—one misbehaving dependency could break the transport channel.
TL;DR: We still support `stdio` transport, but it is not recommended.

To launch the server in **workspace context**:
```sh
dlt workspace mcp

INFO:     Started server process [24925]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:43654 (Press CTRL+C to quit)
```
The workspace MCP server has **43654** as the default port and is configured without any path (i.e., `/sse`), so users can just copy the link above into the appropriate client.

To launch the server in **pipeline context**:
```sh
dlt pipeline fruitshop mcp

Starting dlt MCP server
INFO:     Started server process [28972]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:43656 (Press CTRL+C to quit)

```
The pipeline MCP server has **43656** as the default port. The pipeline is already attached when the MCP server starts. Both pipeline and workspace MCP servers can work side by side.


### Configure MCP server

#### Cursor, Cline, Claude Desktop
```json
{
  "mcpServers": {
    "dlt-workspace": {
      "url": "http://127.0.0.1:43654/"
    },
    "dlt-pipeline-mcp": {
      "url": "http://127.0.0.1:43656/"
    }
  }
}
```

### Continue (local)

```yaml
name: dlt mcps
version: 0.0.1
schema: v1
mcpServers:
  - name: dlt-workspace
    type: sse
    url: "http://localhost:43654"
```

### Continue Hub

With Continue, you can use [Continue Hub](https://docs.continue.dev/hub/introduction) for a 1-click install of the MCP, or a local config file. Select `Agent Mode` to enable the MCP server.

See the [dltHub page](https://hub.continue.dev/dlthub) and select the `dlt` or `dltHub` Assistant. This bundles the MCP with additional Continue-specific features. 

## Configure MCP server
The server can still be started with `stdio` transport and a different port using the command line.

🚧 The feature below is in development and not yet available:
The plan is to allow full configuration of MCP via the dlt configuration system.

```toml
[workspace.mcp]
path="/sse"
port=888
```

```toml
[pipelines.fruitshop.mcp]
transport="stdio"
```


## Model Context Protocol
The [Model Context Protocol](https://modelcontextprotocol.io/introduction) (MCP) is a standard initiated by Anthropic to connect large language models (LLMs) to external data and systems.

In the context of MCP, the **client** is built into the user-facing application. The most common clients are LLM-enabled IDEs or extensions such as Continue, Cursor, Claude Desktop, Cline, etc. The **server** is a process that handles requests to interact with external data and systems.

### Core constructs

- **Resources** are data objects that can be retrieved by the client and added to the context (i.e., prompt) of the LLM request. Resources will be manually selected by the user, or certain clients will automatically retrieve them.

- **Tools** provide a way to execute code and provide information to the LLM. Tools are called by the LLM; they can't be selected by the user or the client.

- **Prompts** are strings, or templated strings, that can be injected into the conversation. Prompts are selected by the user. They provide shortcuts for frequent commands or allow asking the LLM to use specific tools.
