---
title: MCP server
description: Install the dlt MCP with your preferred LLM-enabled IDE.
keywords: [mcp, llm, agents, ai]
---

# MCP Server

Currently, dltHub is [building two MCP servers](https://dlthub.com/blog/deep-dive-assistants-mcp-continue) that you can run locally and integrate with your preferred IDE. One server is for the open-source `dlt` library and the other integrates with `dlt+` features ([Learn more](../../plus/features/ai.md)).

This page gives an overview of what we're building and includes detailed instructions to install the MCP in your favorite IDE.

:::caution
🚧 This feature is under development, and the interface may change in future releases. Interested in becoming an early tester? [Join dlt+ early access](https://info.dlthub.com/waiting-list).
:::

## Model Context Protocol
The [Model Context Protocol](https://modelcontextprotocol.io/introduction) (MCP) is a standard initiated by Anthropic to connect large language models (LLMs) to external data and systems.

In the context of the MCP, the **client** is built-in the user-facing application. The most common clients are LLM-enabled IDEs or extensions such as Continue, Cursor, Claude Desktop, Cline, etc. The  **server** is a process that handles requests to interact with external data and systems.

### Core constructs

- **Resources** are data objects that can be retrieved by the client and added to the context (i.e., prompt) of the LLM request. Resources will be manually selected by the user, or certain clients will automatically retrieved them.

- **Tools** provided a way to execute code and provide information to the LLM. Tools are called by the LLM; they can't be selected by the user or the client. 

- **Prompts** are strings, or templated strings, that can be injected in the conversation. Prompts are selected by the user. They provide shortcuts for frequent commands, or allow to ask the LLMs to use specific tools.


:::note
The MCP is progressively being adopted and not all clients support all the features used by the dlt MCP server. See [this page](https://modelcontextprotocol.io/clients) for an overview of client capabilities
:::


## Features

The `dlt` and `dlt+` MCP servers aim to be a toolbox to help developers build, maintain, and operate `dlt` pipelines. There are two primary avenues:

- **Code generation**: LLMs are good at writing Python code, but they don't know everything about `dlt`. The MCP provides resources and tools to provide up-to-date information to the LLm about the dlt library and the specifics of your project.

- **Assistance**: The MCP resources, tools, and prompts can also be used to learn more about `dlt`. The built-in knowledgebase combined with your project's context brings Q&A support inside your IDE.

The next sections are a non-exhaustive documentation of existing and upcoming features.

### Tools

- Use your local pipeline directory (default: `~/.dlt/pipelines`) to retrieve pipeline metadata. This includes: available pipleines, available tables, table schemas. It also include operational information such as last load date, schema changes, schema diffs, load packages data.

- Use your pipeline context to inspect your dlt configuration (sources, resources, destinations).

- Connect to the destination to retrieve data. This enables table previews and light text-to-SQL capabilities.

### Resources

- LLM-optimized dlt documentation pages. These can be selected and added to your prompt to help the LLM generate valid `dlt` code.

### Prompts

- Tutorial-like instructions that puts the LLM in "assistant mode". For example, the LLM can ask you questions about your data and "think" with you to select the right loading strategy.

- Command-like instructions that gives the LLM a task to complete. For example, the LLM can initialize a new pipeline. This is akin to a conversational command line tool.


## Installation

The `dlt` and `dlt+` MCP servers are intended to run locally on your machine and communicate over standard I/O. Typically, the MCP server process is launched by the MCP client, i.e., the IDE. We will use the [uv package manager](https://docs.astral.sh/uv/#installation) to launch the MCP server.

The next sections include client-specific instructions, references, and snippets to configure the MCP server. They are mainly derived from this `uv` command:

```sh
uv tool run --with "dlt-plus[mcp]==0.9.0" dlt mcp run
```

To explain each part:
- [uv tool run](https://docs.astral.sh/uv/guides/tools/) executes the command in an isolated virtual environment
- the `--with PACKAGE_NAME` specify the Python dependencies required by the MCP server
- `dlt-plus==0.9.0` pins a specific `dlt-plus` version (where the MCP code lives). We suggest at least pinning the `dlt-plus` version to provide a consistent experience
- `dlt mcp run` is a CLI command found in `dlt+`

Then, to enable the MCP server and tool usage, several IDEs require you to enable "tool/agent/mcp mode".

### dlt+ MCP server

To run the `dlt+` MCP server, you will need to set your [dlt+ License](../../plus/getting-started/installation#licensing) globally in `~/.dlt/secrets.toml` or in an environment variable (must be set before lauching the IDE).

Then, use `dlt mcp run_plus` to launch the server instead of `dlt mcp run`. If the `dlt+` license is missing, the `dlt` MCP server will be launched instead. You can tell the two apart by the tools, resources, and prompts available­.


### Continue

With Continue, you can use [Continue Hub](https://docs.continue.dev/hub/introduction) for a 1-click install of the MCP, or a local config file. Select `Agent Mode` to enable the MCP server.

#### Continue Hub
See the [dltHub page](https://hub.continue.dev/dlthub) and select the `dlt` or `dlt+` Assistants. This bundles the MCP with additional Continue-specific features. You can also select the `dlt` or `dlt+` MCP blocks to install the server exclusively.

#### Local
You can define an assistant locally with the same YAML syntax as the Continue Hub by adding files to `$PROJECT_ROOT/.continue/assistants`. This snippet creates an assistant with the MCP only.

```yaml
# local_dlt.yaml
name: dlt MCP  # can change
version: 0.0.1  # can change
schema: v1
mcpServers:
  - name: dlt  # can change
    command: uv
    args:
      - tool
      - run
      - --with
      - dlt-plus==0.9.0
      - --with
      - sqlglot
      - --with
      - pyarrow
      - --with
      - pandas
      - --with
      - duckdb
      - --with
      - mcp
      - dlt
      - mcp
      - run
```

There's also a global configuration specs in JSON
```json
{
  // ... more content above
  "experimental": {
    "modelContextProtocolServers": [
      {
        "transport": {
          "type": "stdio",
          "command": "uv",
          "args": [
            "tool",
            "run",
            "--with",
            "dlt-plus[mcp]==0.9.0",
            "dlt",
            "mcp",
            "run"
          ]
        }
      }
    ]
  }
}
```

### Claude Desktop

You need to [add a JSON configuration file](https://modelcontextprotocol.io/quickstart/user#2-add-the-filesystem-mcp-server) on your system. See our [full Claude Desktop tutorial](../../plus/features/ai.md)

```json
{
  "mcpServers": {
    "dlt": {  // server name
      "command": "uv",
      "args": [
        "tool",
        "run",
        "--with",
        "dlt-plus[mcp]==0.9.0",
        "dlt",
        "mcp",
        "run"
      ]
    } 
  }
}
```


### Cursor

Select `Agent Mode` to enable the MCP server. The [configuration](https://docs.cursor.com/context/model-context-protocol#configuring-mcp-servers) follows the same JSON specs as Claude Desktop

```json
{
  "mcpServers": {
    "dlt": {  // server name
      "command": "uv",
      "args": [
        "tool",
        "run",
        "--with",
        "dlt-plus[mcp]==0.9.0",
        "dlt",
        "mcp",
        "run"
      ]
    } 
  }
}
```

### Cline

Follow [this tutorial](https://docs.cline.bot/mcp-servers/mcp-quickstart) to use the IDE's menu to add MCP servers.
