---
title: AI workflows
description: Explore data in your dltHub project with Claude Desktop using the Model Context Protocol
keywords: [dltHub, Claude Desktop, MCP, Model Context Protocol]
---

# AI workflows

As part of dltHub, we are developing several tools to enhance development with AI workflows. The first of these is a [Model Context Protocol (MCP)](https://modelcontextprotocol.io) plugin for Claude Desktop for data exploration.

## Prerequisites
- dltHub installed in a virtual environment (see [installation guide](../getting-started/installation.md))
- [Claude Desktop](https://claude.ai/download) installed

## Install dltHub with MCP support

Make sure your virtual environment is activated, then:

```sh
pip install dlthub[mcp]
```

## Set up or use a dltHub project

You can either use your existing dltHub project or create a simple test project to try out the MCP workflow.

### Using an existing project
If you already have a dltHub project, you can use it directly - just make sure you have run at least one pipeline so there's some data to explore. You can skip to [configure Claude Desktop](#configure-claude-desktop) if you have a project ready.

### Creating a test project
If you don't have a project yet, here's how to create a simple one:

On Unix-based systems:

```sh
touch dlt.yml
```

Alternatively, you can create an empty dlt.yml file in any text editor.

Copy and paste the following configuration into the `dlt.yml` file:

```yaml
sources:
  pokemon_api:
    type: dlt.sources.rest_api.rest_api
    client:
      base_url: https://pokeapi.co/api/v2
    resource_defaults:
      endpoint:
        params:
          limit: 1000
    resources:
      - pokemon
      - berry
      - location

destinations:
  pokemon_local:
    type: filesystem
    bucket_url: pokemon_data

pipelines:
  pokemon:
    source: pokemon_api
    destination: pokemon_local
    dataset_name: pokemon_dataset

datasets:
  pokemon_dataset:
    destination:
      - pokemon_local
```

This will create a dltHub project with a single pipeline that loads data from the Pokemon API and stores it in a local directory.

Validate the project configuration:

```sh
dlt project config validate
```

If the configuration is valid, you should see the following message:

```sh
Configuration validation successful!
```

That means you can now run the pipeline to get some data:

```sh
dlt pipeline pokemon run
```

If the pipeline runs successfully, you should see the following message:

```sh
1 load package(s) were loaded to destination pokemon_local and into dataset pokemon_dataset
The pokemon_local destination used file:///path/to/your/project/_data/dev/local/pokemon_data location to store data
Load package 1739383145.0668569 is LOADED and contains no failed jobs
```

Great, you have some data in your project. The next step is configuring Claude Desktop, but for this, you'll need to get a path to your `dlt` executable. When you are using a virtual environment, the `dlt` executable is typically located in its `bin` directory (on Unix-like systems). For example, if your virtual environment is located in a `.venv` directory, the path to the `dlt` executable is `.venv/bin/dlt`.
Running `which dlt` in your terminal will give you the path to the `dlt` executable. Take note of it; we will use it in the next step.

## Configure Claude Desktop

Make sure you have installed [Claude Desktop](https://claude.ai/download) and have an account.

### Update Claude desktop config

In Claude Desktop, go to settings (in macOS, Claude > Settings) and under "Developer," click "Edit Config." You will see the location of the `claude_desktop_config.json` file.

Open the file in a text editor and add the following configuration:

```json
{
  "mcpServers": {
    "dltHub project": {
      "command": "</path/to/your/project/.venv/bin/dlt>",
      "args": [
        "project",
        "--project",
        "<path/to/your/project>",
        "mcp"
      ]
    }
  }
}
```

Replace `</path/to/your/project/.venv/bin/dlt>` with the path to your `dlt` executable from the previous step and save the file.

:::warning
If you are using [environment variables](../../general-usage/credentials/setup.md#environment-variables) to configure dlt, make sure to include them as part of the command before the `dlt` executable.
:::

### Restart Claude desktop

**Important**: Restart Claude Desktop so the new configuration is loaded.

:::note
Make sure to restart Claude Desktop after you have updated the config.
:::

### Check the connection

After you have restarted Claude Desktop, you can check the connection by looking at the tool icon in the bottom right corner of the chat box:

![Claude Desktop connection icon](https://storage.googleapis.com/dlt-blog-images/plus/mcp/claude-desktop-tool-icon.png)

If you do not see the icon, ensure your `claude_desktop_config.json` is saved properly and that Claude Desktop was fully restarted.

When you click on the icon, you will see the "Available MCP Tools" popup with the tool's description.

![Claude Desktop available MCP tools](https://storage.googleapis.com/dlt-blog-images/plus/mcp/claude-desktop-available-tools.png)

## Start chatting

Now you can start chatting with Claude Desktop and ask it questions about the data in your dltHub project.

For example, you may ask, "Which tables do I have in my pipeline?":

![Claude Desktop chat example](https://storage.googleapis.com/dlt-blog-images/plus/mcp/claude-desktop-chat-example.png)

Claude will ask you for permission to run the tool locally:

![Claude Desktop permission request](https://storage.googleapis.com/dlt-blog-images/plus/mcp/claude-desktop-permission-request.png)

After you grant the permission, Claude Desktop will run the tool (available_datasets) and (depending on the result) may proceed with selecting and running other tools.

:::tip
To see all the available tools, click on the tool icon in the bottom right corner of the chat box.
:::

More examples of the queries you can ask:

- "What columns does the pokemon table have?"
- "How many rows are in the pokemon table?"
- "Transform the pokemon table to add a new column with the pokemon name length."
- "What is the average height of the pokemon?"

That's it! You can now explore your dltHub project from Claude Desktop using the MCP.

