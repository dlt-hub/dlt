---
title: AI Workflows
description: Explore data in your dlt+ project with Claude Desktop using the Model Context Protocol
keywords: [dlt+, Claude Desktop, MCP, Model Context Protocol]
---

# AI Workflows

As part of dlt+ we are developing several tools to enhance development with AI workflows. The first of these is an  [Model Context Protocol (MCP)](https://modelcontextprotocol.io) plugin for Claude Desktop for data exploration.

### Prerequisites
- Python 3.10+
-  A virtual environment (and all commands should be run from within it)
- [Claude Desktop](https://claude.ai/download) installed

### Install dlt+ with MCP support

Make sure your virtual environment is activated, then:

```bash
pip install dlt-plus[mcp]
```

### Set up a test dlt+ project

On Unix-based systems:

```bash
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
    bucket_url: ${tmp_dir}pokemon_data

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

Validate the project configuration:

```bash
dlt project config validate
```

If the configuration is valid, you should see the following message:

```
Configuration validation successful!
```

That means you can now run the pipeline to get some data:

```bash
dlt pipeline pokemon run
```

```
1 load package(s) were loaded to destination pokemon_local and into dataset pokemon_dataset
The pokemon_local destination used file:///path/to/your/project/_storage/pokemon_data location to store data
Load package 1739383145.0668569 is LOADED and contains no failed jobs
```

Great, you have some data in your project. The next is configuring Claude Desktop, but for this you'd need to get a path to your `dlt` executable. When you are using a virtual environment, the `dlt` executable is typically located in its `bin` directory (on Unix-like systems). For example, if your virtual environment is located in a `.venv` directory, the path to `dlt` executable is `.venv/bin/dlt`.
Running `which dlt` in your terminal will give you the path to `dlt` executable. Take note of it; we will use it in the next step.

### Configure Claude Desktop

Make sure you have installed [Claude Desktop](https://claude.ai/download) and have an account.

#### Update Claude Desktop Config

In Claude Desktop, go to settings (In macOS, Claude > Settings) and under “Developer” click “Edit Config”. You will see the location of `claude_desktop_config.json` file.

Open the file in a text editor and add the following configuration:

```json
{
  "mcpServers": {
    "dlt+ project": {
      "command": "</path/to/your/project/.venv/bin/dlt>",
      "args": [
        "mcp",
        "--project",
        "<path/to/your/project>",
        "run"
      ]
    }
  }
}
```

Replace `</path/to/your/project/.venv/bin/dlt>` with the path to your `dlt` executable from the previous step and save the file.

#### Restart Claude Desktop

**Important**: Restart Claude Desktop so the new configuration is loaded.

:::note
Make sure to restart Claude Desktop after you have updated the config.
:::

### Check the connection

After you have restarted Claude Desktop, you can check if the connection by looking at the tool icon in the bottom right corner of the chat box:

![Claude Desktop connection icon](https://storage.googleapis.com/dlt-blog-images/plus/mcp/claude-desktop-tool-icon.png)

If you do not see the icon, ensure your `claude_desktop_config.json` is saved properly and that Claude Desktop was fully restarted.

When you click on the icon, you will see the "Available MCP Tools" popup with the tools description.

![Claude Desktop available MCP tools](https://storage.googleapis.com/dlt-blog-images/plus/mcp/claude-desktop-available-tools.png)

### Start chatting

Now you can start chatting with Claude Desktop and ask it questions about the data in your dlt+ project.

For example, we may ask "Which tables do I have in my pipeline?":

![Claude Desktop chat example](https://storage.googleapis.com/dlt-blog-images/plus/mcp/claude-desktop-chat-example.png)

Claude will ask you a permission to run the tool locally:

![Claude Desktop permission request](https://storage.googleapis.com/dlt-blog-images/plus/mcp/claude-desktop-permission-request.png)

After you grant the permission, Claude Desktop will run the tool (available_datasets) and (depending on the result) may proceed with selecting and running other tools.

That’s it! You can now explore your dlt+ project from Claude Desktop using the MCP.
