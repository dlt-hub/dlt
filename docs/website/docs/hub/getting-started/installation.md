---
title: Installation
description: Installation information for the dlthub package
---

:::info Supported Python versions

dltHub currently supports Python versions 3.10-3.13.

:::

## Quickstart

To install the `dlt[hub]` package, create a new [Python virtual environment](#setting-up-your-environment) and run:
```sh
uv pip install "dlt[hub]"
```
This installs `dlt` plus two plugin packages pulled in by the `hub` extra:
* `dlthub` — enables features that require a [license](#licensing)
* `dlthub-client` — enables access to the [managed dltHub Platform](../runtime/overview.md) (login, deploy, run, serve, ...)

If you also want the common local development dependencies (`duckdb`, `marimo`, `pyarrow`, `fastmcp`, ...), install them with the destination/feature extras you actually need, e.g.:
```sh
uv pip install "dlt[hub,duckdb,parquet]"
```

If you need to install `uv` (a modern package manager), [please refer to the next section](#configuration-of-the-python-environment).

### Upgrade existing installation

To upgrade just the `hub` extra without upgrading `dlt` itself run:
```sh
uv pip install -U "dlt[hub]==1.20.0"
```
This keeps the current `1.20.0` `dlt` and upgrades `dlthub` and `dlthub-client` to their newest matching versions.

:::tip
A particular `dlt` version expects `dlthub` and `dlthub-client` versions in a matching range. For example: `1.20.x` expects
`0.20.x` of each plugin. This is enforced via dependencies in the `hub` extra and at import time. Installing a plugin directly will not change the
installed `dlt` version (to prevent unwanted upgrades). For example if you run:
```sh
uv pip install dlthub
```
and it downloads `0.21.0` of the plugin, `dlt` `1.20.0` will still be installed but it will report a wrong plugin version on import (with instructions
how to install a compatible plugin version).
:::

### Enable dltHub Free and Paid features

:::info
The full [dltHub feature surface](../intro.md#tiers--licensing) (profiles, the `dlthub` CLI host, managed-platform commands) is enabled by switching your project into **Workspace mode**. The simplest way to do that is:

```sh
dlthub init
```

This scaffolds a fresh dltHub workspace — it creates the `.dlt/.workspace` marker file (the toggle that activates the extended CLI surface), plus `config.toml`, `secrets.toml`, `.gitignore`, and a `pyproject.toml` (or `requirements.txt` if `uv` isn't on `PATH`).

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

:::

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


## Licensing

To access dltHub’s paid features, such as Iceberg support or Python-based transformations, you need a dltHub Software License. [Contact us](https://info.dlthub.com/waiting-list) to purchase one or request a trial.

#### Install your license

If you've received your license from us, you can install it in one of two ways:

In the `secrets.toml` file:
```toml
license = "your-dlthub-license-key"
```

As an environment variable:
```sh
export DLT_LICENSE_KEY="your-dlthub-license-key"
```

#### Features requiring a license

- [@dlt.hub.transformation](../features/transformations/index.md) - a powerful Python decorator to build transformation pipelines and notebooks
- [dbt transformations](../features/transformations/dbt-transformations.md) - a staging layer for data transformations, combining a local cache with schema enforcement, debugging tools, and integration with existing data workflows.
- [Iceberg support](../ecosystem/iceberg.md).
- [Data Checks](../features/quality/data-quality.md).
- [MSSQL Change Tracking source](../ecosystem/ms-sql.md).

Please also review our [End User License Agreement](../EULA.md).
