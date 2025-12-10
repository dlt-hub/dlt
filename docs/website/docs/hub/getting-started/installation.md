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
This will install `dlt` with two additional extras:
* `dlthub` which enables features that require a [license](#self-licensing)
* `dlt-runtime` which enables access to [dltHub Runtime](../runtime/overview.md)

When working with locally you'll need several dependencies like `duckdb`, `marimo`, `pyarrow` or `fastmcp`. You can install them all with:
```sh
uv pip install "dlt[workspace]"
```

If you need to install `uv` (a modern package manager), [please refer to the next section](#configuration-of-the-python-environment).

### Upgrade existing installation

To upgrade just the `hub` extra without upgrading `dlt` itself run:
```sh
uv pip install -U "dlt[hub]==1.20.0"
```
This will keep current `1.20.0` `dlt` and upgrade `dlthub` and `dlt-runtime` to their newest matching versions.

:::tip
Note that particular `dlt` version expects `dlthub` and `dlt-runtime` versions in a matching range. For example: `1.20.x` versions expects
`0.20.x` version of a plugin. This is enforced via dependencies in `hub` extra and at import time. Installing plugin directly will not affect
installed `dlt` version to prevent unwanted upgrades. For example if you run:
```sh
uv pip install dlthub
```
and it downloads `0.21.0` version of a plugin, `dlt` `1.20.0` will still be there but it will report a wrong plugin version on import (with instructions
how to install valid plugin version).
:::

### Enable dltHub Free and Paid features

:::info
The most recent [dltHub features](../intro.md#tiers--licensing) like profiles and runtime access are hidden behind a feature flag,
which means you need to manually enable them before use.

To activate these features, create an empty `.dlt/.workspace` file in your project directory; this tells `dlt` to switch from the classic project mode to the Workspace mode.

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

To access dltHub’s paid features, such as Iceberg support or Python-based transformations, you need a dltHub Software License.

1. [Contact us](https://info.dlthub.com/waiting-list) if you want to purchase a license or get a trial license with unlimited use.
2. Issue a [limited trial license](#self-licensing) yourself.


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

#### Features requiring a license:

- [@dlt.hub.transformation](../features/transformations/index.md) - a powerful Python decorator to build transformation pipelines and notebooks
- [dbt transformations](../features/transformations/dbt-transformations.md) - a staging layer for data transformations, combining a local cache with schema enforcement, debugging tools, and integration with existing data workflows.
- [Iceberg support](../ecosystem/iceberg.md).
- [Data Checks](../features/quality/data-quality.md).
- [MSSQL Change Tracking source](../ecosystem/ms-sql.md).

For more information about the feature scopes, see [Scopes](#scopes).
Please, also review our [End User License Agreement](../EULA.md)

### Self-licensing

You can self-issue an anonymous 30-day trial license to explore dltHub’s paid features.
This trial license is intended for development, education, and CI operations only. Self-issued licenses are bound to the specific machine on which they were created. They cannot be transferred or reused on other machines, workspaces, or environments.

See the [Special Terms](../EULA.md#specific-terms-for-the-self-issued-trial-license-self-issued-trial-terms) in our EULA for more details.

#### Issue a Trial License

Choose a scope for the feature you want to test, then issue a license with:
```sh
dlt license issue <scope>
```

for example:
```sh
dlt license issue dlthub.transformation
```
This will do the following:
* Issue a new license (or merge with existing scopes) for the [transformations](../features/transformations/index.md) feature.
* Print your license key in the CLI output.
* Put the license key into your `toml` file.

#### Scopes

Display available scopes by running the following command:

```sh
dlt license scopes
```

You can self-issue multiple licenses; newly issued licenses will automatically include previously granted features.

To view your installed licenses:
```sh
dlt license info
```
