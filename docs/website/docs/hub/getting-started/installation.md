---
title: Installation
description: Installation information of dlthub package
---

:::info Supported Python versions

dltHub currently supports Python versions 3.9-3.13.

:::

## Quickstart

To install the `dlt[workspace]` package, run:

```sh
pip install "dlt[workspace]"
```


This comes with the dlthub package. To use the features of dlthub, please get a valid license key before proceeding, as described under [licensing](#self-licensing).


## Setting up your environment

### Configuration of the Python environment

Check if your Python environment is configured:

```sh
python --version
pip --version
```

If you have a different Python version installed or are missing pip, follow the instructions below to update your Python version and/or install `pip`.

<Tabs values={[{"label": "Ubuntu", "value": "ubuntu"}, {"label": "macOS", "value": "macos"}, {"label": "Windows", "value": "windows"}]} groupId="operating-systems" defaultValue="ubuntu">
<TabItem value="ubuntu">

You can install Python 3.10 with `apt`.

```sh
sudo apt update
sudo apt install python3.10
pip install uv
```

  </TabItem>
  <TabItem value="macos">

On macOS, you can use [Homebrew](https://brew.sh) to install Python 3.10.

```sh
brew update
brew install python@3.10
pip install uv
```

  </TabItem>
  <TabItem value="windows">

After installing [Python 3.10 (64-bit version) for Windows](https://www.python.org/downloads/windows/), you can install `pip`.

```sh
C:\> pip3 install -U pip
C:\> pip3 install uv
```

  </TabItem>
</Tabs>

### Virtual environment

We recommend working within a [virtual environment](https://docs.python.org/3/library/venv.html) when creating Python projects.
This way, all the dependencies for your current project will be isolated from packages in other projects.

<Tabs values={[{"label": "Ubuntu", "value": "ubuntu"}, {"label": "macOS", "value": "macos"}, {"label": "Windows", "value": "windows"}]} groupId="operating-systems" defaultValue="ubuntu">

  <TabItem value="ubuntu">

Create a new virtual environment in your working folder. This will create a `./venv` directory where your virtual environment will be stored:

```sh
uv venv --python 3.10
```

Activate the virtual environment:

```sh
source .venv/bin/activate
```

  </TabItem>
  <TabItem value="macos">

Create a new virtual environment in your working folder. This will create a `./venv` directory where your virtual environment will be stored:

```sh
uv venv --python 3.10
```

Activate the virtual environment:

```sh
source .venv/bin/activate
```

  </TabItem>
  <TabItem value="windows">

Create a new virtual environment in your working folder. This will create a `./venv` directory where your virtual environment will be stored:

```bat
C:\> uv venv --python 3.10
```

Activate the virtual environment:

```bat
C:\> .\venv\Scripts\activate
```

  </TabItem>
</Tabs>

### Install dlthub

`dlthub` will be automatically installed with workspace extra:

```sh
# install the newest dlt[workspace] version or upgrade the existing version to the newest one
uv pip install -U "dlt[workspace]"
```

Please install a valid license before proceeding, as described under [licensing](#self-licensing).



## Licensing 

To access dltHub’s paid features, such as Iceberg support or Python-based transformations, you need a dltHub Software License.


When you purchase a paid dltHub offering, the required license will be issued and managed automatically for your account.

You can also manually configure a license for local development or CI environments as shown below.


#### Applying your license

You can provide your license key in one of two ways:

In the `secrets.toml` file:
```toml
license = "your-dlthub-license-key"
```

As an environment variable
```bash
export DLT_LICENSE_KEY="your-dlthub-license-key"
```

#### Features requiring a license:

- [@dlt.hub.transformation](../features/transformations/index.md) - powerful Python decorator to build transformation pipelines and notebooks
- [dbt transformations](../features/transformations/dbt-transformations.md): a staging layer for data transformations, combining a local cache with schema enforcement, debugging tools, and integration with existing data workflows.
- [Iceberg support](../ecosystem/iceberg.md)
- [Secure data access and sharing](../features/data-access.md)
- [AI workflows](../features/ai.md): agents to augment your data engineering team.

For more information about the feature scopes, see [Scopes](#scopes).
Please also review our End User License Agreement [(EULA)](../EULA.md)

### Self-licensing

You can self-issue an anonymous 30-day trial license to explore dltHub’s paid features.
This trial license is intended for development, education, and CI operations only. Self-issued licenses are bound to the specific machine on which they were created. They cannot be transferred or reused on other machines, workspaces, or environments.

See the [Special Terms](../EULA.md#specific-terms-for-the-self-issued-trial-license-self-issued-trial-terms) in our EULA for more details.

#### Issue a Trial License
Choose a scope for the feature you want to test, then issue a license with:
```sh
dlt license issue <scope>
```

#### Scopes:

* `*`: All features 
* `dlthub`: All dltHub features 
* `dlthub.dbt_generator`: [Generate dbt packages from dlt pipelines](../features/transformations/dbt-transformations)
* `dlthub.sources.mssql`: [Change tracking for MSSQL](../ecosystem/ms-sql)
* `dlthub.project`: [Declarative yaml interface for dlt](../features/project/)
* `dlthub.transformation`: [Python-first query-agnostic data transformations](../features/transformations/)
* `dlthub.destinations.iceberg`: [Iceberg destination with full catalog support](../ecosystem/iceberg)
* `dlthub.destinations.snowflake_plus`: [Snowflake iceberg extension with Open Catalog](../ecosystem/snowflake_plus)
* `dlthub.runner`: Production pipeline runner and orchestrator support

You can self-issue multiple licenses; newly issued licenses will automatically include previously granted features.

To view your installed licenses:
```sh
dlt license info
```




