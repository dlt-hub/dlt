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
pip install dlt[workspace]
```


This comes with the dlthub package. To use the features of dlthub, please install a valid license before proceeding, as described under [licensing](#self-licensing).


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



## Self-licensing


You can self-issue an anonymous 30-day trial license to test dlthub features and use it for development, eductation, and ci ops. The self-issued licenses are not granted for "Production Use" and cannot be transfered to other machines/workspaces. 


### Choose a scope and issue a license:

```sh
dlt license issue <scope>
```

#### Known scopes:


* `*`: All features 
* `dlthub`: All dltHub features 
* `dlthub.dbt_generator`: [Generate dbt packages from dlt pipelines](../features/transformations/dbt-transformations)
* `dlthub.sources.mssql`: [Change tracking for MSSQL](../ecosystem/ms-sql)
* `dlthub.project`: [Declarative yaml interface for dlt](../features/project/)
* `dlthub.transformation`: [Python-first query-agnostic data transformations](../features/transformations/)
* `dlthub.destinations.iceberg`: [Iceberg destination with full catalog support](../ecosystem/iceberg)
* `dlthub.destinations.snowflake_plus`: [Snowflake iceberg extension with Open Catalog](../ecosystem/snowflake_plus)
* `dlthub.runner`: Production pipeline runner and orchestrator support


Find your installed licenses with:
```sh
dlt license info
```




