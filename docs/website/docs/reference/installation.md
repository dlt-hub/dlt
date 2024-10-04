---
title: Installation
description: How to install dlt
keywords: [installation, environment, pip install]
---

# Installation

## Setting up your environment

### 1. Make sure you are using **Python 3.8-3.12** and have `pip` installed

```sh
python --version
pip --version
```

If you have a different Python version installed or are missing pip, follow the instructions below to update your Python version and/or install `pip`.

<Tabs values={[{"label": "Ubuntu", "value": "ubuntu"}, {"label": "macOS", "value": "macos"}, {"label": "Windows", "value": "windows"}]}  groupId="operating-systems" defaultValue="ubuntu">
  <TabItem value="ubuntu">

You can install Python 3.10 with `apt`.

```sh
sudo apt update
sudo apt install python3.10
sudo apt install python3.10-venv
```

  </TabItem>
  <TabItem value="macos">

On macOS, you can use [Homebrew](https://brew.sh) to install Python 3.10.

```sh
brew update
brew install python@3.10
```

  </TabItem>
  <TabItem value="windows">

After installing [Python 3.10 (64-bit version) for Windows](https://www.python.org/downloads/windows/), you can install `pip`.

```sh
C:\> pip3 install -U pip
```

  </TabItem>
</Tabs>

### 2. Set up and activate a virtual environment for your Python project

We recommend working within a [virtual environment](https://docs.python.org/3/library/venv.html) when creating Python projects.
This way, all the dependencies for your current project will be isolated from packages in other projects.

<Tabs values={[{"label": "Ubuntu", "value": "ubuntu"}, {"label": "macOS", "value": "macos"}, {"label": "Windows", "value": "windows"}]}  groupId="operating-systems" defaultValue="ubuntu">

  <TabItem value="ubuntu">

Create a new virtual environment in your working folder. This will create an `./env` directory where your virtual environment will be stored:

```sh
python -m venv ./env
```

Activate the virtual environment:

```sh
source ./env/bin/activate
```

  </TabItem>
  <TabItem value="macos">

Create a new virtual environment in your working folder. This will create an `./env` directory where your virtual environment will be stored:

```sh
python -m venv ./env
```

Activate the virtual environment:

```sh
source ./env/bin/activate
```

  </TabItem>
  <TabItem value="windows">

Create a new virtual environment in your working folder. This will create an `./env` directory where your virtual environment will be stored:

```bat
C:\> python -m venv ./env
```

Activate the virtual environment:

```bat
C:\> .\env\Scripts\activate
```

  </TabItem>
</Tabs>

### 3. Install `dlt` library

You can now install `dlt` in your virtual environment by running:

```sh
# install the newest dlt version or upgrade the existing version to the newest one
pip install -U dlt
```

Other installation examples:
```sh
# install dlt with support for duckdb
pip install "dlt[duckdb]"
# install dlt version smaller than 0.5.0
pip install "dlt<0.5.0"
```

### 3.1. Install dlt via Pixi or Conda

Install dlt using `pixi`:

```sh
pixi add dlt
```

Install dlt using `conda`:

```sh
conda install -c conda-forge dlt
```

### 4. Done!

You are now ready to build your first pipeline with `dlt`. Check out these tutorials to get started:

- [Load data from a REST API](../tutorial/rest-api)
- [Load data from a SQL database](../tutorial/sql-database)
- [Load data from a cloud storage or a file system](../tutorial/filesystem)

Or read a more detailed tutorial on how to build a [custom data pipeline with dlt](../tutorial/load-data-from-an-api.md).

