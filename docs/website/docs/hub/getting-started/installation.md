---
title: Installation
description: Installation information for dlt+
---

:::info Supported Python versions

dlt+ currently supports Python versions 3.9-3.12.

:::

## Quickstart

To install the `dlthub` package, run:

```sh
pip install dlthub
```

Please install a valid license before proceeding, as described under [licensing](#licensing).

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

### Install dlt+

You can now install dlt+ in your virtual environment by running:

```sh
# install the newest dlt version or upgrade the existing version to the newest one
uv pip install -U dlthub
```

Please install a valid license before proceeding, as described under [licensing](#licensing).

## Licensing

Once you have a valid license, you can make it available to dlt+ using one of the following methods:

1. **Environment variable**: set the license key as an environment variable:

```sh
export RUNTIME__LICENSE="eyJhbGciOiJSUz...vKSjbEc==="
```

2. **Secrets file**: add the license key to a `secrets.toml` file. You can use either the project-level `secrets.toml` (located in `./.dlt/secrets.toml`) or the global one (located in `~/.dlt/secrets.toml`):

```toml
[runtime]
license="eyJhbGciOiJSUz...vKSjbEc==="
```

3. **`dlt.yml`**: add the license key directly in the [project manifest file](../features/project) referencing a user-defined environment variable:

```yaml
runtime:
  license: { env.MY_ENV_CONTAINING_LICENSE_KEY }
```

You can verify that the license was installed correctly and is valid by running:

```sh
$ dlt license show
```

Our license terms can be found [here](EULA).
