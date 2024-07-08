---
title: Installation
description: How to install dlt
keywords: [installation, environment, pip install]
---

# Installation

## Set up environment

### Make sure you are using **Python 3.8-3.12** and have `pip` installed

```sh
python --version
pip --version
```

### If not, then please follow the instructions below to install it

<Tabs values={[{"label": "Ubuntu", "value": "ubuntu"}, {"label": "macOS", "value": "macos"}, {"label": "Windows", "value": "windows"}]}  groupId="operating-systems" defaultValue="ubuntu">
  <TabItem value="ubuntu">

You can install Python 3.10 with an `apt` command.

```sh
sudo apt update
sudo apt install python3.10
sudo apt install python3.10-venv
```

  </TabItem>
  <TabItem value="macos">

Once you have installed [Homebrew](https://brew.sh), you can install Python 3.10.

```sh
brew update
brew install python@3.10
```

  </TabItem>
  <TabItem value="windows">

You need to install [Python 3.10 (64-bit version) for Windows](https://www.python.org/downloads/windows/).
After this, you can then install `pip`.

```sh
C:\> pip3 install -U pip
```

  </TabItem>
</Tabs>

### Once Python is installed, you should create virtual environment

<Tabs values={[{"label": "Ubuntu", "value": "ubuntu"}, {"label": "macOS", "value": "macos"}, {"label": "Windows", "value": "windows"}]}  groupId="operating-systems" defaultValue="ubuntu">

  <TabItem value="ubuntu">

Create a new virtual environment by making a `./env` directory to hold it.

```sh
python -m venv ./env
```

Activate the virtual environment:

```sh
source ./env/bin/activate
```

  </TabItem>
  <TabItem value="macos">

Create a new virtual environment by making a `./env` directory to hold it.

```sh
python -m venv ./env
```

Activate the virtual environment:

```sh
source ./env/bin/activate
```

  </TabItem>
  <TabItem value="windows">

Create a new virtual environment by making a `./env` directory to hold it.

```bat
C:\> python -m venv ./env
```

Activate the virtual environment:

```bat
C:\> .\env\Scripts\activate
```

  </TabItem>
</Tabs>

## Install `dlt` library

You can install `dlt` in your virtual environment by running:

```sh
pip install -U dlt
```

## Install dlt via Pixi and Conda

Install dlt using `pixi`:

```sh
pixi add dlt
```

Install dlt using `conda`:

```sh
conda install -c conda-forge dlt
```
