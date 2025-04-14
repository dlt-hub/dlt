---
title: Advanced Project tutorial
description: Using the dlt+ cli commands to create and manage dlt+ Project
keywords: [command line interface, cli, dlt init, dlt+, project]
---

import Link from '../../_plus_admonition.md';

<Link/>

In this tutorial, you will explore advanced features of a dlt+ project. By the end, you will be able to:
* Package a dlt+ project for distribution to other teams or stakeholders within your organization

## Prerequisites

Before you begin, ensure the following requirements are met:

- dlt+ is installed and set up according to the [installation guide](./installation.md)
- You are familiar with the [core concepts of dlt](../../reference/explainers/how-dlt-works.md)
- You have completed the [basic project tutorial](./tutorial.md)

Additionally, install the required Python packages:

```sh
pip install pandas numpy pyarrow streamlit dlt[duckdb] uv
```



## Packaging a project

`dlt+` provides tools to help you package a project for distribution.  
This makes your project installable via `pip` and easier to share across your organization.

To create a packaged project, add the `--package` option when initializing:

```sh
dlt project init arrow duckdb --package my_dlt_project
```

This creates the same basic project as in the tutorial, but places it inside a module named `my_dlt_project`, and includes a basic `pyproject.toml` file following PEP standards. 
You’ll also get a default `__init__.py` file to make the package usable after installation:

```sh
.
├── my_dlt_project/       # Your project module
│   ├── __init__.py       # Package entry point
│   ├── dlt.yml           # dlt+ project manifest
│   └── ...               # Other project files
├── .gitignore
└── pyproject.toml        # the main project manifest
```

Your `dlt.yml` works exactly the same as in non-packaged projects. 
The key difference is the module structure and the presence of the `pyproject.toml` file. 
The file includes a special entry point setting to let dlt+ discover your project:

```toml
[project.entry-points.dlt_package]
dlt-project = "my_project"
```

You can still run the pipeline as usual with the cli commands from the root folder:

```sh
dlt pipeline my_pipeline run
```

If you open the `__init__.py` file inside your project module, you'll see the full interface that users of your package will interact with.
This interface is very similar to the `current` interface used in flat (non-packaged) projects. The main difference is that it automatically uses the `access` profile by default.
You can customize the `__init__.py` file to your project's needs.

### Using the packaged project

To demonstrate how your packaged project can be used, let's simulate a real-world scenario where a data scientist installs and runs your project in a separate Python environment.
In this example, we'll use the **uv** package manager, but the same steps apply when using **poetry** or **pip**.
Assume your packaged `dlt+` project is located at: `/Volumes/my_drive/my_folder/pyproject.toml`. 
Navigate to a new directory and initialize your project:

```sh
uv init
```

Install your packaged project directly from the local path:

```sh
uv pip install /Volumes/my_drive/my_folder
```

Your dlt+ project is now available for use in this environment.
Now, create a new Python file named `test_project.py` and use your packaged project like this:

```py
# import the packaged project
from my_dlt_project import current

if __name__ == "__main__":
    # should print "access" as defined in your dlt package
    print(my_dlt_project.config().current_profile)
    # Run the pipeline from the packaged project
    my_dlt_project.runner().run_pipeline("my_pipeline")
```

Run the script inside the uv virtual environment:

```sh
uv run python test_project.py
```


:::info
In a real-world setup, a data scientist wouldn't install the package from a local path.
Instead, it would typically come from a private PyPI repository or a Git URL.
:::


## Next steps