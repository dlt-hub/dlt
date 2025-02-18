# Project

import Link from '../../_plus_admonition.md';

<Link/>

A dlt+ Project offers developers a declarative approach for defining data workflow components: sources, destinations, pipelines, transformations, parameters, etc. It follows an opinionated structure centered around a Python manifest file `dlt.yml`, where all dlt entities are defined in an organized way. The manifest file acts like a single source of truth for data pipelines, keeping all teams aligned.

The project layout has the following components:

1. A dlt manifest file (`dlt.yml`) which specifies data platform entities like sources, destination, pipelines, transformations etc.
2. A Python manifest of the package (`pyproject.toml`) which specifies dependencies, source files, and package build system.
3. Python modules with source code and tests. We propose a strict layout of the modules (i.e., source code is in the `sources/` folder, etc.)

A general dlt+ project has the following structure:

```text
.
├── .dlt/                 # your dlt settings including profile settings
│   ├── config.toml
│   ├── dev.secrets.toml
│   └── secrets.toml
├── _storage/             # local storage for your project, excluded from git
├── sources/              # your sources, contains the code for the arrow source
│   └── arrow.py
├── .gitignore
├── requirements.txt
└── dlt.yml               # the main project manifest
```

dlt+ Project follows the standard Python package structure, allowing users to package their code and configuration as Python packages. These packages can be distributed via PyPI (private or open-source) or as a git repository. Any Python project manager can be used for packaging such as [uv](https://docs.astral.sh/uv/) or [poetry](https://python-poetry.org/).

Read more about dlt+ Project in the [project feature description](../features/projects.md)

:::note
To get started with a dlt+ Project and learn how to manage it using cli commands, check out our [tutorial](../getting-started/tutorial.md)
:::
