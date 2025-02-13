# dlt+ Project

A dlt+ Project is a dlt project with an opinionated structure containing a Python manifest file (`dlt.yml`) where you can define dlt entities (sources, destinations, pipelines, transformations, etc.) in a declarative fashion. The project layout is fully compatible with a standard Python package approach and may be distributed via PyPI or as a git repository. It has the following components:

1. A Python manifest of the package (`pyproject.toml`) which specifies dependencies, source files, and package build system.
2. A dlt manifest file (`dlt.yml`) which specifies data platform entities like sources, destination, pipelines, transformations etc.
3. Python modules with source code and tests. We propose a strict layout of the modules (i.e., source code is in the `sources/` folder, etc.)

Package and project manager [uv](https://docs.astral.sh/uv/) is used for packaging.  
