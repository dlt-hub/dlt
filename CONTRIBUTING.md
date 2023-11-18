# Contributing to dlt

Thank you for considering contributing to **dlt**! We appreciate your help in making dlt better. This document will guide you through the process of contributing to the project.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Submitting Changes](#submitting-changes)
3. [Linting](#linting)
4. [Testing](#testing)
5. [Local Development](#local-development)
6. [Publishing (Maintainers Only)](#publishing-maintainers-only)
7. [Resources](#resources)

## Getting Started

To get started, follow these steps:

1. Fork the `dlt` repository and clone it to your local machine.
2. Install `poetry` with `make install-poetry` (or follow the [official instructions](https://python-poetry.org/docs/#installation)).
3. Run `make dev` to install all dependencies including dev ones.
4. Start working in the `poetry` shell by executing `poetry shell`.

## Submitting Changes

When you're ready to contribute, follow these steps:

1. Create an issue describing the feature, bug fix, or improvement you'd like to make.
2. Create a new branch in your forked repository for your changes.
3. Write your code and tests.
4. Lint your code by running `make lint` and test common modules with `make test-common`.
5. If you're working on destination code, contact us to get access to test destinations.
6. Create a pull request targeting the `devel` branch of the main repository.

**Note:** for some special cases, you'd need to contact us to create a branch in this repository (not fork). See below.
### Testing with Github Actions
We enable our CI to run tests for contributions from forks. All the tests are run, but not all destinations are available due to credentials. Currently
only the `duckdb` and `postgres` are available to forks.

## Submitting Changes Requiring Full CI Credentials.

In case you submit a new destination or make changes to a destination that require credentials (so Bigquery, Snowflake, buckets etc.) you **should contact us so we can add you as contributor**. Then you should make a PR directly to the `dlt` repo.

## Linting

`dlt` uses `mypy` and `flake8` with several plugins for linting.

## Testing

dlt uses `pytest` for testing.

### Common Components

To test common components (which don't require external resources), run `make test-common`.

### Local Destinations

To test local destinations (`duckdb` and `postgres`), run `make test-load-local`.

### External Destinations

To test external destinations use `make test`. You will need following external resources

1. `BigQuery` project
2. `Redshift` cluster
3. `Postgres` instance. You can find a docker compose for postgres instance [here](tests/load/postgres/docker-compose.yml). When run the instance is configured to work with the tests.

```shell
cd tests/load/postgres/
docker-compose up --build -d
```

See `tests/.example.env` for the expected environment variables and command line example to run the tests. Then create `tests/.env` from it. You configure the tests as you would configure the dlt pipeline.
We'll provide you with access to the resources above if you wish to test locally.

## Local Development

Use Python 3.8 for development, as it's the lowest supported version for `dlt`. You'll need `distutils` and `venv`. You may also use `pyenv`, as suggested by [poetry](https://python-poetry.org/docs/managing-environments/).

# Publishing (Maintainers Only)

This section is intended for project maintainers who have the necessary permissions to manage the project's versioning and publish new releases. If you're a contributor, you can skip this section.

## Project Versioning

`dlt` follows the semantic versioning with the [`MAJOR.MINOR.PATCH`](https://peps.python.org/pep-0440/#semantic-versioning) pattern. Currently, we are using **pre-release versioning** with the major version being 0.

- `minor` version change means breaking changes
- `patch` version change means new features that should be backward compatible
- any suffix change, e.g., `post10` -> `post11`, is considered a patch

Before publishing a new release, make sure to bump the project's version accordingly:

1. Modify `pyproject.toml` to add a `post` label or increase post release number ie: `version = "0.2.6.post1"`
2. Run `make build-library` to apply the changes to the project.
3. The source of the version is `pyproject.toml`, and we use `poetry` to manage it.

For pre-release please replace step (1) with:
1. Make sure you are not bumping post-release version. There are reports of `poetry` not working in that case.
2. Use `poetry version prerelease` to bump the pre-release version.

## Publishing to PyPI

Once the version has been bumped, follow these steps to publish the new release to PyPI:

1. Ensure that you are on the `devel` branch and have the latest code that has passed all tests on CI.
2. Verify the current version with `poetry version`.
3. Obtain a PyPI access token and configure it with `poetry config pypi-token.pypi your-api-token`.
4. Run `make publish-library` to publish the new version.
5. Create a release on GitHub, using the version and git tag as the release name.

## Resources

- [dlt Docs](https://dlthub.com/docs)
- [Poetry Documentation](https://python-poetry.org/docs/)

If you have any questions or need help, don't hesitate to reach out to us. We're here to help you succeed in contributing to `dlt`. Happy coding!
