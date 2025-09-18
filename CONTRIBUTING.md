# Contributing to dlt

Thank you for considering contributing to **dlt**! We appreciate your help in making dlt better. This document will guide you through the process of contributing to the project.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Submitting Changes](#submitting-changes)
3. [Adding or Updating Core Dependencies](#adding-or-updating-core-dependencies)
4. [Linting](#linting)
5. [Testing](#testing)
6. [Local Development](#local-development)
7. [Publishing (Maintainers Only)](#publishing-maintainers-only)
8. [Resources](#resources)

## Before You Begin

- **Proposing significant changes or enhancements**: If you're considering major changes, please [submit an issue](https://github.com/dlt-hub/dlt/issues/new/choose) first. This ensures your efforts align with the project's direction and prevents you from investing time in a feature that may not be merged. Please note:
   - 📣 **New destinations are unlikely to be merged** due to high maintenance costs (though we are happy to improve the SQLAlchemy destination to support more dialects).
   - Significant changes require tests and documentation. Writing tests will often be more time-consuming than writing the code.
   - There's probably already an [issue](https://github.com/dlt-hub/dlt/issues) for it—if so, feel free to implement it.

- **Small improvements**: We’re always happy to receive improvements if they are tested and documented.
   - Examples: additional auth methods for destinations, optimizations, or more options.
   - Quality-of-life improvements—better log messages, improved exceptions, fixing inconsistent behaviors.
  
- **Fixing bugs**:
  - Check existing issues first: search [open issues](https://github.com/dlt-hub/dlt/issues) to see if the bug has already been reported.
    - If not reported, [create a new issue](https://github.com/dlt-hub/dlt/issues/new/choose). You're welcome to fix it and submit a pull request with your solution—thank you!
    - If the bug is already reported, please comment on that issue to let others know you're working on it. This avoids duplicate efforts.

## Getting Started

1. Fork the `dlt` repository and clone it to your local machine.
2. Install `uv` with `make install-uv` (or follow the [official instructions](https://docs.astral.sh/uv/getting-started/installation/)).
3. Run `make dev` to install all dependencies, including development ones.
4. Activate your virtual environment with `make shell` and start working, or prepend all commands with `uv run` to run them within the uv environment. `uv run` is encouraged because it automatically keeps your project dependencies up to date.

## Submitting Changes

When you're ready to contribute, follow these steps:

1. Create an issue describing the feature, bug fix, or improvement you'd like to make.
2. Create a new branch in your forked repository for your changes.
3. Write your code and tests.
4. Lint your code by running `make lint` and test common modules with `make test-common`.
5. If you're working on destination code, contact us to get access to test destinations.
6. If you’ve added, removed, or updated dependencies in `pyproject.toml`, make sure `uv.lock` is up to date by running `uv lock`.  
   - If you merge upstream changes from the **devel** branch and get a conflict on the lockfile, it’s best to keep the **devel** version and re-run `uv lock` to re-apply your changes.
7. Create a pull request targeting the **devel** branch of the main repository.

**Note:** In some special cases, you’ll need us to create a branch in this repository (not in your fork). See below.

### Active Branches

- **devel** (default GitHub branch): Used to prepare the next release of `dlt`. We accept all regular contributions here (including most bug fixes).  
- **master**: Used for hotfixes (including documentation) that must be released outside of the normal schedule.  
- On release day, **devel** is merged into **master**. All releases of `dlt` are made only from **master**.

### Branch Naming Rules

To ensure that our git history clearly explains what was changed by which branch or PR, we use the following naming convention (all lowercase, with dashes, no underscores):

```sh
{category}/{ticket-id}-description-of-the-branch
# example:
feat/4922-add-avro-support
````

#### Branch Categories

* **feat**: A new feature (ticket required).
* **fix**: A bug fix (ticket required).
* **exp**: An experiment (ticket encouraged). May later become a `feat`.
* **test**: Related to tests (ticket encouraged).
* **docs**: Documentation changes (ticket optional).
* **keep**: Branches we want to keep and revisit later (ticket encouraged).

#### Ticket Numbers

We encourage attaching your branches to a ticket. If none exists, create one and explain what you’re doing.

* For `feat` and `fix` branches, tickets are **mandatory**.
* For `exp` and `test` branches, tickets are **encouraged**.
* For `docs` branches, tickets are **optional**.

### Submitting a Hotfix

We occasionally fix critical bugs and release `dlt` outside of schedule. Follow the regular procedure but open your PR against the **master** branch. Please ping us on Slack if you do this.

### Submitting Changes Requiring Full CI Credentials

Our CI runs tests for contributions from forks. By default, only tests that do not require credentials are run. Full CI tests may be enabled with labels:

* `ci from fork`: Enables CI credentials in PRs from forks and runs associated tests.
* `ci full`: Runs all tests (by default only essential destination tests are run).

Labels are assigned by the core team. If you need CI credentials for local tests, contact us on Slack.

## Deprecation Guidelines

We introduce breaking changes only in major versions. Meanwhile, we maintain backward compatibility and deprecate features.

**Example:**
The `complex` type was renamed to `json` in a minor version, with backward compatibility:

* `complex` data type is still allowed in schema definitions.
* `migrate_complex_types` is used to migrate schemas and handle `columns` hints at runtime.
* The `warnings` Python module and `Dlt100DeprecationWarning` category are used to generate warnings with full deprecation info.

**What counts as a breaking change:**

* A change in a well-documented and common behavior that breaks user code.
* A change in undocumented behavior that we know is being used.
* We do **not** consider changes that only define previously undefined edge cases. Still, if possible, backward compatibility should be maintained.

**Mechanisms to maintain backward compatibility:**

* Schemas/state files have built-in migration methods (`engine_version`).
* Storages (extract/normalize/load) have versioned layouts and can be upgraded or wiped out if the version changes.
* `DltDeprecationWarning` and its variants provide automatic deprecation info and removal timelines.
* The `deprecated` decorator can be applied to classes, functions, and overloads to generate runtime and type-checking warnings (PEP 702).
* Backward compatibility must be tested—there are many such tests in our codebase.
* We have end-to-end tests in `tests_dlt_versions.py` that create pipelines with old `dlt` versions (starting with `0.3.x`) and then upgrade and test them.

Please review the `warnings.py` module to see how deprecation warnings and decorators are used.

## Adding or Updating Core Dependencies

Our goal is to maintain stability and compatibility across all environments. Please consider the following guidelines carefully when proposing dependency updates.

### Updating Guidelines

1. **Critical updates only**:
   Major or minor version updates should only be made if there are critical security vulnerabilities or issues affecting system integrity.

2. **Using the `>=` operator**:
   Always use the `>=` operator with version minima. This keeps compatibility with older setups while avoiding unsolvable conflicts.

   **Example:**
   If the project currently uses `example-package==1.2.3`, and a security update is released as `1.2.4`, instead of locking to `example-package==1.2.4`, use:

   ```toml
   example-package>=1.2.3,<2.0.0
   ```

   This permits the security update while preventing unintended upgrades to a breaking major version.

   Maintaining minimum versions also prevents cases where dependencies cannot be resolved.

## Linting

`dlt` uses `mypy` and `flake8` (with several plugins) for linting.

## Testing

`dlt` uses `pytest` for testing.

### Common Components

To test components that don’t require external resources, run:

```sh
make test-common
```

### Local Destinations

To test local destinations (`duckdb` and `postgres`), run:

```sh
make test-load-local
```

### External Destinations

To test external destinations, run:

```sh
make test
```

You’ll need access to:

1. A `BigQuery` project
2. A `Redshift` cluster
3. A `Postgres` instance (see [docker-compose.yml](tests/load/postgres/docker-compose.yml) for setup).

```sh
cd tests/load/postgres/
docker-compose up --build -d
```

See `tests/.example.env` for expected environment variables. Copy it to `tests/.env` and configure it as you would configure your dlt pipeline.

We can provide access to these resources if you’d like to test locally.

## Local Development

Use Python 3.9 for development, as it is the lowest supported version. You can select (and download if necessary) the version with:

```sh
uv venv --python 3.11.6
```

See the [uv docs on Python versions](https://docs.astral.sh/uv/concepts/python-versions/#managed-and-system-python-installations).

## Publishing (Maintainers Only)

This section is intended for project maintainers with permissions to manage versioning and releases. Contributors can skip this section.

First, review how we [version the library](README.md#adding-as-dependency).

The source of truth for the current version is `pyproject.toml`, managed with `uv`.

### Regular Release

1. Check out the **devel** branch.
2. Bump the version with `uv version --bump patch` (or `minor`/`major`).
3. Run `make build-library` to apply changes.
4. Create a new branch and PR targeting **devel**, then merge it.

To publish:

1. Merge **devel** into **master** with a ❗ **merge commit** (not squash).
2. Ensure **master** has the latest passing code.
3. Verify the version with `uv version`.
4. Obtain a PyPI access token.
5. Run `make publish-library` and provide the token.
6. Create a GitHub release using the version and git tag.

### Hotfix Release

1. Check out **master**.
2. Bump the patch version with `uv version --bump patch`.
3. Run `make build-library`.
4. Create a new branch and PR targeting **master**, then merge it.
5. Re-submit the same fix to **devel**.

### Pre-release

Occasionally, we may release an alpha version from a branch:

1. Check out **devel**.
2. Manually update the alpha version in `pyproject.toml` and run `uv sync`.
3. Run `make build-library`.
4. Create a branch, open a PR to **devel**, and merge it.

## Resources

* [dlt Docs](https://dlthub.com/docs)
* [uv Documentation](https://docs.astral.sh/uv/)

If you have any questions or need help, don’t hesitate to reach out. We’re here to help you succeed in contributing to `dlt`. Happy coding!

