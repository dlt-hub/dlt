# Contributing to dlt

Thank you for considering contributing to **dlt**! We appreciate your help in making dlt better. This document will guide you through the process of contributing to the project.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Submitting Changes](#submitting-changes)
3. [Adding or Updating Core Dependencies](#adding-or-updating-core-dependencies)
4. [Formatting and Linting](#formatting-and-linting)
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
4. Activate your virtual environment (run `source .venv/bin/activate` if you're on Linux/macOS) and start working, or prepend all commands with `uv run` to run them within the uv environment. `uv run` is encouraged, it automatically keeps your project dependencies up to date.

## Submitting Changes

When you're ready to contribute, follow these steps:

1. Create an issue describing the feature, bug fix, or improvement you'd like to make.
2. Create a new branch in your forked repository for your changes.
3. Write your code and tests.
4. Lint your code by running `make lint` and test common modules with `make test-common`.
5. If you're working on destination code, contact us to get access to test destinations.
6. If you’ve added, removed, or updated dependencies in `pyproject.toml`, make sure `uv.lock` is up to date by running `uv lock`.  
   - If you merge upstream changes from the **devel** branch and get a conflict on the lockfile, it’s best to keep the **devel** version and re-run `uv lock` to re-apply your changes.
7. Create a pull request targeting the **devel** branch of the main repository. Please link the ticket that describes what you are doing in the PR, or write a PR comment that makes it clear to us and other users without prior knowledge what you are doing here.

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
```

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

Our goal is to maintain stability and compatibility across all environments. Please consider the following guidelines carefully when proposing dependency updates. Our CI runs the tests for the common modules as well as some smoke tests on DuckDB on the lowest allowed version and the newest allowed version additionally to the versions pinned in `uv.lock` to try to catch problems in dependent packages.

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

## Formatting and Linting

`dlt` uses `mypy` and `flake8` (with several plugins) for linting. You can run the linter locally with `make lint`. We also run a code formatter with `black` which you can run with `make format`. The lint step will also ensure that the code is formatted correctly. It is good practice to run `make format && make lint` before every commit.

## Testing

`dlt` uses `pytest` for testing. 

### CI Setup

You can view our GitHub Actions setup in `.github/workflows` to see which tests are run with which dependencies  / extras installed, and which platforms and python versions are used for linting and testing. The main entry point is `.github/workflows/main.yml` which orchestrates all other workflows. Certain dependencies exist, for example no tests will be run if the linter reports problems. Some workflows use test matrixes to test several destinations or run tests on various operating systems and with various python versions or dependency resolution strategies. 

### Common Components

To test components that don’t require external resources, run:

```sh
make test-common
```

You can see the GitHub actions setup for the common tests, including DuckDb smoke-tests in `.github/workflows/test_common.yml`.

### Local Destinations

Several destinations can be tested locally. `duckdb` does not require a running database service, while `postgres`, `clickhouse` and others provide Docker containers that can be launched locally for testing. To test these destinations:

1. Install Docker on your machine
2. Launch all test containers with `make start-test-containers`, or launch just the specific service you need
3. Copy the local dev credentials from `tests/.dlt/dev.secrets.toml` to `tests/.dlt/secrets.toml`
4. Now you can run your tests - for example, to run all Postgres load tests, use `pytest tests/load -k postgres`

To test the two primary local destinations (`duckdb` and `postgres`), start your test containers and run:

```sh
make test-load-local
```

You can see the GitHub actions setup for local destinations in `.github/workflows/test_destinations_local.yml`.

### External Destinations

To run all tests including all external destinations run:

```sh
make test
```

For this to work you will need credentials to all destinations supported by dlt in scope of the tests in `tests/.dlt/secrets.tom`. Note that these tests will take a long time to run. See below how to develop for a particular destination efficiently.

We can provide access to these resources if you’d like to test locally.

You can see the GitHub actions setup for remote destinations in `.github/workflows/test_destinations_remote.yml`.


### E2E Tests

`dlt` ships with the Workspace Dashboard (https://dlthub.com/docs/general-usage/dashboard). To ensure that the dashboard works correctly in the Browser on all Platforms, we have e2e tests with Playwright as part of our test suite. To run the e2e tests locally, please:

1. Install all dependencies with `make dev`
2. Install the dashboard testing dependencies with `uv sync --group dashboard-tests`
3. Install playwright dependencies with `playwright install`
4. Start the dashboard in silent mode from one terminal window: `make start-dlt-dashboard-e2e`
5. Start the dashboard e2e test in another windows in headed mode so you can see what is going on: `make test-e2e-dashboard-headed`

You can see the GitHub actions setup for the dashboard unit and e2e tests in `.github/workflows/test_tools_dashboard.yml`.


### Testing tips and tricks

When developing, you generally want to avoid catching test errors only in CI, as you'll have to commit and push your code and wait a while to get a report about what works and what doesn't. Here are some strategies you can use to get fast local test results to rule out major problems in your code. Note that when working on internals that change how data gets loaded to destinations, sometimes there's no way around relying on CI results, since all destinations need to work with your code and running the full suite can take considerable time.

- If you're working on code in the extraction and normalizing parts, it's usually sufficient to run the common tests with `make test-common` or run specific files/folders that test those aspects. You can also run all loader tests for DuckDB with `pytest tests/load -k "duckdb"` before final submission, which will rule out many destination-related tests and complete relatively quickly.

- If you're working on code in the loader part of dlt which manages pushing data to destinations, it's best to run relevant tests against DuckDB first and then fix problems that appear in other destinations. For example, if you're working on changing the merge write_disposition, you'll likely modify `tests/load/pipeline/test_merge_disposition.py`. Get it to pass with DuckDB and Postgres locally first before testing on all other destinations or running on CI: `pytest tests/load/pipeline/test_merge_disposition.py -k "duckdb"`, `pytest tests/load/pipeline/test_merge_disposition.py -k "postgres"`

- You can also select which destination tests to run using the `ACTIVE_DESTINATIONS` and `ALL_FILESYSTEM_DRIVERS` environment variables. The former selects destinations to use, while the latter determines which buckets to use for the filesystem destination and staging destinations. For example, the command `ACTIVE_DESTINATIONS='["duckdb", "filesystem"]' ALL_FILESYSTEM_DRIVERS='["memory", "file"]' uv run pytest tests/load` will run all loader tests on DuckDB and the filesystem (in-memory filesystem and local files). You can see these environment variables being used in our workflow setup.



## Local Development

Use Python 3.9 for development, as it is the lowest supported version. You can select (and download if necessary) the version with:

```sh
uv venv --python 3.11.6
```

In rare cases you may find you will have to check your code in several Python version. See the [uv docs on Python versions](https://docs.astral.sh/uv/concepts/python-versions/#managed-and-system-python-installations).

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

