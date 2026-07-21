# Contributing to dlt

Thank you for considering contributing to **dlt**! We appreciate your help in making dlt better.

Maintainer review time is limited. To ensure contributors work on changes that are useful, appropriately scoped, and ready for review, please follow the contribution eligibility and approval process below. Pull requests that do not follow this process may be closed without review.

## Table of Contents

1. [Before You Begin](#before-you-begin)
2. [Getting Started](#getting-started)
3. [Submitting Changes](#submitting-changes)
4. [AI-Assisted Contributions](#ai-assisted-contributions)
5. [Active Branches](#active-branches)
6. [Branch Naming Rules](#branch-naming-rules)
7. [Commit Message Rules](#commit-message-rules)
8. [Submitting a Hotfix](#submitting-a-hotfix)
9. [Submitting Changes Requiring Full CI Credentials](#submitting-changes-requiring-full-ci-credentials)
10. [Deprecation Guidelines](#deprecation-guidelines)
11. [Adding or Updating Core Dependencies](#adding-or-updating-core-dependencies)
12. [Formatting and Linting](#formatting-and-linting)
13. [Testing](#testing)
14. [Local Development](#local-development)
15. [Publishing — Maintainers Only](#publishing-maintainers-only)
16. [Resources](#resources)

## Before You Begin

### Contribution eligibility

New and first-time contributors should submit pull requests only for issues labeled with

* [`help wanted`](https://github.com/dlt-hub/dlt/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22help%20wanted%22)
* [`good first issue`](https://github.com/dlt-hub/dlt/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22)

> **Maintainers:** always pair `good first issue` with `help wanted`. If we don't want to hand an issue out, apply neither.

A pull request for any other issue requires explicit maintainer approval before implementation begins. Approval must be recorded in the linked GitHub issue or discussion.

An issue being open does not automatically mean that the project is ready to accept an implementation. Before starting work:

1. Confirm that the issue has an eligible label or explicit maintainer approval.
2. Check that nobody else is already assigned to or actively working on it.
3. Comment on the issue with a brief description of your intended approach.
4. Wait for a maintainer to confirm that the issue is available and that your proposed approach is appropriate.

Do not submit speculative implementations while waiting for approval.

Pull requests that:

* do not link to an eligible or explicitly approved issue;
* implement an approach that was not approved;
* significantly exceed the agreed scope; or
* duplicate work already in progress

may be closed without a detailed review.

Small, self-contained corrections to documentation, spelling, or broken links may be accepted without a pre-existing issue. Maintainers may still ask you to open an issue or close the pull request if the change is subjective, broad, or requires product decisions.

Established contributors may propose work outside the eligible issue list, but significant changes must still be discussed and approved before implementation.

### Scope and ownership

Acceptance of an issue or proposed approach is not a guarantee that the resulting pull request will be merged.

Contributors are expected to:

* keep the change within the agreed scope;
* understand and be able to explain the implementation;
* write or update the required tests and documentation;
* respond to review feedback;
* revise the implementation when requested; and
* remain available to help resolve problems caused by the change.

### Proposing significant changes or enhancements

Do not begin implementing a major feature, architectural change, refactor, new public API, or significant behavior change without prior maintainer approval.

Create or find a relevant [issue](https://github.com/dlt-hub/dlt/issues) and describe:

* the problem being solved;
* the proposed behavior;
* the intended implementation approach;
* alternatives considered.

Please note:

* **New destinations are unlikely to be merged** because of their ongoing maintenance cost. We are, however, happy to consider improvements to the SQLAlchemy destination that add support for more dialects.
* Significant changes require tests and documentation.
* There may already be an issue for the requested change. Search the [existing issues](https://github.com/dlt-hub/dlt/issues) before creating a new one.
* Creating an issue does not reserve the work or authorize implementation. Wait for explicit maintainer approval.

### Small improvements

Small improvements must also be linked to an eligible issue or receive explicit maintainer approval before implementation, except for minor documentation corrections described above.
Even small changes must be tested and documented when they affect behavior.

### Fixing bugs

Before working on a bug:

1. Search the [open issues](https://github.com/dlt-hub/dlt/issues) to see whether it has already been reported.
2. If it has not been reported, [create a new issue](https://github.com/dlt-hub/dlt/issues/new/choose) with a minimal reproduction and enough information for maintainers to evaluate it.
3. If it has already been reported, comment on the existing issue rather than opening a duplicate.
4. Wait for the issue to receive an eligible label or for a maintainer to approve your proposed fix.
5. Comment with your intended approach and wait for confirmation before beginning substantial implementation work.

Please do not open a pull request immediately after reporting a new bug unless a maintainer has approved the work.

## Getting Started

After a maintainer has confirmed your contribution:

1. Fork the `dlt` repository and clone it to your local machine.
2. Install `uv` with `make install-uv` (or follow the [official instructions](https://docs.astral.sh/uv/getting-started/installation/)).
3. Run `make dev` to install all dependencies, including development ones.
4. Activate your virtual environment (run `source .venv/bin/activate` if you're on Linux/macOS) and start working, or prepend all commands with `uv run` to run them within the uv environment. `uv run` is encouraged, it automatically keeps your project dependencies up to date.

## Submitting Changes

When you are ready to contribute, follow these steps:

1. Select an issue labeled `help wanted` or `good first issue`, or obtain explicit maintainer approval for another issue.
2. Create a new branch in your fork using the required branch naming convention.
3. Keep the implementation focused on the agreed issue.
4. Write or update the required code, tests, and documentation.
5. Run `make format` and `make lint`.
6. Run `make test-common` and any additional test suites relevant to the changed components.
7. If you are working on destination code, contact us to request access to the necessary test destinations.
8. If you added, removed, or updated dependencies in `pyproject.toml`, update `uv.lock` by running `uv lock`.

   * If you merge upstream changes from the **devel** branch and encounter a lockfile conflict, keep the **devel** version of the lockfile and run `uv lock` again to apply your changes.
9. Create a pull request targeting the **devel** branch of the main repository, unless the change qualifies as a hotfix.
10. Link the issue in the pull request description. Prefer a closing reference such as `Fixes #1234` when the pull request fully resolves the issue. Explain the change in the pull request description so that maintainers and other contributors can understand it without prior knowledge of the issue.

A pull request should be ready for review when it is opened.

Maintainers may close a pull request without detailed review when it:

* was submitted without prior approval;
* does not link to an eligible issue;
* does not follow the agreed approach;
* is incomplete or speculative;
* lacks required tests or documentation;
* contains unrelated changes;
* cannot be clearly explained by its author;
* repeatedly ignores review instructions; or
* imposes a review or maintenance cost that is disproportionate to its benefit.

## AI-Assisted Contributions

AI tools may be used as development aids, but they do not replace contributor judgment, understanding, testing, or accountability.

The person submitting a contribution is fully responsible for every part of it, including:

* correctness;
* security;
* performance;
* compatibility;
* licensing and provenance;
* tests;
* documentation; and
* long-term maintainability.

Do not add AI tools as commit authors or co-authors, and do not add generated tool footers to commit messages.

### Human understanding is required

You must be able to explain the implementation in your own words and justify important design decisions. A pull request may be closed if the author cannot demonstrate sufficient understanding of the submitted change.

### Autonomous submissions are not accepted

Do not use autonomous agents or bots to select issues without human approval, open issues or pull requests.

All issue comments, pull request descriptions, and review responses must be written or carefully reviewed by the human contributor responsible for the work.

Primarily generated and unverified submissions may be closed without detailed review. Repeated submissions of this kind may result in restrictions on further participation.

## Active Branches

* **devel** is the default GitHub branch and is used to prepare the next release of `dlt`. Regular contributions, including most bug fixes, target this branch.
* **master** is used for hotfixes, including urgent documentation fixes, that must be released outside the normal schedule.
* On release day, **devel** is merged into **master**.
* All releases of `dlt` are made from **master**.

## Branch Naming Rules

To ensure that our git history clearly explains what was changed by which branch or PR, we use the following naming convention (all lowercase, with dashes, no underscores):

```sh
{category}/{ticket-id}-description-of-the-branch
# example:
feat/4922-add-avro-support
```

### Branch Categories

* **feat**: A new feature. An approved issue is required.
* **fix**: A bug fix. An approved issue is required.
* **exp**: An approved experiment. An issue and explicit maintainer approval are required.
* **test**: A test-related change. An issue is normally required.
* **docs**: A documentation change. An issue is required unless the change is a minor spelling, formatting, or broken-link correction.
* **keep**: A maintainer-approved branch that will be retained and revisited later.

### Ticket Numbers

All code changes must be linked to an eligible or explicitly approved issue.

* `feat`, `fix`, and `exp` branches require an issue number.
* `test` branches normally require an issue number.
* `docs` branches require an issue number unless they contain only a minor documentation correction.
* The issue number in the branch name must match the issue linked in the pull request.
* Note: creating an issue is not sufficient; the issue must be eligible for contribution or explicitly approved by a maintainer.

## Commit Message Rules

We use [Conventional Commits](https://www.conventionalcommits.org/). Keep messages clean — this matters most when squash-merging a PR, since GitHub hides everything after the first line in the commit list:

* `{type}:` or `{type}({scope}):`, lowercase imperative subject, no trailing period (types match the branch categories above).
* Subject line only for most commits; add a short body only for a non-obvious *why*.
* No footers (e.g. `Co-Authored-By`, "Generated with …") and no emojis.
* When squash-merging, clean the squash message down to a single subject line.

## Submitting a Hotfix

We occasionally fix critical bugs and release `dlt` outside of the normal schedule. Follow the regular procedure but open your PR against the **master** branch. Please ping us on Slack if you do this.

## Submitting Changes Requiring Full CI Credentials

Our CI runs tests for contributions from forks. By default, only tests that do not require credentials are run.

Full CI tests may be enabled with the following labels:

* `ci from fork`: Enables CI credentials in pull requests from forks and runs the associated tests.
* `ci full`: Runs all tests. By default, only essential destination tests are run.

These labels are assigned by the core team after reviewing the pull request. If you need CI credentials for local tests, contact us on Slack.

## Deprecation Guidelines

We introduce breaking changes only in major versions. Meanwhile, we maintain backward compatibility and deprecate features.

**Example:**

The `complex` type was renamed to `json` in a minor version while preserving backward compatibility:

* The `complex` data type remains valid in schema definitions.
* `migrate_complex_types` migrates schemas and handles `columns` hints at runtime.
* The Python `warnings` module and the `Dlt100DeprecationWarning` category generate warnings containing complete deprecation information.

### What counts as a breaking change

* A change in a well-documented and common behavior that breaks user code.
* A change in undocumented behavior that we know is being used.
* We do **not** consider changes that only define previously undefined edge cases. Still, if possible, backward compatibility should be maintained.

### Mechanisms for maintaining backward compatibility

* Schemas/state files have built-in migration methods (`engine_version`).
* Storages (extract/normalize/load) have versioned layouts and can be upgraded or wiped out if the version changes.
* `DltDeprecationWarning` and its variants provide automatic deprecation info and removal timelines.
* The `deprecated` decorator can be applied to classes, functions, and overloads to generate runtime and type-checking warnings (PEP 702).
* Backward compatibility must be tested—there are many such tests in our codebase.
* We have end-to-end tests in `tests_dlt_versions.py` that create pipelines with old `dlt` versions (starting with `0.3.x`) and then upgrade and test them.

Review the `warnings.py` module to understand how deprecation warnings and decorators are used.

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

   This permits compatible security updates while preventing unintended upgrades to a breaking major version.

   Maintaining minimum versions also prevents cases where dependencies cannot be resolved.

## Formatting and Linting

`dlt` uses `mypy` and `flake8` (with several plugins) for linting. You can run the linter locally with `make lint`. We also run a code formatter with `black` which you can run with `make format`. The lint step will also ensure that the code is formatted correctly. It is good practice to run `make format && make lint` before every commit.

## Testing

`dlt` uses `pytest` for testing.

### Parallel testing

Parallel testing has been introduced to allow for more time-efficient testing via `pytest-xdist`. The harnessing of parallel testing has already been done in the root Makefile of the project. You can consult the Makefile directly to see the specifics of the implementation. Summarized, the different testing suites that are described below will have an equivalent Make invocation command with a suffix `-p`, which will enable parallelized testing for the selected tests. This parallelizing strategy helps surfacing test leakages an execution-order dependencies.

How does the parallelization strategy work? `pytest-xdist` spawns python processes where all tests are collected, then the `pytest-xdist` workers will pick up any test available to be executed (in any order, from any module). By default, when running tests locally, the number of python parallel processes is set to auto. This will spawn as many processes as cpu cores the machine has. You can override this value by passing `PYTEST_XDIST_N` to the `make` invocation command with the number of desired processes. Example:

```sh
PYTEST_XDIST_N=4 make test-common-p
```

They pytest `serial` marker has been used to mark a series of tests that cannot be run in parallel. An example of tests that cannot run in parallel are the ones that require all cpu cores saturating for performance reasons. Any parallel test execution will do a second pass to run tests that have the `serial` marker to be run without parallelism.

In general, the parallel testing safety is achieved through:
- unique pipeline names (with a unique identifier in the pipeline name, usually `from dlt.common.utils import uniq_id`)
- independent test storage root folders based on `pytest-xdist` workers, called "_storage_gwX" where X is the worker number. In order to get the pytest-worker-aware test storage root, you can use `from tests.utils import get_test_storage_root`

Make sure to respect those guidelines to keep parallelization safe.

If, for any reason, you need to access the `pytest-xdist` worker id, do it with `from tests.utils import get_test_worker_id`.

### CI Setup

You can view our GitHub Actions setup in `.github/workflows` to see which tests are run with which dependencies  / extras installed, and which platforms and python versions are used for linting and testing. The main entry point is `.github/workflows/main.yml` which orchestrates all other workflows. Certain dependencies exist, for example no tests will be run if the linter reports problems. Some workflows use test matrixes to test several destinations or run tests on various operating systems and with various python versions or dependency resolution strategies. To reduce CI execution time and improve feedback cycles, parallel test execution via `pytest-xdist` has been enabled in CI. Try to run any test suite that is involved in your development work in parallel if possible, since that is how it will be run in CI. Some CI tests have been restricted the number of workers due to destination performance reasons.

### Common Components

To test components that do not require external resources, run:

```sh
make test-common
```

or, in parallel:

```sh
make test-common-p
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

or, in parallel:

```sh
make test-load-local-p
```

You can see the GitHub actions setup for local destinations in `.github/workflows/test_destinations_local.yml`.

### External Destinations

To run all tests, including tests for external destinations, use:

```sh
make test
```

For this to work you will need credentials to all destinations supported by dlt in scope of the tests in `tests/.dlt/secrets.toml`. Note that these tests will take a long time to run. See below how to develop for a particular destination efficiently.

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

Use Python 3.10 for development, as it is the lowest supported version. You can select (and download if necessary) the version with:

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
3. For minor / major bump the **hub** extra dependencies (see below)
4. Run `make build-library` to apply changes.
5. Create a new branch and PR targeting **devel**, then merge it.
6. Merge **devel** into **master** with a ❗ **merge commit** (not squash).

### Publishing

1. Check out **master** and pull the latest code.
2. Verify the version with `uv version`.
3. Obtain a PyPI access token.
4. Run `make publish-library` and provide the token.
5. Create a GitHub release using the version and git tag.

**bump hub extra dependencies on minor/major bump**:

1. Find the `hub` extra in `pyproject.toml` and bump the **upper bound** minor version on each plugin.
2. You may keep the lower bound if this `dlt` version is compatible
with plugin versions in allowed range.

### Hotfix Release

1. Check out **master**.
2. Bump the patch version with `uv version --bump patch`.
3. Run `make build-library`.
4. Create a new branch and PR targeting **master**, then merge it.
5. Re-submit the same fix to **devel**.

Then follow the "publish" from Regular release

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
