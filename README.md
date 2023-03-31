![](https://github.com/dlt-hub/dlt/raw/devel/docs/DLT-Pacman-Big.gif)

<p align="center">

[![PyPI version](https://badge.fury.io/py/python-dlt.svg)](https://pypi.org/project/python-dlt/)
[![LINT Badge](https://github.com/dlt-hub/dlt/actions/workflows/lint.yml/badge.svg)](https://github.com/dlt-hub/dlt/actions/workflows/lint.yml)
[![TEST COMMON Badge](https://github.com/dlt-hub/dlt/actions/workflows/test_common.yml/badge.svg)](https://github.com/dlt-hub/dlt/actions/workflows/test_common.yml)
[![TEST DESTINATIONS Badge](https://github.com/dlt-hub/dlt/actions/workflows/test_destinations.yml/badge.svg)](https://github.com/dlt-hub/dlt/actions/workflows/test_destinations.yml)
[![TEST BIGQUERY Badge](https://github.com/dlt-hub/dlt/actions/workflows/test_destination_bigquery.yml/badge.svg)](https://github.com/dlt-hub/dlt/actions/workflows/test_destination_bigquery.yml)
[![TEST DBT Badge](https://github.com/dlt-hub/dlt/actions/workflows/test_dbt_runner.yml/badge.svg)](https://github.com/dlt-hub/dlt/actions/workflows/test_dbt_runner.yml)


</p>

# data load tool (dlt)

**data load tool (dlt)** is a simple, open source Python library that makes data loading easy
- Automatically turn the JSON returned by any API into a live dataset stored wherever you want it
- `pip install python-dlt` and then include `import dlt` to use it in your Python loading script
- The **dlt** library is licensed under the Apache License 2.0, so you can use it for free forever

Read more about it on the [dlt Docs](https://dlthub.com/docs)

# semantic versioning
`python-dlt` will follow the semantic versioning with [`MAJOR.MINOR.PATCH`](https://peps.python.org/pep-0440/#semantic-versioning) pattern. Currently we do **pre-release versioning** with major version being 0.
- `minor` version change means breaking changes
- `patch` version change means new features that should be backward compatible
- any suffix change ie. `a10` -> `a11` is a patch

# development
`python-dlt` uses `poetry` to manage, build and version the package. It also uses `make` to automate tasks. To start
```sh
make install-poetry  # will install poetry, to be run outside virtualenv
```
then
```sh
make dev  # will install all deps including dev
```
Executing `poetry shell` and working in it is very convenient at this moment.

## python version
Use python 3.8 for development which is the lowest supported version for `python-dlt`. You'll need `distutils` and `venv`:

```shell
sudo apt-get install python3.8
sudo apt-get install python3.8-distutils
sudo apt install python3.8-venv
```
You may also use `pyenv` as [poetry](https://python-poetry.org/docs/managing-environments/) suggests.

## bumping version
Please use `poetry version prerelease` to bump patch and then `make build-library` to apply changes. The source of the version is `pyproject.toml` and we use poetry to manage it.

## testing and linting
`python-dlt` uses `mypy` and `flake8` with several plugins for linting. We do not reorder imports or reformat code.

`pytest` is used as test harness. `make test-common` will run tests of common components and does not require any external resources.

### testing destinations
To test destinations use `make test`. You will need following external resources
1. `BigQuery` project
2. `Redshift` cluster
3. `Postgres` instance. You can find a docker compose for postgres instance [here](tests/load/postgres/docker-compose.yml). When run the instance is configured to work with the tests.
```shell
cd tests/load/postgres/
docker-compose up --build -d
```

See `tests/.example.env` for the expected environment variables and command line example to run the tests. Then create `tests/.env` from it. You configure the tests as you would configure the dlt pipeline.
We'll provide you with access to the resources above if you wish to test locally.

To test local destinations (`duckdb` and `postgres`) run `make test-local`. You can run this tests without additional credentials (just copy `.example.env` into `.env`)

## publishing

1. Make sure that you are on `devel` branch and you have the newest code that passed all tests on CI.
2. Verify the current version with `poetry version`
3. You'll need `pypi` access token and use `poetry config pypi-token.pypi your-api-token` then
```
make publish-library
```
4. Make a release on github, use version and git tag as release name

## contributing

To contribute via pull request:
1. Create an issue with your idea for a feature etc.
2. Write your code and tests
3. Lint your code with `make lint`. Test the common modules with `make test-common`
4. If you work on a destination code then contact us to get access to test destinations
5. Create a pull request
