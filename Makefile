.PHONY: \
  test-load-local test-load-local-p \
  test-load-local-postgres test-load-local-postgres-p \
  test-dest-load test-dest-load-serial \
  test-dest-remote-essential test-dest-remote-nonessential \
  test-dbt-no-venv test-dbt-runner-venv

PYV=$(shell python3 -c "import sys;t='{v[0]}.{v[1]}'.format(v=list(sys.version_info[:2]));sys.stdout.write(t)")
.SILENT:has-uv

# read version from package
# AUTV=$(shell cd dlt && python3 -c "from __version__ import __version__;print(__version__)")

# NAME   := dlthub/dlt
# TAG    := $(shell git log -1 --pretty=%h)
# IMG    := ${NAME}:${TAG}
# LATEST := ${NAME}:latest${VERSION_SUFFIX}
# VERSION := ${AUTV}${VERSION_SUFFIX}
# VERSION_MM := ${AUTVMINMAJ}${VERSION_SUFFIX}

help:
	@echo "make"
	@echo "		install-uv"
	@echo "			installs newest uv version"
	@echo "		dev"
	@echo "			prepares development env"
	@echo "		lint"
	@echo "			runs flake and mypy"
	@echo "		test"
	@echo "			tests all the components including destinations"
	@echo "		test-load-local"
	@echo "			tests all components using local destinations: duckdb and postgres"
	@echo "		test-common"
	@echo "			tests common components"
	@echo "		lint-and-test-snippets"
	@echo "			tests and lints snippets and examples in docs"
	@echo "		build-library"
	@echo "			makes dev and then builds dlt package for distribution"
	@echo "		publish-library"
	@echo "			builds library and then publishes it to pypi"

install-uv:
ifneq ($(VIRTUAL_ENV),)
	$(error you cannot be under virtual environment $(VIRTUAL_ENV))
endif
	curl -LsSf https://astral.sh/uv/install.sh | sh

has-uv:
	uv --version

dev: has-uv
	uv sync --all-extras --no-extra hub --group dev --group providers --group pipeline --group sources --group sentry-sdk --group ibis --group adbc --group dashboard-tests

dev-airflow: has-uv
	uv sync --all-extras --no-extra hub --group providers --group pipeline --group sources --group sentry-sdk --group ibis --group airflow

dev-hub: has-uv
	uv sync --all-extras --group dev --group providers --group pipeline --group sources --group sentry-sdk --group ibis --group adbc --group dashboard-tests

lint: lint-core lint-security lint-docstrings lint-lock

lint-lock:
	uv lock --check
	uv run python tools/check_hub_extras.py

lint-core:
	uv run mypy --config-file mypy.ini dlt tests tools
	# NOTE: we need to make sure docstring_parser_fork is the only version of docstring_parser installed
	uv pip uninstall docstring_parser
	uv pip install docstring_parser_fork --reinstall
	uv run ruff check
	# NOTE: we exclude all D lint errors (docstrings)
	uv run flake8 --extend-ignore=D --max-line-length=200 dlt tools
	uv run flake8 --extend-ignore=D --max-line-length=200 tests --exclude tests/reflection/module_cases,tests/common/reflection/cases/modules/

format:
	uv run black dlt tests tools --extend-exclude='.*syntax_error.py|^_storage[^/]*/'

format-check:
	$(MAKE) format
	git diff --exit-code

lint-security:
	# go for ll by cleaning up eval and SQL warnings.
	uv run bandit -r dlt/ -n 3 -lll

# check docstrings for all important public classes and functions
lint-docstrings:
	uv run flake8 --count \
		dlt/common/pipeline.py \
		dlt/extract/decorators.py \
		dlt/destinations/decorators.py \
		dlt/sources/**/__init__.py \
		dlt/extract/source.py \
		dlt/common/destination/dataset.py \
		dlt/destinations/impl/**/factory.py \
		dlt/pipeline/pipeline.py \
		dlt/pipeline/__init__.py \
		tests/pipeline/utils.py

# Generate random PYTHONHASHSEED between 0 and 50
PYTHONHASHSEED := $(shell shuf -i 0-50 -n 1 2>/dev/null || echo $$((RANDOM % 51)))
PYTEST = PYTHONHASHSEED=$(PYTHONHASHSEED) uv run pytest --rootdir=.
PYTEST_ARGS ?=

# Enable xdist if PYTEST_XDIST_N is set
ifneq ($(strip $(PYTEST_XDIST_N)),)
  PYTEST_ARGS += -p xdist -n $(PYTEST_XDIST_N)
endif

# convenience test commands to run tests locally
test:
	$(PYTEST) $(PYTEST_ARGS) tests

test-common:
	$(PYTEST) $(PYTEST_ARGS) \
		tests/common \
		tests/normalize \
		tests/extract \
		tests/pipeline \
		tests/reflection \
		tests/sources \
		tests/workspace \
		tests/load/test_dummy_client.py \
		tests/libs \
		tests/destinations

test-common-p:
	$(MAKE) test-common PYTEST_XDIST_N=auto

test-load-local:
	ACTIVE_DESTINATIONS='["duckdb", "filesystem"]' \
	ALL_FILESYSTEM_DRIVERS='["memory", "file"]' \
	$(MAKE) test-dest-load

	ACTIVE_DESTINATIONS='["duckdb", "filesystem"]' \
	ALL_FILESYSTEM_DRIVERS='["memory", "file"]' \
	$(MAKE) test-dest-load-serial

test-load-local-p:
	$(MAKE) test-load-local PYTEST_XDIST_N=auto

test-load-local-postgres:
	DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data \
	ACTIVE_DESTINATIONS='["postgres"]' \
	ALL_FILESYSTEM_DRIVERS='["memory"]' \
	$(MAKE) test-dest-load

	DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data \
	ACTIVE_DESTINATIONS='["postgres"]' \
	ALL_FILESYSTEM_DRIVERS='["memory"]' \
	$(MAKE) test-dest-load-serial

test-load-local-postgres-p:
	$(MAKE) test-load-local-postgres PYTEST_XDIST_N=auto

# these are convenience make commands for testing remote snowflake from local dev env but not re-used by CI
install-snowflake-extras:
	uv sync --group pipeline --group ibis --group providers \
		--extra snowflake --extra s3 --extra gs --extra az --extra parquet
		
test-remote-snowflake:
	ACTIVE_DESTINATIONS='["snowflake"]' \
	ALL_FILESYSTEM_DRIVERS='["memory"]' \
	$(MAKE) test-dest-remote-essential

test-remote-snowflake-p:
	$(MAKE) test-remote-snowflake PYTEST_XDIST_N=auto

# CI: these make commands are used by github actions
# common
test-common-core:
	$(PYTEST) $(PYTEST_ARGS) \
		tests/common \
		tests/normalize \
		tests/reflection \
		tests/plugins \
		tests/load/test_dummy_client.py \
		tests/extract/test_extract.py \
		tests/extract/test_sources.py \
		tests/pipeline/test_pipeline_state.py

test-common-smoke:
	$(PYTEST) $(PYTEST_ARGS) \
		tests/pipeline/test_pipeline.py \
		tests/pipeline/test_import_export_schema.py \
		tests/sources/rest_api/integration/

test-workspace:
	$(PYTEST) $(PYTEST_ARGS) \
		tests/workspace

test-full:
	$(PYTEST) $(PYTEST_ARGS) \
		tests/extract \
		tests/pipeline \
		tests/libs \
		tests/destinations \
		tests/dataset \
		tests/sources

test-sql-database:
	$(PYTEST) $(PYTEST_ARGS) \
		tests/sources/sql_database

#local dest
test-dest-load:
	$(PYTEST) $(PYTEST_ARGS) \
		-m "not serial" \
		tests/load \
		--ignore tests/load/sources \
		--ignore tests/load/filesystem_sftp

test-dest-load-serial:
	$(PYTEST) \
		-m serial \
		tests/load \
		--ignore tests/load/sources \
		--ignore tests/load/filesystem_sftp \
		|| [ $$? -eq 5 ]

#remote dest
test-dest-remote-essential:
	$(PYTEST) $(PYTEST_ARGS) \
		--ignore tests/load/sources \
		-m essential \
		tests/load

test-dest-remote-nonessential:
	$(PYTEST) $(PYTEST_ARGS) \
		--ignore tests/load/sources \
		-m "not essential" \
		tests/load

#dbt
test-dbt-no-venv:
	$(PYTEST) $(PYTEST_ARGS) \
		-k "not venv" \
		tests/helpers/dbt_tests

test-dbt-runner-venv:
	$(PYTEST) $(PYTEST_ARGS) \
		--ignore tests/helpers/dbt_tests/local \
		-k "not local" \
		tests/helpers/dbt_tests

#dashboard
test-workspace-dashboard:
	$(PYTEST) $(PYTEST_ARGS) tests/workspace/helpers/dashboard

#sources
test-sources-load:
	$(PYTEST) $(PYTEST_ARGS) tests/load/sources

test-sources-sql-database:
	$(PYTEST) $(PYTEST_ARGS) tests/load/sources/sql_database
#--end CI

build-library: dev lint-lock
	uv version
	uv build

clean-dist:
	-@rm -r dist/

publish-library: clean-dist build-library
	ls -l dist/
	@bash -c 'read -s -p "Enter PyPI API token: " PYPI_API_TOKEN; echo; \
	uv publish --token "$$PYPI_API_TOKEN"'

test-build-images: build-library
	# NOTE: uv export does not work with our many different deps, we install a subset and freeze
	# uv sync --extra gcp --extra redshift --extra duckdb
	# uv pip freeze > _gen_requirements.txt
	# filter out libs that need native compilation
	# grep `cat compiled_packages.txt` _gen_requirements.txt > compiled_requirements.txt
	docker build -f deploy/dlt/Dockerfile.airflow --build-arg=COMMIT_SHA="$(shell git log -1 --pretty=%h)" --build-arg=IMAGE_VERSION="$(shell uv version --short)" .
	docker build -f deploy/dlt/Dockerfile.minimal --build-arg=COMMIT_SHA="$(shell git log -1 --pretty=%h)" --build-arg=IMAGE_VERSION="$(shell uv version --short)" .

start-test-containers:
	docker compose -f "tests/load/dremio/docker-compose.yml" up -d
	docker compose -f "tests/load/postgres/docker-compose.yml" up -d
	docker compose -f "tests/load/weaviate/docker-compose.yml" up -d
	docker compose -f "tests/load/filesystem_sftp/docker-compose.yml" up -d
	docker compose -f "tests/load/sqlalchemy/docker-compose.yml" up -d
	docker compose -f "tests/load/clickhouse/docker-compose.yml" up -d

update-cli-docs:
	uv run dlt --debug render-docs docs/website/docs/reference/command-line-interface.md

check-cli-docs:
	uv run dlt --debug render-docs docs/website/docs/reference/command-line-interface.md --compare

# Commands for running dashboard e2e tests
# To run these tests locally, run `make start-dlt-dashboard-e2e` in one terminal and `make test-e2e-dashboard-headed` in another terminal

test-e2e-dashboard:
	uv run pytest --browser chromium tests/e2e

test-e2e-dashboard-headed:
	uv run pytest --headed --browser chromium tests/e2e

# creates the dashboard test pipelines globally for manual testing of the dashboard app and cli
create-test-pipelines:
	uv run python tests/workspace/helpers/dashboard/example_pipelines.py
