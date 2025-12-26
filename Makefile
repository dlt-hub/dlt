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

# Generate random PYTHONHASHSEED that stays constant between xdist workers to detect order-dependent bugs
PYTHONHASHSEED := $(shell shuf -i 0-50 -n 1 2>/dev/null || echo $$((RANDOM % 51)))

# Extra pytest args passed from CLI
# Example: make test-common PYTEST_EXTRA="-k trace -vv"
PYTEST_EXTRA ?=

# Additional marker expression passed from CLI
# Example: make test-common PYTEST_MARKERS="integration and not slow"
PYTEST_MARKERS ?=

# Number of xdist workers
PYTEST_XDIST_N ?=

# Marker expressions (NO -m here)
PARALLEL_MARKER_EXPR = not serial and not forked
SERIAL_MARKER_EXPR   = serial or forked

ifeq ($(OS),Windows_NT)
  PYTEST_EXTRA += -m "not forked and not rfam"
  PYTEST_EXTRA += -p no:forked
  SERIAL_MARKER_EXPR = serial
endif

# Combine user markers with internal markers
define COMBINE_MARKERS
$(strip \
$(if $(PYTEST_MARKERS),($(PYTEST_MARKERS)) and ,)$(1))
endef

# Base pytest command (never includes xdist or markers)
PYTEST_BASE = PYTHONHASHSEED=$(PYTHONHASHSEED) uv run pytest --rootdir=. $(PYTEST_EXTRA)

# Parallel vs serial execution
PYTEST_PARALLEL = \
	$(PYTEST_BASE) \
	$(if $(PYTEST_XDIST_N),-p xdist -n $(PYTEST_XDIST_N) $(if $(PYTEST_XDIST_DIST),--dist=$(PYTEST_XDIST_DIST))) \
	-m "$(call COMBINE_MARKERS,$(PARALLEL_MARKER_EXPR))"

PYTEST_SERIAL = \
	$(PYTEST_BASE) \
	-m "$(call COMBINE_MARKERS,$(SERIAL_MARKER_EXPR))"

# Run parallel-safe tests first, then serial/forked tests
define RUN_XDIST_SAFE_SPLIT
	$(PYTEST_PARALLEL) $(1)
	$(PYTEST_SERIAL)   $(1) || [ $$? -eq 5 ]
endef

## Run the full test suite (single process)
test:
	$(PYTEST_BASE) tests

TEST_COMMON_PATHS = \
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

## Run common tests (parallel-safe first, then serial/forked)
test-common:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_COMMON_PATHS))

## Run common tests with xdist enabled
test-common-p:
	$(MAKE) test-common PYTEST_XDIST_N=auto

## Run load tests locally (duckdb + filesystem)
test-load-local:
	ACTIVE_DESTINATIONS='["duckdb", "filesystem"]' \
	ALL_FILESYSTEM_DRIVERS='["memory", "file"]' \
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_LOAD_PATHS))

TEST_LOAD_PATHS = tests/load

## Run load tests locally with xdist enabled
test-load-local-p:
	$(MAKE) test-load-local PYTEST_XDIST_N=auto

## Run load tests locally against Postgres
test-load-local-postgres:
	DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data \
	ACTIVE_DESTINATIONS='["postgres"]' \
	ALL_FILESYSTEM_DRIVERS='["memory"]' \
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_LOAD_PATHS))

## Run Postgres load tests with xdist enabled
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
UV_SYNC_ARGS ?=

install-common-core: ## Install minimal dependencies for core tests
	uv sync $(UV_SYNC_ARGS) --group sentry-sdk

TEST_COMMON_CORE_PATHS = \
	tests/common \
	tests/normalize \
	tests/reflection \
	tests/plugins \
	tests/load/test_dummy_client.py \
	tests/extract/test_extract.py \
	tests/extract/test_sources.py \
	tests/pipeline/test_pipeline_state.py

test-common-core:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_COMMON_CORE_PATHS))

install-common-core-source:
	uv sync $(UV_SYNC_ARGS) --group sentry-sdk --extra sql_database

## Install pipeline smoke deps (duckdb, no pandas)
install-pipeline-min:
	uv sync $(UV_SYNC_ARGS) --group sentry-sdk --extra duckdb

TEST_PIPELINE_MIN_PATHS = \
	tests/pipeline/test_pipeline.py \
	tests/pipeline/test_import_export_schema.py \
	tests/sources/rest_api/integration/

test-pipeline-min:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_PIPELINE_MIN_PATHS))

## Install workspace deps
install-workspace:
	uv sync $(UV_SYNC_ARGS) --extra workspace --extra cli

TEST_WORKSPACE_PATHS = tests/workspace

test-workspace:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_WORKSPACE_PATHS))

## Install full pipeline + sources deps
install-pipeline-full:
	uv sync \
		$(UV_SYNC_ARGS) \
		--group sentry-sdk \
		--group pipeline \
		--group sources \
		--group ibis \
		--extra http \
		--extra duckdb \
		--extra parquet \
		--extra deltalake \
		--extra sql_database

TEST_FULL_PATHS = \
	tests/extract \
	tests/pipeline \
	tests/libs \
	tests/destinations \
	tests/dataset \
	tests/sources

test-full:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_FULL_PATHS))

install-sqlalchemy2:
	uv run pip install sqlalchemy==2.0.32

TEST_SQL_DATABASE_PATHS = tests/sources/sql_database

test-sql-database:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_SQL_DATABASE_PATHS))

#local dest
test-dest-load:
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/load \
		--ignore tests/load/sources \
		--ignore tests/load/filesystem_sftp \
	)

#remote dest
test-dest-remote-essential:
	PYTEST_MARKERS=essential \
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/load \
		--ignore tests/load/sources \
	)

test-dest-remote-nonessential:
	PYTEST_MARKERS="not essential" \
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/load \
		--ignore tests/load/sources \
	)

#dbt
test-dbt-no-venv:
	PYTEST_EXTRA='-k "not venv"' \
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/helpers/dbt_tests \
	)

test-dbt-runner-venv:
	PYTEST_EXTRA='--ignore tests/helpers/dbt_tests/local -k "not local"' \
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/helpers/dbt_tests \
	)

#dashboard
test-workspace-dashboard:
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/workspace/helpers/dashboard \
	)

#sources
test-sources-load:
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/load/sources \
	)

test-sources-sql-database:
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/load/sources/sql_database \
	)

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
