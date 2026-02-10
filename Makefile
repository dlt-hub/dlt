.DEFAULT_GOAL := help
.PHONY: install-uv has-uv dev lint test test-common test-common-p reset-test-storage recreate-compiled-deps build-library-prerelease build-library publish-library test-load-local test-load-local-p test-load-local-postgres test-load-local-postgres-p install-snowflake-extras test-remote-snowflake test-remote-snowflake-p install-common-core test-common-core install-common-core-source test-common-core-source install-common-source install-pipeline-min test-pipeline-min install-pipeline-arrow test-pipeline-arrow install-pipeline-min-arrow test-pipeline-min-arrow install-workspace test-workspace test-workspace-dashboard install-pipeline-full test-pipeline-full install-pipeline-full-sql test-pipeline-full-sql install-sqlalchemy2 test-with-sqlalchemy-2 test-dest-load test-dest-remote-essential test-dest-remote-nonessential test-dbt-no-venv test-dbt-runner-venv test-sources-load test-sources-sql-database

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

# Add " ## description" after any target name to include it in `make help` output.
# Example:  my-target: ## Does something useful
help: ## Shows this help message
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-32s %s\n", $$1, $$2}'

install-uv: ## Installs newest uv version
ifneq ($(VIRTUAL_ENV),)
	$(error you cannot be under virtual environment $(VIRTUAL_ENV))
endif
	curl -LsSf https://astral.sh/uv/install.sh | sh

has-uv:
	uv --version

dev: has-uv ## Prepares development environment
	uv sync --all-extras --no-extra hub --group dev --group providers --group pipeline --group sources --group sentry-sdk --group ibis --group adbc --group dashboard-tests

dev-airflow: has-uv ## Prepares development environment with airflow support
	uv sync --all-extras --no-extra hub --group providers --group pipeline --group sources --group sentry-sdk --group ibis --group airflow

dev-hub: has-uv ## Prepares development environment with hub support
	uv sync --all-extras --group dev --group providers --group pipeline --group sources --group sentry-sdk --group ibis --group adbc --group dashboard-tests

lint: lint-core lint-security lint-docstrings lint-lock lint-deps ## Runs all linters (mypy, ruff, flake8, bandit, docstrings, lockfile, deps)

lint-lock: ## Checks uv lockfile is in sync
	uv lock --check

lint-deps: ## Checks dependencies, hub extras, and API breaking changes (informational)
	uv run python tools/check_hub_extras.py
	-uv run python -m tools.check_dependency_changes
	-uv run python -m tools.check_api_breaking check

lint-core: ## Runs core linting (mypy, ruff, flake8)
	uv run mypy --config-file mypy.ini dlt tests tools
	# NOTE: we need to make sure docstring_parser_fork is the only version of docstring_parser installed
	uv pip uninstall docstring_parser
	uv pip install docstring_parser_fork --reinstall
	uv run ruff check
	# NOTE: we exclude all D lint errors (docstrings)
	uv run flake8 --extend-ignore=D --max-line-length=200 dlt tools
	uv run flake8 --extend-ignore=D --max-line-length=200 tests --exclude tests/reflection/module_cases,tests/common/reflection/cases/modules/

format: ## Formats code with black
	uv run black dlt tests tools --extend-exclude='.*syntax_error.py|^_storage[^/]*/'

format-check: ## Formats and verifies no files changed (CI)
	$(MAKE) format
	git diff --exit-code

lint-security: ## Runs security linting with bandit
	# go for ll by cleaning up eval and SQL warnings.
	uv run bandit -r dlt/ -n 3 -lll

lint-docstrings: ## Checks docstrings for public API classes and functions
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

# ======================================================================
# TEST EXECUTION MODEL (shared by local + CI)
# ======================================================================

# Random but stable per run; shared across xdist workers to surface order bugs
PYTHONHASHSEED := $(shell shuf -i 0-50 -n 1 2>/dev/null || echo $$((RANDOM % 51)))

# User-provided overrides
PYTEST_ARGS        ?=
PYTEST_MARKERS     ?=
PYTEST_XDIST_N     ?=
PYTEST_TARGET_ARGS :=

# Internal marker model
PARALLEL_MARKER_EXPR = (not serial and not forked)
SERIAL_MARKER_EXPR   = (serial or forked)

ifeq ($(OS),Windows_NT)
  PYTEST_MARKERS += not forked and not rfam
  PYTEST_ARGS += -p no:forked
  SERIAL_MARKER_EXPR = serial
endif

define COMBINE_MARKERS
$(strip $(if $(PYTEST_MARKERS),($(PYTEST_MARKERS)) and ,)$(1))
endef

# Base pytest invocation (no xdist, no markers)
PYTEST_BASE = \
	PYTHONHASHSEED=$(PYTHONHASHSEED) \
	uv run pytest \
	$(PYTEST_TARGET_ARGS) \
	$(PYTEST_ARGS)

# Parallel execution with PYTEST_XDIST_N provided
PYTEST_PARALLEL = \
	$(PYTEST_BASE) \
	$(if $(PYTEST_XDIST_N),-p xdist -n $(PYTEST_XDIST_N) $(if $(PYTEST_XDIST_DIST),--dist=$(PYTEST_XDIST_DIST))) \
	-m "$(call COMBINE_MARKERS,$(PARALLEL_MARKER_EXPR))"

# Normal serial execution
PYTEST_SERIAL = \
	$(PYTEST_BASE) \
	-m "$(call COMBINE_MARKERS,$(SERIAL_MARKER_EXPR))"

# Run xdist-safe tests first, then serial/forked ones
define RUN_XDIST_SAFE_SPLIT
	PYTEST_MARKERS="$(PYTEST_MARKERS)" PYTEST_ARGS="$(PYTEST_ARGS)" \
	$(PYTEST_PARALLEL) $(1) || [ $$? -eq 5 ]
	PYTEST_MARKERS="$(PYTEST_MARKERS)" PYTEST_ARGS="$(PYTEST_ARGS)" \
	$(PYTEST_SERIAL)   $(1) || [ $$? -eq 5 ]
endef

# ======================================================================
# LOCAL / DEV TESTING
# ======================================================================

test: ## Tests all components including destinations
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

test-common: ## Tests common components without external resources
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_COMMON_PATHS))

test-common-p: ## Tests common components in parallel
	$(MAKE) test-common PYTEST_XDIST_N=auto

# ----------------------------------------------------------------------
# Local load tests
# ----------------------------------------------------------------------

TEST_LOAD_PATHS = tests/load

test-load-local: ## Tests load with local destinations (duckdb + filesystem)
	ACTIVE_DESTINATIONS='["duckdb", "filesystem"]' \
	ALL_FILESYSTEM_DRIVERS='["memory", "file"]' \
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_LOAD_PATHS))

test-load-local-p: ## Tests load with local destinations in parallel
	$(MAKE) test-load-local PYTEST_XDIST_N=auto

test-load-local-postgres: ## Tests load with local postgres (requires start-test-containers)
	DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data \
	ACTIVE_DESTINATIONS='["postgres"]' \
	ALL_FILESYSTEM_DRIVERS='["memory"]' \
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_LOAD_PATHS))

test-load-local-postgres-p: ## Tests load with local postgres in parallel
	$(MAKE) test-load-local-postgres PYTEST_XDIST_N=auto

# ----------------------------------------------------------------------
# Local remote-destination smoke (not used by CI)
# ----------------------------------------------------------------------

install-snowflake-extras: ## Installs extras needed for snowflake testing
	uv sync --group pipeline --group ibis --group providers \
		--extra snowflake --extra s3 --extra gs --extra az --extra parquet

test-remote-snowflake: ## Tests essential load tests against snowflake
	ACTIVE_DESTINATIONS='["snowflake"]' \
	ALL_FILESYSTEM_DRIVERS='["memory"]' \
	$(MAKE) test-dest-remote-essential

test-remote-snowflake-p: ## Tests essential load tests against snowflake in parallel
	$(MAKE) test-remote-snowflake PYTEST_XDIST_N=auto

# ======================================================================
# CI TARGETS (used by GitHub Actions)
# ======================================================================

UV_SYNC_ARGS ?=

# ----------------------------------------------------------------------
# CI: minimal core dependency surface
# ----------------------------------------------------------------------

install-common-core:
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

test-tools:
	uv run pytest tools/tests -v

install-common-source:
	uv sync $(UV_SYNC_ARGS) --group sentry-sdk --extra sql_database

test-common-source:
	uv run pytest tests/sources/test_minimal_dependencies.py

# ----------------------------------------------------------------------
# CI: pipeline smoke tests
# ----------------------------------------------------------------------

install-pipeline-min:
	uv sync $(UV_SYNC_ARGS) --group sentry-sdk --extra duckdb

TEST_PIPELINE_MIN_PATHS = \
	tests/pipeline/test_pipeline.py \
	tests/pipeline/test_import_export_schema.py \
	tests/sources/rest_api/integration/

test-pipeline-min:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_PIPELINE_MIN_PATHS))

install-pipeline-arrow:
	uv sync $(UV_SYNC_ARGS) --extra duckdb --extra cli --extra parquet

TEST_PIPELINE_ARROW_PATHS = tests/pipeline/test_pipeline_extra.py

test-pipeline-arrow: PYTEST_TARGET_ARGS = -k arrow
test-pipeline-arrow:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_PIPELINE_ARROW_PATHS))

# ----------------------------------------------------------------------
# CI: workspace
# ----------------------------------------------------------------------

install-workspace:
	uv sync $(UV_SYNC_ARGS) --extra workspace --extra cli

TEST_WORKSPACE_PATHS = tests/workspace

test-workspace:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_WORKSPACE_PATHS))

# ----------------------------------------------------------------------
# CI: full pipeline + sources + destinations
# ----------------------------------------------------------------------

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
		--extra pyiceberg \
		--extra sql_database

TEST_FULL_PATHS = \
	tests/extract \
	tests/pipeline \
	tests/libs \
	tests/destinations \
	tests/dataset \
	tests/sources

test-pipeline-full:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_FULL_PATHS))

install-sqlalchemy2:
	uv run pip install sqlalchemy==2.0.32

TEST_SQL_DATABASE_PATHS = tests/sources/sql_database tests/common/libs/

test-with-sqlalchemy-2:
	$(call RUN_XDIST_SAFE_SPLIT,$(TEST_SQL_DATABASE_PATHS))

# ----------------------------------------------------------------------
# CI: destination- and feature-specific
# ----------------------------------------------------------------------

test-dest-load:
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/load \
		--ignore tests/load/sources \
		--ignore tests/load/filesystem_sftp \
	)

test-dest-remote-essential: PYTEST_MARKERS = essential
test-dest-remote-essential:
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/load \
		--ignore tests/load/sources \
	)

test-dest-remote-nonessential: PYTEST_MARKERS = not essential
test-dest-remote-nonessential:
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/load \
		--ignore tests/load/sources \
	)

# ----------------------------------------------------------------------
# CI: dbt
# ----------------------------------------------------------------------

test-dbt-no-venv: PYTEST_TARGET_ARGS = -k "not venv"
test-dbt-no-venv:
	$(call RUN_XDIST_SAFE_SPLIT, tests/helpers/dbt_tests)

test-dbt-runner-venv: PYTEST_TARGET_ARGS = \
	--ignore tests/helpers/dbt_tests/local \
	-k "not local"
test-dbt-runner-venv:
	$(call RUN_XDIST_SAFE_SPLIT, tests/helpers/dbt_tests)

# ----------------------------------------------------------------------
# CI: workspace dashboard & sources
# ----------------------------------------------------------------------

test-workspace-dashboard:
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/workspace/helpers/dashboard \
	)

# ----------------------------------------------------------------------
# CI: sources
# ----------------------------------------------------------------------

test-sources-load:
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/load/sources \
	)

test-sources-sql-database:
	$(call RUN_XDIST_SAFE_SPLIT, \
		tests/load/sources/sql_database \
	)

###################### END CI

build-library: dev lint-lock ## Builds dlt package for distribution
	uv version
	uv build

clean-dist: ## Removes dist/ directory
	-@rm -r dist/

publish-library: clean-dist build-library ## Builds and publishes dlt to PyPI
	ls -l dist/
	@bash -c 'read -s -p "Enter PyPI API token: " PYPI_API_TOKEN; echo; \
	uv publish --token "$$PYPI_API_TOKEN"'

test-build-images: build-library ## Builds Docker images for testing
	# NOTE: uv export does not work with our many different deps, we install a subset and freeze
	# uv sync --extra gcp --extra redshift --extra duckdb
	# uv pip freeze > _gen_requirements.txt
	# filter out libs that need native compilation
	# grep `cat compiled_packages.txt` _gen_requirements.txt > compiled_requirements.txt
	docker build -f deploy/dlt/Dockerfile.airflow --build-arg=COMMIT_SHA="$(shell git log -1 --pretty=%h)" --build-arg=IMAGE_VERSION="$(shell uv version --short)" .
	docker build -f deploy/dlt/Dockerfile.minimal --build-arg=COMMIT_SHA="$(shell git log -1 --pretty=%h)" --build-arg=IMAGE_VERSION="$(shell uv version --short)" .

start-test-containers: ## Starts docker containers for local testing (postgres, clickhouse, etc.)
	docker compose -f "tests/load/dremio/docker-compose.yml" up -d
	docker compose -f "tests/load/postgres/docker-compose.yml" up -d
	docker compose -f "tests/load/weaviate/docker-compose.yml" up -d
	docker compose -f "tests/load/filesystem_sftp/docker-compose.yml" up -d
	docker compose -f "tests/load/sqlalchemy/docker-compose.yml" up -d
	docker compose -f "tests/load/clickhouse/docker-compose.yml" up -d

update-cli-docs: ## Regenerates CLI reference docs
	uv run dlt --debug render-docs docs/website/docs/reference/command-line-interface.md

check-cli-docs: ## Checks CLI reference docs are up to date (CI)
	uv run dlt --debug render-docs docs/website/docs/reference/command-line-interface.md --compare

test-e2e-dashboard: ## Runs dashboard e2e tests with headless chromium
	uv run pytest --browser chromium tests/e2e

test-e2e-dashboard-headed: ## Runs dashboard e2e tests with visible browser
	uv run pytest --headed --browser chromium tests/e2e

create-test-pipelines: ## Creates test pipelines for manual dashboard testing
	uv run python tests/workspace/helpers/dashboard/example_pipelines.py
