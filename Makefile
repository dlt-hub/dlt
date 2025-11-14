.PHONY: install-uv build-library-prerelease has-uv dev lint test test-common reset-test-storage recreate-compiled-deps build-library-prerelease publish-library

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
	uv sync --all-extras --group docs --group dev --group providers --group pipeline --group sources --group sentry-sdk --group ibis --group adbc --group dashboard-tests

dev-airflow: has-uv
	uv sync --all-extras --group docs --group providers --group pipeline --group sources --group sentry-sdk --group ibis --group airflow

lint: lint-core lint-security lint-docstrings

lint-core:
	uv run mypy --config-file mypy.ini dlt tests
	# NOTE: we need to make sure docstring_parser_fork is the only version of docstring_parser installed
	uv pip uninstall docstring_parser
	uv pip install docstring_parser_fork --reinstall
	uv run ruff check
	# NOTE: we exclude all D lint errors (docstrings)
	uv run flake8 --extend-ignore=D --max-line-length=200 dlt
	uv run flake8 --extend-ignore=D --max-line-length=200 tests --exclude tests/reflection/module_cases,tests/common/reflection/cases/modules/
	uv run black dlt docs tests --check --diff --color --extend-exclude=".*syntax_error.py"

format:
	uv run black dlt docs tests --extend-exclude='.*syntax_error.py|_storage/.*'
	uv run black docs/education --ipynb --extend-exclude='.*syntax_error.py|_storage/.*'

lint-snippets:
	cd docs/tools && uv run python check_embedded_snippets.py full
	# TODO: re-enable transformation snippets tests when dlthub dep is available
	uv pip install docstring_parser_fork --reinstall
	uv run mypy --config-file mypy.ini docs/website docs/tools --exclude docs/tools/lint_setup --exclude docs/website/docs_processed --exclude docs/website/versioned_docs/
	uv run ruff check
	uv run flake8 --max-line-length=200 docs/website docs/tools --exclude docs/website/.dlt-repo,docs/website/node_modules

lint-and-test-snippets: lint-snippets
	cd docs/website/docs && uv run pytest --ignore=node_modules

lint-and-test-examples:
	uv pip install docstring_parser_fork --reinstall
	cd docs/tools && uv run python prepare_examples_tests.py
	uv run ruff check
	uv run flake8 --max-line-length=200 docs/examples
	uv run mypy --config-file mypy.ini docs/examples
	cd docs/examples && uv run pytest

test-examples:
	cd docs/examples && uv run pytest

lint-security:
	# go for ll by cleaning up eval and SQL warnings.
	uv run bandit -r dlt/ -n 3 -lll

lint-notebooks:
	uv run nbqa flake8 docs/education --extend-ignore=D,F704 --max-line-length=200
	uv run nbqa mypy docs/education \
	--ignore-missing-imports \
	--disable-error-code=no-redef \
	--disable-error-code=top-level-await

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

test:
	uv run pytest tests

test-load-local:
	ACTIVE_DESTINATIONS='["duckdb", "filesystem"]' ALL_FILESYSTEM_DRIVERS='["memory", "file"]'  uv run pytest tests/load

test-load-local-postgres:
	DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data ACTIVE_DESTINATIONS='["postgres"]' ALL_FILESYSTEM_DRIVERS='["memory"]'  uv run pytest tests/load

test-common:
	uv run pytest tests/common tests/normalize tests/extract tests/pipeline tests/reflection tests/sources tests/workspace tests/load/test_dummy_client.py tests/libs tests/destinations

build-library: dev
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

preprocess-docs:
	# run docs preprocessing to run a few checks and ensure examples can be parsed
	cd docs/website && npm i && npm run preprocess-docs

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
