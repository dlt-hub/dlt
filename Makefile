.PHONY: install-poetry build-library-prerelease has-poetry dev lint test test-common reset-test-storage recreate-compiled-deps build-library-prerelease publish-library

PYV=$(shell python3 -c "import sys;t='{v[0]}.{v[1]}'.format(v=list(sys.version_info[:2]));sys.stdout.write(t)")
.SILENT:has-poetry

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
	@echo "		install-poetry"
	@echo "			installs newest poetry version"
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

install-poetry:
ifneq ($(VIRTUAL_ENV),)
	$(error you cannot be under virtual environment $(VIRTUAL_ENV))
endif
	curl -sSL https://install.python-poetry.org | python3 -

has-poetry:
	poetry --version

dev: has-poetry
	poetry install --all-extras --with docs,providers,pipeline,sources,sentry-sdk,ibis,adbc


dev-airflow: has-poetry
	poetry install --all-extras --with docs,providers,pipeline,sources,sentry-sdk,ibis,airflow
	
lint:
	poetry run python ./tools/check-lockfile.py
	poetry run mypy --config-file mypy.ini dlt tests
	# NOTE: we exclude all D lint errors (docstrings)
	poetry run flake8 --extend-ignore=D --max-line-length=200 dlt
	poetry run flake8 --extend-ignore=D --max-line-length=200 tests --exclude tests/reflection/module_cases,tests/common/reflection/cases/modules/
	poetry run black dlt docs tests --check --diff --color --extend-exclude=".*syntax_error.py"
	# poetry run isort ./ --diff
	$(MAKE) lint-security
	$(MAKE) lint-docstrings

format:
	poetry run black dlt docs tests --extend-exclude='.*syntax_error.py|_storage/.*'

lint-snippets:
	cd docs/tools && poetry run python check_embedded_snippets.py full

lint-and-test-snippets: lint-snippets
	poetry run mypy --config-file mypy.ini docs/website docs/tools --exclude docs/tools/lint_setup --exclude docs/website/docs_processed
	poetry run flake8 --max-line-length=200 docs/website docs/tools --exclude docs/website/.dlt-repo
	cd docs/website/docs && poetry run pytest --ignore=node_modules

lint-and-test-examples:
	cd docs/tools && poetry run python prepare_examples_tests.py
	poetry run flake8 --max-line-length=200 docs/examples
	poetry run mypy --config-file mypy.ini docs/examples
	cd docs/examples && poetry run pytest

test-examples:
	cd docs/examples && poetry run pytest

lint-security:
	# go for ll by cleaning up eval and SQL warnings.
	poetry run bandit -r dlt/ -n 3 -lll

# check docstrings for all important public classes and functions
lint-docstrings:
	poetry run flake8 --count \
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
	poetry run pytest tests

test-load-local:
	ACTIVE_DESTINATIONS='["duckdb", "filesystem"]' ALL_FILESYSTEM_DRIVERS='["memory", "file"]'  poetry run pytest tests/load

test-load-local-postgres:
	DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data ACTIVE_DESTINATIONS='["postgres"]' ALL_FILESYSTEM_DRIVERS='["memory"]'  poetry run pytest tests/load

test-common:
	poetry run pytest tests/common tests/normalize tests/extract tests/pipeline tests/reflection tests/sources tests/cli/common tests/load/test_dummy_client.py tests/libs tests/destinations tests/transformations

reset-test-storage:
	-rm -r _storage
	mkdir _storage
	python3 tests/tools/create_storages.py

build-library: dev
	poetry version
	poetry build

publish-library: build-library
	poetry publish

test-build-images: build-library
	# NOTE: poetry export does not work with our many different deps, we install a subset and freeze
	# poetry export -f requirements.txt --output _gen_requirements.txt --without-hashes --extras gcp --extras redshift
	poetry install --no-interaction -E gcp -E redshift -E duckdb
	poetry run pip freeze > _gen_requirements.txt
	# filter out libs that need native compilation
	grep `cat compiled_packages.txt` _gen_requirements.txt > compiled_requirements.txt
	docker build -f deploy/dlt/Dockerfile.airflow --build-arg=COMMIT_SHA="$(shell git log -1 --pretty=%h)" --build-arg=IMAGE_VERSION="$(shell poetry version -s)" .
	# enable when we upgrade arrow to 20.x
	# docker build -f deploy/dlt/Dockerfile --build-arg=COMMIT_SHA="$(shell git log -1 --pretty=%h)" --build-arg=IMAGE_VERSION="$(shell poetry version -s)" .

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
	poetry run dlt --debug render-docs docs/website/docs/reference/command-line-interface.md

check-cli-docs:
	poetry run dlt --debug render-docs docs/website/docs/reference/command-line-interface.md --compare

test-e2e-studio:
	poetry run pytest --browser chromium tests/e2e

test-e2e-studio-headed:
	poetry run pytest --headed --browser chromium tests/e2e

start-dlt-studio-e2e:
	poetry run marimo run --headless dlt/helpers/studio/app.py -- -- --pipelines_dir _storage/.dlt/pipelines --with_test_identifiers true