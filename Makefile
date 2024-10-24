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
	poetry install --all-extras --with docs,providers,pipeline,sources,sentry-sdk,airflow

lint:
	./tools/check-package.sh
	poetry run python ./tools/check-lockfile.py
	poetry run mypy --config-file mypy.ini dlt tests
	poetry run flake8 --max-line-length=200 dlt
	poetry run flake8 --max-line-length=200 tests --exclude tests/reflection/module_cases
	poetry run black dlt docs tests --check --diff --color --extend-exclude=".*syntax_error.py"
	# poetry run isort ./ --diff
	# $(MAKE) lint-security

format:
	poetry run black dlt docs tests --exclude=".*syntax_error.py|\.venv.*|_storage/.*"
	# poetry run isort ./

lint-and-test-snippets:
	cd docs/tools && poetry run python check_embedded_snippets.py full
	poetry run mypy --config-file mypy.ini docs/website docs/examples docs/tools --exclude docs/tools/lint_setup --exclude docs/website/docs_processed
	poetry run flake8 --max-line-length=200 docs/website docs/examples docs/tools
	cd docs/website/docs && poetry run pytest --ignore=node_modules

lint-and-test-examples:
	cd docs/tools && poetry run python prepare_examples_tests.py
	poetry run flake8 --max-line-length=200 docs/examples
	poetry run mypy --config-file mypy.ini docs/examples
	cd docs/examples && poetry run pytest


test-examples:
	cd docs/examples && poetry run pytest

lint-security:
	poetry run bandit -r dlt/ -n 3 -l

test:
	(set -a && . tests/.env && poetry run pytest tests)

test-load-local:
	DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data DESTINATION__DUCKDB__CREDENTIALS=duckdb:///_storage/test_quack.duckdb  poetry run pytest tests -k '(postgres or duckdb)'

test-common:
	poetry run pytest tests/common tests/normalize tests/extract tests/pipeline tests/reflection tests/sources tests/cli/common tests/load/test_dummy_client.py tests/libs tests/destinations

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
	# TODO: enable when we can remove special duckdb setting for python 3.12
	# poetry export -f requirements.txt --output _gen_requirements.txt --without-hashes --extras gcp --extras redshift
	# grep `cat compiled_packages.txt` _gen_requirements.txt > compiled_requirements.txt
	docker build -f deploy/dlt/Dockerfile.airflow --build-arg=COMMIT_SHA="$(shell git log -1 --pretty=%h)" --build-arg=IMAGE_VERSION="$(shell poetry version -s)" .
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
