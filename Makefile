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
	@echo "			tests all components unsing local destinations: duckdb and postgres"
	@echo "		test-common"
	@echo "			tests common components"
	@echo "		test-and-lint-snippets"
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
	poetry install --all-extras --with airflow --with docs --with providers --with pipeline --with sentry-sdk

lint:
	./check-package.sh
	poetry run black ./ --diff --exclude=".*syntax_error.py|\.venv.*"
	# poetry run isort ./ --diff
	poetry run mypy --config-file mypy.ini dlt tests
	poetry run flake8 --max-line-length=200 dlt
	poetry run flake8 --max-line-length=200 tests --exclude tests/reflection/module_cases
	# $(MAKE) lint-security

format:
	poetry run black ./ --exclude=".*syntax_error.py|\.venv.*"
	# poetry run isort ./

test-and-lint-snippets:
	poetry run mypy --config-file mypy.ini docs/website docs/examples
	poetry run flake8 --max-line-length=200 docs/website docs/examples
	cd docs/website/docs && poetry run pytest --ignore=node_modules

lint-security:
	poetry run bandit -r dlt/ -n 3 -l

test:
	(set -a && . tests/.env && poetry run pytest tests)

test-load-local:
	DESTINATION__POSTGRES__CREDENTIALS=postgresql://loader:loader@localhost:5432/dlt_data DESTINATION__DUCKDB__CREDENTIALS=duckdb:///_storage/test_quack.duckdb  poetry run pytest tests -k '(postgres or duckdb)'

test-common:
	poetry run pytest tests/common tests/normalize tests/extract tests/pipeline tests/reflection tests/sources tests/cli/common

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
	poetry export -f requirements.txt --output _gen_requirements.txt --without-hashes --extras gcp --extras redshift
	grep `cat compiled_packages.txt` _gen_requirements.txt > compiled_requirements.txt
	docker build -f deploy/dlt/Dockerfile.airflow --build-arg=COMMIT_SHA="$(shell git log -1 --pretty=%h)" --build-arg=IMAGE_VERSION="$(shell poetry version -s)" .
	docker build -f deploy/dlt/Dockerfile --build-arg=COMMIT_SHA="$(shell git log -1 --pretty=%h)" --build-arg=IMAGE_VERSION="$(shell poetry version -s)" .

