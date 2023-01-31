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
	@echo "		test-common"
	@echo "			tests common components"
	@echo "		build-library"
	@echo "			makes dev and then builds python-dlt for distribution"
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
	poetry install -E postgres -E redshift -E bigquery -E dbt

lint:
	./check-package.sh
	poetry run mypy --config-file mypy.ini dlt docs/examples
	poetry run flake8 --max-line-length=200 dlt docs/examples
	poetry run flake8 --max-line-length=200 tests
	# $(MAKE) lint-security

lint-security:
	poetry run bandit -r dlt/ -n 3 -l

test:
	(set -a && . tests/.env && poetry run pytest tests)

test-common:
	poetry run pytest tests --ignore=tests/load --ignore=tests/helpers --ignore=tests/cli

reset-test-storage:
	-rm -r _storage
	mkdir _storage
	python3 tests/tools/create_storages.py

recreate-compiled-deps:
	poetry export -f requirements.txt --output _gen_requirements.txt --without-hashes --extras gcp --extras redshift
	grep `cat compiled_packages.txt` _gen_requirements.txt > compiled_requirements.txt

build-library: dev
	poetry version
	poetry build

publish-library: build-library
	poetry publish