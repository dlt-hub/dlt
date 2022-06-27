PYV=$(shell python3 -c "import sys;t='{v[0]}.{v[1]}'.format(v=list(sys.version_info[:2]));sys.stdout.write(t)")
.SILENT:has-poetry

# pipeline version info
AUTV=$(shell python3 -c "from dlt import __version__;print(__version__)")
AUTVMINMAJ=$(shell python3 -c "from dlt import __version__;print('.'.join(__version__.split('.')[:-1]))")

NAME   := scalevector/dlt
TAG    := $(shell git log -1 --pretty=%h)
IMG    := ${NAME}:${TAG}
LATEST := ${NAME}:latest${VERSION_SUFFIX}
VERSION := ${AUTV}${VERSION_SUFFIX}
VERSION_MM := ${AUTVMINMAJ}${VERSION_SUFFIX}


# dbt runner version info
DBT_AUTV=$(shell python3 -c "from dlt.dbt_runner._version import __version__;print(__version__)")
DBT_AUTVMINMAJ=$(shell python3 -c "from dlt.dbt_runner._version import __version__;print('.'.join(__version__.split('.')[:-1]))")

DBT_NAME   := scalevector/dlt-dbt-runner
DBT_IMG    := ${DBT_NAME}:${TAG}
DBT_LATEST := ${DBT_NAME}:latest${VERSION_SUFFIX}
DBT_VERSION := ${DBT_AUTV}${VERSION_SUFFIX}
DBT_VERSION_MM := ${DBT_AUTVMINMAJ}${VERSION_SUFFIX}

install-poetry:
ifneq ($(VIRTUAL_ENV),)
	$(error you cannot be under virtual environment $(VIRTUAL_ENV))
endif
	curl -sSL https://install.python-poetry.org | python3

has-poetry:
	poetry --version

dev: has-poetry
	# will install itself as editable module with all the extras
	poetry install -E "postgres redshift dbt gcp"

lint:
	./check-package.sh
	poetry run mypy --config-file mypy.ini dlt examples
	poetry run flake8 --max-line-length=200 examples dlt/pipeline dlt/common/schema dlt/common/normalizers
	# $(MAKE) lint-security

lint-security:
	poetry run bandit -r dlt/ -n 3 -l

reset-test-storage:
	-rm -r _storage
	mkdir _storage
	python3 test/tools/create_storages.py

recreate-compiled-deps:
	poetry export -f requirements.txt --output _gen_requirements.txt --without-hashes --extras gcp --extras redshift
	grep `cat compiled_packages.txt` _gen_requirements.txt > compiled_requirements.txt

.PHONY: build-library
build-library:
	poetry version ${VERSION}
	poetry build

publish-library: build-library
	poetry publish -u __token__

build-image-tags:
	@echo ${IMG}
	@echo ${LATEST}
	@echo ${NAME}:${VERSION_MM}
	@echo ${NAME}:${VERSION}

build-image-no-version-tags:
	# poetry export -f requirements.txt --output _gen_requirements.txt --without-hashes --extras gcp --extras redshift
	docker build -f deploy/dlt/Dockerfile --build-arg=COMMIT_SHA=${TAG} --build-arg=IMAGE_VERSION="${VERSION}" . -t ${IMG}

build-image: build-image-no-version-tags
	docker tag ${IMG} ${LATEST}
	docker tag ${IMG} ${NAME}:${VERSION_MM}
	docker tag ${IMG} ${NAME}:${VERSION}

push-image:
	docker push ${IMG}
	docker push ${LATEST}
	docker push ${NAME}:${VERSION_MM}
	docker push ${NAME}:${VERSION}

dbt-build-image-tags:
	@echo ${DBT_IMG}
	@echo ${DBT_LATEST}
	@echo ${DBT_VERSION_MM}
	@echo ${DBT_VERSION}

dbt-build-image: build-library
	# poetry export -f requirements.txt --output _gen_requirements_dbt.txt --without-hashes --extras dbt
	docker build -f deploy/dbt_runner/Dockerfile --build-arg=COMMIT_SHA=${TAG} --build-arg=IMAGE_VERSION="${DBT_VERSION}" --build-arg=DLT_VERSION="${VERSION}" . -t ${DBT_IMG}
	docker tag ${DBT_IMG} ${DBT_LATEST}
	docker tag ${DBT_IMG} ${DBT_NAME}:${DBT_VERSION_MM}
	docker tag ${DBT_IMG} ${DBT_NAME}:${DBT_VERSION}

dbt-push-image:
	docker push ${DBT_IMG}
	docker push ${DBT_LATEST}
	docker push ${DBT_NAME}:${DBT_VERSION_MM}
	docker push ${DBT_NAME}:${DBT_VERSION}

docker-login:
	docker login -u scalevector -p ${DOCKER_PASS}
