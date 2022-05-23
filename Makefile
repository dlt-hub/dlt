install-poetry:
ifneq ($(VIRTUAL_ENV),)
	$(error you cannot be under virtual environment $(VIRTUAL_ENV))
endif
	curl -sSL https://install.python-poetry.org | python3

has-poetry:
	poetry --version

dev: has-poetry
	# will install itself as editable module
	poetry install
	poetry run pip install -e ../rasa_data_ingestion

lint:
	poetry run mypy --config-file mypy.ini dlt examples
	poetry run flake8 --max-line-length=200 dlt examples
	$(MAKE) lint-security

lint-security:
	poetry run bandit -r autopoiesis/ -n 3 -ll
