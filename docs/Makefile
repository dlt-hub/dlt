

dev: ## Prepares development environment for docs tooling
	uv sync 

lint: ## Lints the docs tooling, non-embedded snippets, examples and notebooks NOTE: embedded snippets and notebooks are linted separately
	uv run ruff check
	uv run mypy --config-file ../mypy.ini docs_tools website examples --exclude docs_tools/snippets/lint_setup --exclude website/docs_processed --exclude website/versioned_docs/
	uv run flake8 --max-line-length=200 docs_tools website examples --exclude website/.dlt-repo,website/node_modules --exclude docs_tools/snippets/lint_setup
	uv run black docs_tools website examples --check --diff --color

lint-embedded-snippets: ## Lints all embedded snippets in the docs
	uv run lint-embedded-snippets full

lint-notebooks: ## Lints the notebooks in the education folder
	uv run nbqa flake8 education --extend-ignore=D,F704 --max-line-length=200
	uv run nbqa mypy  education \
	--ignore-missing-imports \
	--no-strict-optional \
	--disable-error-code=no-redef \
	--disable-error-code=top-level-await

test-examples: ## Tests the examples in the examples folder
	uv run prepare-examples-tests
	cd examples && uv run pytest

test-snippets: ## Tests the snippets in the snippets folder
	cd website/docs && uv run pytest --ignore=node_modules

format: ## Formats the docs tooling, website, examples, and notebooks
	uv run black docs_tools website examples
	uv run black education/*/*.ipynb --ipynb 

generate-api-ref: ## Generates the API reference documentation from dlt codebase for website
	cd docs_tools/api_docs && uv run pydoc-markdown
	uv run clean-pydoc-sidebar

preprocess-docs:  ## Preprocesses the docs pages, copies docs to docs_processed folder and inserts snippets and tuba links
	uv run preprocess-docs
	$(MAKE) generate-api-ref

preprocess-docs-watch: ## Preprocesses the docs pages, copies docs to docs_processed folder and inserts snippets and tuba links and watches for changes
	uv run preprocess-docs --watch

test-preprocess-molabs: ## Tests functions used to build Molabs
	uv run pytest docs_tools/education/tests

build-molabs: ## Format the notebooks files first and build Molabs
	uv run black education/*/*.ipynb --ipynb
	uv run python docs_tools/education/preprocess_to_molab.py
	uv run black education/*/*.py

validate-molabs: build-molabs ## Validate marimo notebooks are up-to-date
	git diff --quiet --exit-code -- education/
