

dev: ## Prepares development environment for docs tooling
	uv sync 

lint: ## Lints the docs tooling
	uv run ruff check
	uv run mypy --config-file ../mypy.ini docs_tools  --exclude docs_tools/snippets/lint_setup
	uv run flake8 --max-line-length=200 docs_tools

format: ## Formats the docs tooling
	uv run ruff format

generate-api-ref: ## Generates the API reference documentation from dlt codebase for website
	cd docs_tools/api_docs && uv run pydoc-markdown
	uv run clean-pydoc-sidebar

preprocess-docs:  ## Preprocesses the docs pages, copies docs to docs_processed folder and inserts snippets and tuba links
	uv run preprocess-docs
	$(MAKE) generate-api-ref

preprocess-docs-watch: ## Preprocesses the docs pages, copies docs to docs_processed folder and inserts snippets and tuba links and watches for changes
	uv run preprocess-docs --watch

lint-embedded-snippets: ## Lints all embedded snippets in the docs
	uv run lint-embedded-snippets full

lint-notebooks: ## Lints the notebooks in the education folder
	uv run nbqa flake8 education --extend-ignore=D,F704 --max-line-length=200
	uv run nbqa mypy education \
	--ignore-missing-imports \
	--disable-error-code=no-redef \
	--disable-error-code=top-level-await