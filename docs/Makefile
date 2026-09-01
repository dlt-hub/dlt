.DEFAULT_GOAL := help

# Add " ## description" after any target name to include it in `make help` output.
# Example:  my-target: ## Does something useful
help: ## Shows this help message
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-28s %s\n", $$1, $$2}'

dev: ## Prepares development environment for docs tooling
	uv sync --all-extras --all-groups --all-packages --quiet
	uv run prek install

lint: lint-core lint-notebooks lint-website check-orphan-docs ## Runs all linters

lint-website: ## Lints the docusaurus website JS/TS sources with Biome
	cd website && npm run lint

check-orphan-docs: ## Checks that all docs pages are reachable from website sidebars or marked unlisted
	cd website && npm run check-orphan-docs

cli-generate: ## Regenerates CLI reference docs
	uv --quiet run --script tools/generate_cli_docs.py website/docs/reference/command-line-interface.md

cli-check: ## Checks CLI reference docs are up to date (CI)
	uv --quiet run --script tools/generate_cli_docs.py website/docs/reference/command-line-interface.md --compare

# Generated example test files (docs/examples/**/test_*.py) are gitignored and
# created by prepare-examples-tests, so exclude them from mypy.
# generate_api_reference.py declares its deps inline (PEP 723) and runs in its
# own env, so they are not resolvable here
lint-core: ## Lints the docs tooling, non-embedded snippets, examples and notebooks
	uv run ruff check
	uv run mypy --config-file ../pyproject.toml tools website --exclude tools/generate_api_reference.py --exclude website/docs_processed --exclude website/versioned_docs/ --exclude 'examples/[^/]+/test_.*\.py'
	uv run flake8 --max-line-length=200 tools website/docs
	uv run black tools website --check --diff --color

lint-notebooks: ## Lints the notebooks in the education folder
	uv run nbqa flake8 education --extend-ignore=D,F704 --max-line-length=200
	uv run nbqa mypy education \
	--ignore-missing-imports \
	--no-strict-optional \
	--disable-error-code=no-redef \
	--disable-error-code=top-level-await

format: ## Formats the docs tooling, website, examples, and notebooks
	uv run black tools website
	uv run black education/*/*.ipynb --ipynb

generate-api-ref: ## Generates the API reference documentation from dlt codebase for website
	uv run --script tools/generate_api_reference.py

preprocess-docs:  ## Preprocesses the docs pages, copies docs to docs_processed folder and inserts snippets and tuba links
	uv run --script tools/preprocess_docs.py
	$(MAKE) generate-api-ref

test-tools: ## Tests docs tools
	uv run pytest tools/

build-molabs: ## Format the notebooks files first and build Molabs
	uv run black education/*/*.ipynb --ipynb --quiet
	uv run tools/preprocess_to_molab.py
	uv run black education/*/*.py --quiet
	uv run marimo check education/*/*.py --fix --quiet

validate-molabs: build-molabs ## Validate marimo notebooks are up-to-date
	git diff --exit-code -- education/
