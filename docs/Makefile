.DEFAULT_GOAL := help

# Add " ## description" after any target name to include it in `make help` output.
# Example:  my-target: ## Does something useful
help: ## Shows this help message
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-28s %s\n", $$1, $$2}'

dev: ## Set up development environment
	uv sync --all-extras --all-groups --all-packages --quiet --locked
	uv run prek install --hook-type pre-commit --hook-type pre-push
	cd website && npm install

start: ## Generate docs and start the docusaurus dev server
	$(MAKE) preprocess-docs
	cd website && npx docusaurus start

build: ## Full production build of the docs website into `website/build/docs`
	cd website && uv run npm run update-versions
	cd website && npm run compile-redirects
	$(MAKE) check-orphan-docs
	$(MAKE) preprocess-docs
	cd website && npx docusaurus build --out-dir build/docs
	cd website && npm run verify-redirects
	cd website && node scripts/verify-llms-txt.js

# NOTE the command `preprocess-docs` must exist and produce the
# documentation and API reference for `update_versions.js` to work
# TODO make this mechanism less brittle
#
preprocess-docs: ## Dynamically generate pages under `website/docs_processed`
	# Order matters: preprocess-markers wipes and recreates docs_processed, so
	# generators that write into it must run afterwards.
	uv run --script tools/preprocess_docs.py
	uv run --script tools/generate_api_reference.py

lint: lint-core lint-notebooks lint-website check-orphan-docs ## Runs all linters

lint-website: ## Lints the docusaurus website JS/TS sources with Biome
	cd website && npm run lint

check-orphan-docs: ## Checks that all docs pages are reachable from website sidebars or marked unlisted
	cd website && npm run check-orphan-docs

cli-generate: ## Regenerates CLI reference docs
	uv --quiet run --script tools/generate_cli_docs.py website/docs/reference/command-line-interface.md

cli-check: ## Checks CLI reference docs are up to date (CI)
	uv --quiet run --script tools/generate_cli_docs.py website/docs/reference/command-line-interface.md --compare

# TODO consolidate the flake8, black, mypy tooling to be the same as root `pyproject.toml`
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

test-tools: ## Tests docs tools
	uv run pytest tools/

build-molabs: ## Format the notebooks files first and build Molabs
	uv run black education/*/*.ipynb --ipynb --quiet
	uv run tools/preprocess_to_molab.py
	uv run black education/*/*.py --quiet
	uv run marimo check education/*/*.py --fix --quiet

validate-molabs: build-molabs ## Validate marimo notebooks are up-to-date
	git diff --exit-code -- education/

snippets-lint:
	uv run pytest test_snippets.py::test_lint_snippets

snippets-format:
	uv run pytest test_snippets.py::test_lint_snippets --update-examples

snippets-typecheck:
	uv run pytest test_snippets.py::test_typecheck_snippets

snippets-test:
	uv run pytest test_snippets.py::test_run_snippets
