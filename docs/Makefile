.DEFAULT_GOAL := help

# Add " ## description" after any target name to include it in `make help` output.
# Example:  my-target: ## Does something useful
help: ## Shows this help message
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-28s %s\n", $$1, $$2}'

dev: ## Set up development environment
	uv sync --all-extras --all-groups --all-packages --quiet
	uv run prek install --hook-type pre-commit --hook-type pre-push

start: ## Generate docs and start the docusaurus dev server
	$(MAKE) preprocess-docs
	cd website && npx docusaurus start

# Pagefind indexes rendered HTML, so this needs a production build. The dev server
# (`make start`) serves this index as is via `website/plugins/pagefind-dev.js`:
# run it at least once to populate search, and again to refresh it.
search-index: ## Build the docs and the search index used by the dev server
	cd website && npx docusaurus build --out-dir build/docs
	cd website && npm run search-index

build: ## Full production build of the docs website into `website/build/docs`
	cd website && uv run npm run update-versions
	cd website && npm run compile-redirects
	$(MAKE) check-orphan-docs
	$(MAKE) preprocess-docs
	cd website && npx docusaurus build --out-dir build/docs
	cd website && npm run search-index
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
	$(MAKE) cli-generate-hub
	$(MAKE) api-generate-hub
	$(MAKE) sdk-generate-hub

lint: lint-core lint-notebooks lint-website check-orphan-docs ## Runs all linters

lint-website: ## Lints the docusaurus website JS/TS sources with Biome
	cd website && npm run lint

check-orphan-docs: ## Checks that all docs pages are reachable from website sidebars or marked unlisted
	cd website && npm run check-orphan-docs

cli-generate: ## Regenerates CLI reference docs
	uv --quiet run --script tools/generate_cli_docs.py website/docs/reference/command-line-interface.md

cli-check: ## Checks CLI reference docs are up to date (CI)
	uv --quiet run --script tools/generate_cli_docs.py website/docs/reference/command-line-interface.md --compare

# the CLI, SDK and REST API references describe the newest stable dlthub-client on PyPI from
# 0.28.8 on, the first stable release that bundles the API specs and SDK docs. Only a prerelease
# specifier enables alphas, so the stable requirement and dlthub stay on stable releases
HUB_CLIENT_STABLE_REQ := dlthub-client>=0.28.8
# TODO: remove the alpha fallback below once dlthub-client 0.28.8 is released and set
# `HUB_CLIENT_REQ ?= $(HUB_CLIENT_STABLE_REQ)`. It only lets the docs build from the 0.28.8
# alphas until then, and switches to the stable release on its own when it is out
HUB_CLIENT_ALPHA_REQ := dlthub-client>=0.28.8a3
ifndef HUB_CLIENT_REQ
HUB_CLIENT_REQ := $(shell echo "$(HUB_CLIENT_STABLE_REQ)" | uv pip compile --quiet --no-deps \
	--refresh-package dlthub-client - >/dev/null 2>&1 \
	&& echo "$(HUB_CLIENT_STABLE_REQ)" || echo "$(HUB_CLIENT_ALPHA_REQ)")
endif
# resolved once, the generate targets that `preprocess-docs` runs reuse it
export HUB_CLIENT_REQ
HUB_DLTHUB_REQ := dlthub>=0.30,<0.32
HUB_RUN = uv --quiet run --isolated --no-project --prerelease explicit \
	--refresh-package dlthub --refresh-package dlthub-client --with "$(HUB_CLIENT_REQ)"

# runs in tools/dlthub_cli because workspace commands need a workspace context
HUB_CLI_DOCS ?= website/docs_processed/hub/command-line-interface.md
cli-generate-hub: ## Generates the dlthub CLI reference into `website/docs_processed`
	cd tools/dlthub_cli && RUNTIME__LICENSE="" $(HUB_RUN) \
		--with "../../..[cli]" --with "$(HUB_DLTHUB_REQ)" \
		python ../generate_cli_docs.py ../../$(HUB_CLI_DOCS) \
		--executable-name dlthub --title "CLI reference" --sort-commands --show-versions dlthub dlthub-client

# TODO consolidate the flake8, black, mypy tooling to be the same as root `pyproject.toml`
lint-core: ## Lints the docs tooling, non-embedded snippets, examples and notebooks
	uv run ruff check
	uv run mypy --config-file ../pyproject.toml tools website --exclude tools/generate_api_reference.py --exclude tools/generate_sdk_docs.py --exclude website/docs_processed --exclude website/versioned_docs/ --exclude 'examples/[^/]+/test_.*\.py'
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

# set to a folder of specs, e.g. a runtime checkout's clients/cli/dlthub_sdk/_specs
RUNTIME_OPENAPI_DIR ?=
API_DOCS_DIR ?= website/docs_processed/hub/reference/api
api-generate-hub: ## Generates the runtime REST API reference into `website/docs_processed`
	$(HUB_RUN) --with pyyaml python tools/generate_openapi_docs.py --output-dir $(API_DOCS_DIR) \
		$(if $(RUNTIME_OPENAPI_DIR),--spec-dir $(RUNTIME_OPENAPI_DIR))

# read with griffe, without importing the package
SDK_DOCS_DIR ?= website/docs_processed/hub/reference/python-sdk
sdk-generate-hub: ## Generates the Python SDK reference into `website/docs_processed`
	$(HUB_RUN) --with "griffe>=1.5" python tools/generate_sdk_docs.py $(SDK_DOCS_DIR)

# links into the generated hub references resolve against fresh, git-ignored copies
md-check:  ## Check markdown file formatting errors in website/docs
	$(MAKE) cli-generate-hub HUB_CLI_DOCS=website/docs/hub/command-line-interface.md
	$(MAKE) api-generate-hub API_DOCS_DIR=website/docs/hub/reference/api
	$(MAKE) sdk-generate-hub SDK_DOCS_DIR=website/docs/hub/reference/python-sdk
	uv run mdsmith check website/docs

md-fix:  ## Fix markdown file formatting errors in website/docs
	uv run mdsmith fix website/docs
