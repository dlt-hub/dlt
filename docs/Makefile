

dev:
	uv sync 

generate-api-ref: ## Generates the API reference documentation from dlt codebase for website
	cd tools/api_docs && uv run pydoc-markdown
	uv run python -m tools.api_docs.clean_pydoc_sidebar

lint-embedded-snippets: ## Lints all embedded snippets in the docs
	uv run lint-embedded-snippets full