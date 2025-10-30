

generate-api-ref:
	cd tools/api_docs && uv run pydoc-markdown
	uv run python -m tools.api_docs.clean_pydoc_sidebar