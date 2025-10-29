

generate-api-ref:
	cd tools/docs_tools/api_docs && uv run pydoc-markdown
	cd tools && uv run python -m docs_tools.api_docs.clean_pydoc_sidebar