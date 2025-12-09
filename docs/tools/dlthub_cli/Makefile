.PHONY: update-cli-docs, check-cli-docs, dev
# this must be run from `dlthub_cli` to see workspace commands
# it will use dlthub and dlt-runtime versions from dlt/docs/pyproject.toml to generate docs

dev:
	uv sync

update-cli-docs: dev
	# generate as there's no license
	RUNTIME__LICENSE="" uv run dlt --debug render-docs ../../website/docs/hub/command-line-interface.md --commands license workspace profile runtime

check-cli-docs: dev
	# generate as there's no license
	RUNTIME__LICENSE="" uv run dlt --debug render-docs ../../website/docs/hub/command-line-interface.md --compare --commands license workspace profile runtime
