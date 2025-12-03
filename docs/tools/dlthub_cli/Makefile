.PHONY: install-dlthub, update-cli-docs, check-cli-docs
# this must be run from `dlthub_cli` to see workspace commands

install-dlthub:
	uv pip install dlthub

update-cli-docs: install-dlthub
	uv run dlt --debug render-docs ../../website/docs/hub/command-line-interface.md --commands license workspace profile

check-cli-docs: install-dlthub
	uv run dlt --debug render-docs ../../website/docs/hub/command-line-interface.md --compare --commands license workspace profile