---
paths:
  - "docs/**"
---

# Documentation guidelines

## Setup
- `cd docs && make dev` (Python tooling), then `cd docs/website && npm install` (Node/Docusaurus)
- `make dev` must run before any other docs command

## Linting
- **Always lint after editing docs**: from `docs/` run `make lint-embedded-snippets`
- Filter by path: `uv run lint-embedded-snippets full -f <path-fragment>`
- Filter by snippet number: `-s 49,345`; verbose: `-v`

## Inline code snippets
- Use fenced code blocks with a language tag -- the linter rejects blocks without one
- Allowed tags: `py`, `toml`, `json`, `yaml`, `sh`, `bat`, `sql`, `text`, `hcl`, `dbml`, `dot`, `mermaid`
- Python snippets must parse (`ast.parse`), pass `ruff check`, and (optionally) `mypy`
- TOML/JSON/YAML snippets must parse with their respective parsers

## External snippet references
- Keep reusable examples in `snippets/` subdirectories next to the markdown, named `*-snippets.py`
- Mark code regions in the Python file:
  ```python
  # @@@DLT_SNIPPET_START my_snippet
  ...code...
  # @@@DLT_SNIPPET_END my_snippet
  ```
- Reference in markdown with an HTML comment: `<!--@@@DLT_SNIPPET ./snippets/file-snippets.py::my_snippet-->`
- The preprocessor (`uv run preprocess-docs`) replaces these markers with the extracted code block

## Internal links
- Use relative paths **with** `.md` extension: `[schema contracts](schema-contracts.md)`
- Link to sections with `#` anchors (auto-generated from headings, lowercase hyphenated): `[merge strategies](merge-loading.md#merge-strategies)`
- Go up directories as needed: `[adjust a schema](../walkthroughs/adjust-a-schema.md)`

## Markdown frontmatter
- Every doc page needs YAML frontmatter with at least `title`, `description`, and `keywords`:
  ```yaml
  ---
  title: Schema
  description: Schema definition and evolution
  keywords: [schema, dlt schema, yaml]
  ---
  ```
