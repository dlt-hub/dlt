---
paths:
  - "docs/**"
---

# Documentation guidelines

## Setup
- On your first setup, go to directory `docs/` and run `make dev`. This will install the required Python and JS/TS dependencies to build the docs `cd docs && make dev`.
- This will also install pre-commit hooks using `prek`. This will automatically run linting, type checking, etc. on each `git commit` and `git push`

## Code snippets
- Inline code snippets will automatically be formatted, linted, and type-checked. To opt-out, add the directive to the codefence `notype` or `nolint`.
- The `execute` directive means the snippet will be ran during before each commit and on CI. The executed snippets should be lightweight.

For example

    > \```python notype execute
    >
    > import foo
    > 
    > def my_func() -> int: ...
    > 
    > \``` 
  
## Internal links
- Use relative paths **with** `.md` extension: `[schema contracts](schema-contracts.md)`
- Link to sections with `#` anchors (auto-generated from headings, lowercase hyphenated): `[merge strategies](merge-loading.md#merge-strategies)`
- Go up directories as needed: `[adjust a schema](../walkthroughs/adjust-a-schema.md)`

## Markdown formatting
- `mdsmith` lints `website/docs` on CI (`md-check` hook), configured in `docs/.mdsmith.yml`. After editing a page, run from `docs/`:
  ```sh
  make md-fix    # pads and aligns tables, fixes whitespace
  make md-check  # must report failures=0
  ```
- Tables: write them unaligned and let `make md-fix` pad the columns (MDS025). It cannot fix a cell over 35 words (MDS026): shorten the text or move the detail below the table.

## Markdown frontmatter
- Every doc page needs YAML frontmatter with at least `title`, `description`, and `keywords`:
  ```yaml
  ---
  title: Schema
  description: Schema definition and evolution
  keywords: [schema, dlt schema, yaml]
  ---
  ```
