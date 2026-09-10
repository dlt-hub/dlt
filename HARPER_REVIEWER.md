# Harper Reviewer

## Scope
This document contains instructions for reading diagnostics returned by the `harper` grammar checking tool and
how to fix them.


Running Harper over a file is fast and inexpensive. You should use the command to get all diagnostics, make a few edits, and validate your changes. Avoid trying to fix all issues at once. It's most effective to look at them in batches and make sure that you're making progress fixing diagnostics.


## Run Harper

To run Harper to grammar check a specific file, you can run the following command from `docs/` directory

```shell
uv run prek run harper-lint --files FILE_PATH
```

Or from the root directory `/`

```shell
uv --directory docs run prek run harper-lint --files FILE_PATH
```

## Reviewing diagnostics

Running Harper will output diagnostics with locations. Typically, it will require making changes to the document. 

Tips:

- `Spelling::SpellCheck` will raise when a term is used inconsistently (e.g., AWS Lakeformation, Lake Formation, LakeFormation). Only add the canonical form to `docs/harper-dictionary.txt` and fix the other spellings.
- `Spelling::SpellCheck` will raise on variable names and references to code snippets in the body of the text (e.g., "set the lakeformation tags"). In this case, don't modify the dictionary entry and escape the variable in backticks (e.g., "set the `lakeformation` tags"). Those are often incorrectly escaped in bold markers `**`.
- "Setup guide" can be replaced to "Set up guide"
- Rephrase text to avoid possessive forms that make it hard to validate (e.g., "dlt's schema tracking feature" -> "the schema tracking feature in dlt")
- "Data source" is a valid form that Harper will flag as invalid "datasource". Change it to "source" alone if the meaning remains unambiguous.
- "Data type" is a valid form that Harper will flag as invalid "datatype".
