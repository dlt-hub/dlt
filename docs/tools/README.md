# DLT docs tools

## `check_embedded_snippets.py`
This script find's all embedded snippets in our docs, extracts them and performs the following check:

* Snippet must have a valid language set, e.g. ```py
* Snippet must be parseable (works for py, toml, yaml and json snippets)
* Snippet must pass linting (works for py)
* Coming soon: snippet must pass type checking

This script is run on CI to ensure code quality in our docs.

### Usage

```sh
# Run a full check on all snippets
python check_embedded_snippets.py full

# Show all available commands and arguments for this script
python check_embedded_snippets.py --help

# Only run the linting stage
python check_embedded_snippets.py lint

# Run all stages but only for snippets in files that have the string "walkthrough" in the filepath
# you will probably be using this a lot when working on one doc page
python check_embedded_snippets.py full -f walkthrough

# Run the parsing stage, but only on snippets 49, 345 and 789
python check_embedded_snippets.py parse -s 49,345,789

# run all checks but with a bit more output to the terminal
python check_embedded_snippets.py full -v
```

### Snippet numbers
Each snippet will be assigned an index in the order it is encountered. This is useful during creation of new snippets in the docs to selectively only run a few snippets. These numbers will change as snippets are inserted into the docs.
