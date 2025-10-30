# DLT docs tools

This is a collection of useful tools to manage our docs. Some of these require additional dependencies not added
to our pyproject.toml in the root dir. To install these with pip, run:

```sh
pip3 install -r requirements.txt
```

from this folder.

## `check_embedded_snippets.py`
This script find's all embedded snippets in our docs, extracts them and performs the following checks:

* Snippet must have a valid language set, e.g. ```py
* Snippet must be parseable (works for py, toml, yaml and json snippets)
* Snippet must pass linting (works for py)
* Coming soon: snippet must pass type checking with mypy

This script is run on CI to ensure code quality in our docs.

### Usage

```sh
# Run a full check on all snippets
python check_embedded_snippets.py full

# Show all Available subcommands and arguments for this script
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

## `fix_grammar_gpt.py`
This script will run all (or selected) docs markdown files through the open ai api to correct grammar. You will need to place the open ai key in an `.env` file in this or the root folder. See `.env.example`. We pay for each openai api call, so be a bit considerate of your usage :). It is good to check the grammar on new pages.

### Usage

```sh
# Fix all pages
python fix_grammar_gpt.py

# Fix grammar for all files that have the string "walkthrough" in the filepath
python fix_grammar_gpt.py -f walkthrough

# Fix grammar for the particular file
python fix_grammar_gpt.py -f ../website/docs/intro.md
```
