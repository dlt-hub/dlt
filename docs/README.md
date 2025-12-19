# DLT docs & tools

## Deployed docs website

Please go to ./website/README.md to learn more about how to contribute to the docs deployed at https://dlthub.com/docs/intro.

## Docs examples

Please go to ./excamples/CONTRIBUTING.md to learn how to add the code examples found here: https://dlthub.com/docs/examples

## Education

Please check the ./education folder to find the dlt education notebooks, that are also found at on the website at: https://dlthub.com/docs/tutorial/education

## WASM notebooks

Got to ./notebooks to see the WASM notebooks deployed to our website, currently we only have the WASM playground you can find at https://dlthub.com/docs/tutorial/playground

## Tools

in the docs_tools folder you can find various tools that can aid you when working with the docs. You will need to install the python uv packages manager (https://dlthub.com/docs/tutorial/playground) on your system to run these commands.

## `uv run check-embedded-snippets`
This script find's all embedded snippets in our docs, extracts them and performs the following checks:

* Snippet must have a valid language set, e.g. ```py
* Snippet must be parseable (works for py, toml, yaml and json snippets)
* Snippet must pass linting (works for py)
* Coming soon: snippet must pass type checking with mypy

This script is run on CI to ensure code quality in our docs.

### Usage

```sh
# Run a full check on all snippets
uv run check-embedded-snippets full

# Show all Available subcommands and arguments for this script
uv run check-embedded-snippets --help

# Only run the linting stage
uv run check-embedded-snippets lint

# Run all stages but only for snippets in files that have the string "walkthrough" in the filepath
# you will probably be using this a lot when working on one doc page
uv run check-embedded-snippets full -f walkthrough

# Run the parsing stage, but only on snippets 49, 345 and 789
uv run check-embedded-snippets parse -s 49,345,789

# run all checks but with a bit more output to the terminal
uv run check-embedded-snippets full -v
```

### Snippet numbers
Each snippet will be assigned an index in the order it is encountered. This is useful during creation of new snippets in the docs to selectively only run a few snippets. These numbers will change as snippets are inserted into the docs.

## `uv run fix-grammar`
This script will run all (or selected) docs markdown files through the open ai api to correct grammar. You will need to place the open ai key in an `.env` file in this or the root folder. See `.env.example`. We pay for each openai api call, so be a bit considerate of your usage :). It is good to check the grammar on new pages.

### Usage

```sh
# Fix all pages
uv run fix-grammar

# Fix grammar for all files that have the string "walkthrough" in the filepath
uv run fix-grammar -f walkthrough

# Fix grammar for the particular file
uv run fix-grammar -f ../website/docs/intro.md
```

## `uv run prepare-examples-tests`

Prepares the examples you find at ./examples to by able to be tested with pytest. See the `test-examples` Make command in this folder.

## `uv run preprocess-docs`
Copies the markdown files for the website from ./docs to ./docs_processed and performs various tasks underway, such as embedding snippets, inserting destination info into the destination pages and various sanity checks. Is called by the npm script that preview and build the website.
