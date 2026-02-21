# DLT docs & tools

## Deployed docs website

Please go to ./website/README.md to learn more about how to contribute to the docs deployed at https://dlthub.com/docs/intro.

## Docs examples

Please go to ./examples/CONTRIBUTING.md to learn how to add the code examples found here: https://dlthub.com/docs/examples

## Education

Please check the ./education folder to find the dlt education notebooks, that are also found on the website.

## WASM notebooks

Go to ./notebooks to see the WASM notebooks deployed to our website, currently we only have the WASM playground you can find at https://dlthub.com/docs/tutorial/playground

## Setup

You will need [uv](https://docs.astral.sh/uv/) and Node.js on your system. Then from the `docs/` directory:

```sh
# Install Python dependencies (tools, linters, dlt itself)
make dev

# Install Node.js dependencies for the website
cd website && npm install
```

`make dev` must be run before any other command below.

## Makefile targets

The `Makefile` in this folder is the main entrypoint for docs tasks. Run `make help` (or just `make`) to see all available targets with descriptions.

## Tools

In the `docs_tools/` folder you can find various tools that can also be called directly via `uv run`.

### `uv run lint-embedded-snippets`
Finds all embedded snippets in our docs, extracts them and performs the following checks:

* Snippet must have a valid language set, e.g. ```py
* Snippet must be parseable (works for py, toml, yaml and json snippets)
* Snippet must pass linting (works for py)
* Snippet must pass type checking with mypy (works for py)

This script is run on CI to ensure code quality in our docs.

#### Usage

```sh
# Run a full check on all snippets
uv run lint-embedded-snippets full

# Show all available subcommands and arguments for this script
uv run lint-embedded-snippets --help

# Only run the linting stage
uv run lint-embedded-snippets lint

# Run all stages but only for snippets in files that have the string "walkthrough" in the filepath
# you will probably be using this a lot when working on one doc page
uv run lint-embedded-snippets full -f walkthrough

# Run the parsing stage, but only on snippets 49, 345 and 789
uv run lint-embedded-snippets parse -s 49,345,789

# run all checks but with a bit more output to the terminal
uv run lint-embedded-snippets full -v
```

#### Snippet numbers
Each snippet will be assigned an index in the order it is encountered. This is useful during creation of new snippets in the docs to selectively only run a few snippets. These numbers will change as snippets are inserted into the docs.

### `uv run fix-grammar`
Runs all (or selected) docs markdown files through the OpenAI API to correct grammar. You will need to place the OpenAI key in an `.env` file in this or the root folder. We pay for each OpenAI API call, so be a bit considerate of your usage :). It is good to check the grammar on new pages.

#### Usage

```sh
# Fix all pages
uv run fix-grammar

# Fix grammar for all files that have the string "walkthrough" in the filepath
uv run fix-grammar -f walkthrough

# Fix grammar for the particular file
uv run fix-grammar -f ../website/docs/intro.md
```

### `uv run prepare-examples-tests`

Prepares the examples you find at ./examples to be able to be tested with pytest. See the `test-examples` Make target in this folder.

### `uv run preprocess-docs`
Copies the markdown files for the website from ./docs to ./docs_processed and performs various tasks underway, such as embedding snippets, inserting destination info into the destination pages and various sanity checks. Is called by the npm script that previews and builds the website.
