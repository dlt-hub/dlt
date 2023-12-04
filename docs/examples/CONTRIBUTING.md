# How to contribute your example

Note: All paths in this guide are relative to the `dlt` repository directory.

## Add snippet

- Go to `docs/website/docs/examples/`.
- Copy one of the examples, rename scripts.
- Modify the script in `<example-name>/code/<snippet-name>-snippets.py`:
    - The whole example code should be inside of `def <snippet-name>_snippet()` function.
    - Use tags `# @@@DLT_SNIPPET_START example` and `# @@@DLT_SNIPPET_END example` to indicate which part of the code will be auto-generated in the final script `docs/examples/<examlple-name>/<snippet-name>.py`.
    - Use additional tags as `# @@@DLT_SNIPPET_START smal_part_of_code` to indicate which part of the code will be auto-inserted into a text document `docs/website/docs/examples/<example-name>/index.md` in the form of a code snippet.
- Modify .`dlt/secrets.toml` and `configs.toml` if needed.
- Modify `<example-name>/index.md`:
    - In the section `<Header info=` add the tl;dr for your example, it should be short but informative.
    - Set `slug="<example-name>" run_file="<snippet-name>" />`.
    - List what users will learn from this example. Use bullet points and link corresponding documentation pages.
    - Use tags `<!--@@@DLT_SNIPPET_START ./code/<snippet-name>-snippets.py::smal_part_of_code-->` to insert example code snippets. Do not write them manually!

## Add tests

- Do not forget to add tests to `<example-name>/code/<snippet-name>-snippets.py`.
- They could be short asserts, code should work.
- Use `# @@@DLT_REMOVE` to remove test code from final code example.
- Test your snippets locally first with command:
    - `cd docs/website/docs/examples/<example-name>/code && pytest --ignore=node_modules -s -v`.
- Add `@skipifgithubfork` decorator to your main snippet function, look [example](https://github.com/dlt-hub/dlt/blob/master/docs/website/docs/examples/chess_production/code/chess-snippets.py#L1-L4).

## Run npm start

The command `npm start`  starts a local development server and opens up a browser window.

- To install npm read [README](../website/README.md).
- This command will generate a clean example script in `docs/examples/<examlple-name>` folder based on `docs/website/docs/examples/<example-name>/code/<snippet-name>-snippets.py`.
- Also, this command automatically inserts code snippets to `docs/website/docs/examples/<example-name>/index.md`.

## Add ENV variables

If you use any secrets for the code snippets, e.g. Zendesk requires credentials. You need to add them to GitHub Actions in ENV style:

- First, add the variables to `.github/workflows/test_doc_snippets.yml`:

    Example:

    ```yaml
    # zendesk vars for example
    SOURCES__ZENDESK__CREDENTIALS: ${{ secrets.ZENDESK__CREDENTIALS }}
    ```

- Ask dlt team to add them to the GitHub Secrets.

## Add dependencies

If your example requires any additional dependency, then you can add it

- To  `pyproject.toml` in the `[tool.poetry.group.docs.dependencies]` section.
- Do not forget to update your `poetry.lock` file with `poetry lock --no-update` command and commit.