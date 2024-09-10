# How to contribute your example

Note: All paths in this guide are relative to the `dlt` repository directory.

## Add snippet

- Go to `docs/examples/`.
- Copy the template in `./_template/..`.
- Make sure the folder and your examples script have the same name
- Update the doc string which will compromise the generated markdown file, check the other examples how it is done
- If your example requires any secrets, add the vars to the example.secrects.toml but do not enter the values.
- Add your example code, make sure you have a `if __name__ = "__main__"` clause in which you run the example script, this will be used for testing
- You should add one or two assertions after running your example

## Testing
- You can test your example simply by running your example script from your example folder. On CI a test will be automatically generated.

## Checking your generated markdown

The command `npm start`  starts a local development server and opens up a browser window.

- To install npm read [README](../website/README.md).
- You should your example be automatically added to the examples section in the local version of the docs. Check the rendered output and see wether it looks the way you intended.

## Add ENV variables

If you use any secrets for the code snippets, e.g. Zendesk requires credentials. Please talk to us. We will add them to our google secrets vault.

## Add dependencies

If your example requires any additional dependency, then you can add it

- To  `pyproject.toml` in the `[tool.poetry.group.docs.dependencies]` section.
- Do not forget to update your `poetry.lock` file with `poetry lock --no-update` command and commit.
