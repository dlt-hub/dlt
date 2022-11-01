## Project structure for a create pipeline workflow

Look into [project_structure](project_structure). It is a clone of template repository that we should have in our github. The files in the repository are parametrized with parameters of `dlt init` command.

1. it contains `.dlt` folded with `config.toml` and `secrets.toml`.
2. we prefill those files with values corresponding to the destination
3. the requirements contain `python-dlt` and `requests` in `requirements.txt`
4. `.gitignore` for `secrets.toml` and `.env` (python virtual environment)
5. the pipeline script file `pipeline.py` containing template for a new source
6. `README.md` file with whatever content we need


## dlt init

The prerequisites to run the command is to
1. create virtual environment
2. install `python-dlt` without extras

> Question: any better ideas? I do not see anything simpler to go around.

Proposed interface for the command:
`dlt init <source> <destination>`
Where `destination` must be one of our supported destination names: `bigquery` or `redshift` and source is alphanumeric string.

Should be executed in an empty directory without `.git` or any other files. It will clone a template and create the project structure as above. The files in the project will be customized:

1. `secrets.toml` will be prefilled with required credentials and secret values
2. `config.toml` will contain `pipeline_name`
3. the `pipeline.py` (1) will import the right destination (2) the source name will be changed to `<source>_data` (3) the dataset name will be changed to `<source>` etc.
4. `requirements.txt` will contain a proper dlt extras and requests library

> Questions:
> 1. should we generate a working pipeline as a template (ie. with existing API) or a piece of code with instructions how to change it?
> 2. which features should we show in the template? parametrized source? providing api key and simple authentication? many resources? parametrized resources? configure export and import of schema yaml files? etc?
> 3. should we `pip install` the required extras ans requests when `dlt init` is run?
