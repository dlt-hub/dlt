# The `init` structure
This folder contains files that `dlt init` uses to generate pipeline templates. The template is generated when in `dlt init <source> <destination>` the `<source>` pipeline is not found in `sources` folder.

The files are used as follows:
1. `pipeline.py` will be used as a default pipeline script template.
2. if `--generic` options is passed the `pipeline_generic.py` template is used
3. `dlt init` modifies the script above by passing the `<destination>` to `dlt.pipeline()` calls.
4. It will rename all `dlt.source` and `dlt.resource` function definition and calls to include the `<source>` argument in their names.
5. it copies the `.gitignore`, the pipeline script created above and other files in `TEMPLATE_FILES` variable (see. `__init__.py`)
6. it will copy `secrets.toml` and `config.toml` if they do not exist.
