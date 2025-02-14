## Substitution

You can reference environment variables in the `dlt.yml` file using the `${ENV_VARIABLE_NAME}` syntax. Additionally, dlt+ provides several [predefined project variables](../features/projects.md#substitution) that are automatically substituted during loading.

* `project_dir` - the root directory of the project, i.e., the directory where the `dlt.yml` file is located
* `tmp_dir` - the directory for storing temporary files, can be configured in the project section as seen above, by default, it will be set to `${project_dir}_storage`.
* `name` - the name of the project, can be configured in the project section as seen above
* `default_profile` - the name of the default profile, can be configured in the project section as seen above
* `current_profile` - the name of the current profile, this is set automatically when a profile is used

### Project context

The `dlt.yml` marks the root of a project. Projects can also be nested. If you run any dlt project CLI command, dlt will search for the project root in the filesystem tree starting from the current working directory and run all operations on the found project. So if your `dlt.yml` is in the `tutorial` folder, you can run `dlt pipeline my_pipeline run` from this folder or all subfolders, and it will run the pipeline on the `tutorial` project.

