1. `dlt init` to initialize new project and create project template for `create pipeline` use case. Should it also install `extras`?
2. `dlt deploy` to create deployment package (probably cron)

I have two existing working commands
1. `dlt schema` to load and parse schema file and convert it into `json` or `yaml`
2. `dlt pipeline` to inspect a pipeline with a specified name/working folder

We may also add:
1. `dlt run` or `dlt schedule` to run a pipeline in a script like cron would.
