---
sidebar_position: 2
---

# Deploy a pipeline

Before you can deploy a pipeline, you will need to 1) [install dlt](../installation.mdx),
2) [create a pipeline](./create-a-pipeline.md), and 3) sign up for a [GitHub](https://github.com) account, 
since you will be deploying using [GitHub Actions](https://github.com/features/actions).

## Add your `dlt` project directory to GitHub

If you have not already, you need to initialize a Git repo in your `dlt` project directory and push it to
GitHub as described in [Adding locally hosted code to GitHub](https://docs.github.com/en/get-started/importing-your-projects-to-github/importing-source-code-to-github/adding-locally-hosted-code-to-github).

## Ensure your pipeline works

Before you can deploy, you need a working pipeline. Make sure that it is working by running
```
python3 chess.py # replace chess.py with your pipeline file
```

This should successfully load data from the source to the destination once.

## Initialize deployment

In the same `dlt` project as your working pipeline, you can create a deployment using 
[GitHub Actions](https://github.com/features/actions) that will load data with the
`chess.py` script every 30 minutes by running
```
dlt deploy chess.py github --schedule "*/30 * * * *" # replace chess.py
```

This command checks that your pipeline has run successfully before and creates a GitHub Actions workflow 
file `run_chess_workflow.yml` in `.github/workflows` with the necessary environment variables.

## Add the secret values to GitHub

Copy and paste each `Name` and `Secret` pair printed out by the `dlt deploy` command line tool to the
GitHub UI located at the `github.com/.../settings/secrets/actions` link, which was also printed out by the `dlt deploy`
command line tool.

## Add, commit, and push your files

To finish the GitHub Actions workflow setup, you need to first add and commit your files
```
git add . && git commit -m 'pipeline deployed with github action'
```

And then push them to GitHub
```
git push origin
```

## Monitor (and manually trigger) the pipeline

The pipeline is now running every 30 minutes as you have scheduled. The `dlt deploy` command line
tool printed out a `github.com/.../actions/workflows/run_chess_workflow.yml` link where you can monitor
(and manually trigger) the GitHub Actions workflow that runs your pipeline in your repository.