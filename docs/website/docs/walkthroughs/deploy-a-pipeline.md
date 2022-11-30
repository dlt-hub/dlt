---
sidebar_position: 2
---

# Deploy a pipeline

Before you can deploy a pipeline, you will need to 1) [install dlt](../installation.mdx),
2) [create a pipeline](./create-a-pipeline.md), and 3) sign up for a [GitHub](https://github.com) account, 
since we will deploying using [GitHub Actions](https://github.com/features/actions).

## Initialize deployment

In the same `dlt` project as your working pipeline, you can create a deployment using 
[GitHub Actions](https://github.com/features/actions) that will load your data every 30 minutes by running
```
dlt pipeline deploy <script>.py github --schedule "*/30 * * * *"
```




- Command creates `workflow` file
- Command sets environment variables in workflow file
- Command sets secrets in workflow file and provides information on how to set those secrets in github
- Command will create the requirements.txt if not present (pip freeze) but it will ask first
- Command will give the exact instructions what to do next

## Add the secret values to GitHub

## Add, commit, and push your files

## See it running

```
https://github.com/<user>/<repo>/actions/workflows/<filename>.yml
```