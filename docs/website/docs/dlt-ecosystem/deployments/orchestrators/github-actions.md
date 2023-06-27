---
title: Github actions
description: How to run dlt in github actions
keywords: [dlt, webhook, serverless, deploy, github actions]
---

# Native deployment to GitHub Actions

What is a native deployment to dlt? A native deployment is a deployment where dlt will generate the
glue code and credentials instructions.

[GitHub Actions](https://docs.github.com/en/actions) is an automation tool by GitHub for building,
testing, and deploying software projects. It simplifies development with pre-built actions and
custom workflows, enabling tasks like testing, documentation generation, and event-based triggers.
It streamlines workflows and saves time for developers.

When `dlt` and GitHub Actions join forces, data loading and software development become a breeze.
Say goodbye to manual work and enjoy seamless data management.

For this easy style of deployment, dlt supports the
[cli command](../../../reference/command-line-interface#github-action):

```shell
dlt deploy <script>.py github-action --schedule <cron schedule>
```

which generates the necessary code and instructions.

> 💡 Read our
> [Walkthroughs: Deploy a pipeline with GitHub Actions](../../../walkthroughs/deploy-a-pipeline/deploy-with-github-actions)
> to find out more.
