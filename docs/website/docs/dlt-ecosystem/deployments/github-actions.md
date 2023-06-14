---
title: Github actions
description: How to run dlt in github actions
keywords: [dlt, webhook, serverless]
---

# Native deployment to github actions
What is a native deployment to dlt? a native deployment is a deployment where dlt will generate the glue code and credentials instructions.

For this easy style of deployment, dlt supports the cli command `dlt deploy scriptname.py orchestrator` which generates the necessary code and instructions.

Read more about the [deployment command](/docs/reference/command-line-interface#github-action) here.

## Considerations
* Github actions scheduling is not accurate - jobs will start within 0-30min of the scheduled time.
* Github actions has a free tier and a pricing model that work best for small jobs
* To stop the jobs, disable the scheduled workflows or remove the workflow file.

## How does the cli deployment work?
In the case of github actions, running the deploy command will do the following
1. Instructions will get printed to your CLI. You will need to follow them
2. A git workflow file is created. This file is equivalent to an airflow dag - it contains instructions of when and how to run your pipeline
3. follow the instructions to deploy the workflow file and add credentials to git for execution.
   1. follow the link and paste your credentials in.
   2. git add, commit, push your workflow file
   3. Run it manually to ensure everything runs fine

Here's what the instructions look like - by following them step by step you will have a running deployment:

```
Your github-action deployment for pipeline jira in script jira_pipeline.py is ready!
* A github workflow file run_jira_workflow.yml was created in .github/workflows.
* The schedule with which the pipeline is run is: Every 30 minutes. You can also run the pipeline manually.
* The dependencies that will be used to run the pipeline are stored in requirements_github_action.txt. If you change add more dependencies, remember to refresh your deployment by running the same 'deploy' command again.

You should now add the secrets to github repository secrets, commit and push the pipeline files to github.
1. Add the following secret values (typically stored in .dlt/secrets.toml):
SOURCES__JIRA__SUBDOMAIN
SOURCES__JIRA__EMAIL
SOURCES__JIRA__API_TOKEN
DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY
in https://github.com/dlt-hub/python-dlt-verified-pipelines/settings/secrets/actions

Name:
SOURCES__JIRA__SUBDOMAIN
Secret:
dltdev

Name:
SOURCES__JIRA__EMAIL
Secret:
adrian@dlthub.com

Name:
SOURCES__JIRA__API_TOKEN
Secret:
*****

Name:
DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY
Secret:
-----BEGIN PRIVATE KEY-----
*****
-----END PRIVATE KEY-----


2. Add stage deployment files to commit. Use your Git UI or the following command
git add ../requirements_github_action.txt ../.github/workflows/run_jira_workflow.yml

3. Commit the files above. Use your Git UI or the following command
git commit -m 'run jira pipeline with github action'
WARNING: You have modified files in your repository. Do not forget to push changes to your pipeline script as well!

4. Push changes to github. Use your Git UI or the following command
git push origin

5. Your pipeline should be running! You can monitor it here:
https://github.com/dlt-hub/python-dlt-verified-pipelines/actions/workflows/run_jira_workflow.yml
MacBook-Pro:pipelines adrian$
```


