## Deploy to GitHub Actions

So far we have set up a pipeline that loads all the data we need from the GitHub API. Now we want to run it on a schedule to keep the data up to date. dlt makes it easy to deploy your pipeline to several workflow management platforms. In this tutorial, we'll use [GitHub Actions](https://github.com/features/actions).

### Create a GitHub repository

First, we need to create a GitHub repository. Let's call it `dlt-github-actions-demo`. We'll use it to store our pipeline code and run it on a schedule.

1. Head to GitHub and [create a new repository](https://github.com/new).
2. While still in the folder containing the code from the previous steps, follow the instructions on how to create a new repository on the command line.

### Enable deploy in the CLI

```sh
dlt deploy dlt_github_pipeline.py github-action --schedule "*/30 * * * *"
```

You should see the following output:

```
Looking up the deployment template scripts in https://github.com/dlt-hub/dlt-deploy-template.git...

Your github-action deployment for pipeline github_with_source_secrets in script dlt_github_pipeline.py is ready!
* A github workflow file run_github_with_source_secrets_workflow.yml was created in .github/workflows.
* The schedule with which the pipeline is run is: Every 30 minutes. You can also run the pipeline manually.
* The dependencies that will be used to run the pipeline are stored in requirements_github_action.txt. If you change add more dependencies, remember to refresh your deployment by running the same 'deploy' command again.

You should now add the secrets to github repository secrets, commit and push the pipeline files to github.
1. Add the following secret values (typically stored in ./.dlt/secrets.toml):
ACCESS_TOKEN
in https://github.com/your_username/github-actions-test/settings/secrets/actions

Name:
ACCESS_TOKEN
Secret:
ghp_XXX...X

2. Add stage deployment files to commit. Use your Git UI or the following command
git add requirements_github_action.txt .github/workflows/run_github_with_source_secrets_workflow.yml

3. Commit the files above. Use your Git UI or the following command
git commit -m 'run github_with_source_secrets pipeline with github action'

4. Push changes to github. Use your Git UI or the following command
git push origin

5. Your pipeline should be running! You can monitor it here:
https://github.com/burnash/github-actions-test/actions/workflows/run_github_with_source_secrets_workflow.yml
```

Follow the instructions in the output to finish the deployment.

