---
sidebar_position: 2
---

# Deploy a pipeline

## You need a GitHub account

## You need a working pipeline

## Initialize deployment

`dlt pipeline deploy <script> github --schedule "****" --on-push`

- Command creates `workflow` file
- Command sets environment variables in workflow file
- Command sets secrets in workflow file and provides information on how to set those secrets in github
- Command will create the requirements.txt if not present (pip freeze) but it will ask first
- Command will give the exact instructions what to do next

## Add the secret values to GitHub

## Add, commit, and push your files

## See it running

https://github.com/<user>/<repo>/actions/workflows/<filename>.yml

- This is where you can disable