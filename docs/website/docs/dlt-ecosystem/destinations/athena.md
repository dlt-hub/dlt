---
title: AWS Athena / Glue Catalog
description: AWS Athena `dlt` destination
keywords: [aws, athena, glue catalog]
---

# AWS Athena

### Setup:
* Provide aws credentials for an identity that has the athena full access role
* Provide an s3 bucket which serves as the output for Athena queries, the aws identity will need access to this bucket too.
* Ensure that the aws identity also had read and write access for the buckets that will have the files for the athena tables


[destination.athena]
credentials.aws_access_key_id=""
credentials.aws_secret_access_key=""
credentials.aws_region="eu-central-1"
query_result_bucket="s3://athena-output-bucket"
