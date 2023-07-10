---
title: Staging
description: Configure an s3 or gcs bucket for staging before copying into the destination
keywords: [staging, destination]
---

# Staging

dlt supports a staging location for some destinations. Currently it is possible to copy files from a s3 bucket into redshift and from a gcs bucket into bigquery. dlt will automatically select an appropriate loader file format for the staging files. For this to work you have to set the staging argument of the pipeline to filesystem and provide both the credentials for the staging and the destination. You may also define an alternative staging file format in the run command of the pipeline, DLT will check wether the format is compatible with the final destination. Please refer to the documentation of each destination to learn how to use staging in the respective environment.

# Why staging?

By staging the data, you can leverage parallel processing capabilities of many modern cloud-based storage solutions. This can greatly reduce the total time it takes to load your data compared to uploading via the SQL interface. If you wish you can also retain a history of all imported data files in your bucket for auditing and trouble shooting purposes.