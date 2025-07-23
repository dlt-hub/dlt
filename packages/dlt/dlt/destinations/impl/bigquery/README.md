# Loader account setup

1. Create a new services account, add private key to it and download the `services.json` file.
2. Make sure the newly created account has access to BigQuery API.
3. You must add the following roles to the account above: `BigQuery Data Editor`, `BigQuey Job User` and `BigQuery Read Session User` (storage API)
4. IAM to add roles is here https://console.cloud.google.com/iam-admin/iam?project=chat-analytics-rasa-ci