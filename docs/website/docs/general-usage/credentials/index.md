---
title: Configuration and Secrets
description: How to configure dlt pipelines and set up credentials
keywords: [credentials, secrets.toml, secrets, config, configuration, environment variables]
---
import DocCardList from '@theme/DocCardList';

`dlt` supports two main types of configurations: configs and secrets. Both configs and secrets can be set up in [various ways](how_to_set_up_credentials.md):

1. Environment variables
2. Configuration files (`secrets.toml` and `config.toml`)
3. Configuration or secrets provider

`dlt` automatically extracts configuration settings and secrets based on flexible [naming conventions](how_to_set_up_credentials/#naming-convention). It then [injects](using_config_in_code/#injection-mechanism) these values where needed in code.

# Learn Details About

<DocCardList />