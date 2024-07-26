---
title: Configuration and Secrets
description: How to configure dlt pipelines and set up credentials
keywords: [credentials, secrets.toml, secrets, config, configuration, environment variables]
---
import DocCardList from '@theme/DocCardList';

`dlt` pipelines usually require configurations and credentials. These can be set up in [various ways](setup):

1. Environment variables
2. Configuration files (`secrets.toml` and `config.toml`)
3. Key managers and Vaults

`dlt` automatically extracts configuration settings and secrets based on flexible [naming conventions](setup/#naming-convention). It then [injects](custom_sources/#injection-mechanism) these values where needed in code.

# Learn Details About

<DocCardList />