---
title: Configure credentials and pipelines
description: How to configure dlt pipelines and set up credentials
keywords: [credentials, secrets.toml, secrets, config, configuration, environment variables]
---
import DocCardList from '@theme/DocCardList';

The configuration mechanism in `dlt` provides a flexible, secure way to define credentials to external systems and other settings separately from your code.

## Key features

1. **Separation of secrets and configs from code** - The main role of the configuration system is to keep sensitive information out of your source code.

2. **Built-in credentials** - `dlt` provides built-in support for most common systems with default/machine credential access.

3. **Auto-generated configurations** - For functions decorated with `@dlt.source`, `@dlt.resource`, and `@dlt.destination`, `dlt` automatically generates appropriate configuration specs so they behave like built-in configs and credentials.

4. **Comprehensive configurability** - Nearly all aspects of `dlt` are configurable, including pipelines, normalizers, loaders, and logging, allowing you to change behavior without modifying code. This capability enables performance optimization and other adjustments at runtime.

<DocCardList />
