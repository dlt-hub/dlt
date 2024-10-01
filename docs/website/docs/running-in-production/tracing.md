---
title: Tracing
description: Rich information on executed dlt pipelines
keywords: [tracing, sentry, opt in]
---

# Tracing

`dlt` users can configure [Sentry](https://sentry.io) DSN to start receiving rich information on
executed pipelines, including encountered errors and exceptions. **Sentry tracing is disabled by
default.**

### When and what we send

An exception trace is sent when:

- Any Python logger (including `dlt`) logs an error.
- Any Python logger (including `dlt`) logs a warning (enabled only if the `dlt` logging level is
  `WARNING` or below).
- On unhandled exceptions.

A transaction trace is sent when the `pipeline.run` is called. We send information when
[extract, normalize, and load](../reference/explainers/how-dlt-works.md) steps are completed.

The data available in Sentry makes finding and documenting bugs easy, allowing you to easily find
bottlenecks and profile data extraction, normalization, and loading.

`dlt` adds a set of additional tags (e.g., pipeline name, destination name) to the Sentry data.

Please refer to the Sentry [documentation](https://docs.sentry.io/platforms/python/data-collected/).

### Enable pipeline tracing

To enable Sentry, you should configure the
[DSN](https://docs.sentry.io/product/sentry-basics/dsn-explainer/) in the `config.toml`:

```toml
[runtime]

sentry_dsn="https:///<...>"
```

Alternatively, you can use environment variables:

```sh
RUNTIME__SENTRY_DSN="https:///<...>"
```

The Sentry client is configured after the first pipeline is created with `dlt.pipeline()`. Feel free
to use `sentry_sdk` init again to cover your specific needs.

> 💡 `dlt` does not have Sentry client as a dependency. Remember to install it with `pip install sentry-sdk`.

## Disable all tracing

`dlt` allows you to completely disable pipeline tracing, including the anonymous telemetry and
Sentry. Using `config.toml`:

```toml
enable_runtime_trace=false
```

