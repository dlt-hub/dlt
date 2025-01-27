---
sidebar_label: configuration
title: destinations.impl.sqlalchemy.configuration
---

## SqlalchemyClientConfiguration Objects

```python
@configspec
class SqlalchemyClientConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/sqlalchemy/configuration.py#L60)

### destination\_type

type: ignore

### credentials

SQLAlchemy connection string

### engine\_args

Additional arguments passed to `sqlalchemy.create_engine`

