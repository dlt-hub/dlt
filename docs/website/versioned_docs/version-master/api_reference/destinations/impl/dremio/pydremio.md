---
sidebar_label: pydremio
title: destinations.impl.dremio.pydremio
---

Copyright (C) 2017-2021 Dremio Corporation

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

The code in this module was original from https://github.com/dremio-hub/arrow-flight-client-examples/tree/main/python.
The code has been modified and extended to provide a PEP 249 compatible interface.

This implementation will eagerly gather the full result set after every query.
Eventually, this module should be replaced with ADBC Flight SQL client.
See: https://github.com/apache/arrow-adbc/issues/1559

## DremioClientAuthMiddlewareFactory Objects

```python
class DremioClientAuthMiddlewareFactory(flight.ClientMiddlewareFactory)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/dremio/pydremio.py#L172)

A factory that creates DremioClientAuthMiddleware(s).

## DremioClientAuthMiddleware Objects

```python
class DremioClientAuthMiddleware(flight.ClientMiddleware)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/dremio/pydremio.py#L186)

A ClientMiddleware that extracts the bearer token from
the authorization header returned by the Dremio
Flight Server Endpoint.

Parameters
----------
factory : ClientHeaderAuthMiddlewareFactory
    The factory to set call credentials if an
    authorization header with bearer token is
    returned by the Dremio server.

## CookieMiddlewareFactory Objects

```python
class CookieMiddlewareFactory(flight.ClientMiddlewareFactory)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/dremio/pydremio.py#L216)

A factory that creates CookieMiddleware(s).

## CookieMiddleware Objects

```python
class CookieMiddleware(flight.ClientMiddleware)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/dremio/pydremio.py#L227)

A ClientMiddleware that receives and retransmits cookies.
For simplicity, this does not auto-expire cookies.

Parameters
----------
factory : CookieMiddlewareFactory
    The factory containing the currently cached cookies.

