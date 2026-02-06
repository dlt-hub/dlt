---
name: Destination Implementation Check
description: Verifies that new or modified destination implementations follow dlt's established patterns — factory functions, configuration specs, capabilities declarations, proper base class usage, and SQL client conventions.
---

# Destination Implementation Check

## Context

dlt has 24+ destination implementations under `dlt/destinations/impl/`. Each follows a consistent pattern with factory classes, configuration specs, capability declarations, and client implementations. Reviewers frequently provide feedback about following existing patterns rather than inventing new approaches (e.g., "I still do not understand why you'll not derive from `DuckdbSqlClient` and put all this code into `execute_query`?").

## Required Structure for a Destination

Each destination at `dlt/destinations/impl/<name>/` should have:

```
dlt/destinations/impl/<name>/
├── __init__.py          # Exports factory
├── factory.py           # Destination factory class
├── configuration.py     # @configspec configuration class
├── <name>_client.py     # JobClientBase implementation
├── sql_client.py        # SqlClientBase implementation (for SQL destinations)
└── exceptions.py        # Destination-specific exceptions (optional)
```

## What to Check

### 1. Factory Class

- Extends `Destination[TDestinationConfig, TDestinationClient]` (generic)
- Implements `spec` property returning the configuration class
- Implements `_raw_capabilities()` returning `DestinationCapabilitiesContext`
- Capabilities should be set correctly (transaction support, supported formats, etc.)

### 2. Configuration Spec

- Uses `@configspec` decorator
- Extends appropriate base (e.g., `DestinationClientConfiguration`, `DestinationClientDwhConfiguration`)
- Credentials field uses proper type from `dlt/common/configuration/specs/`
- Uses `inspect.getmodule()` or similar standard library functions rather than custom module detection

### 3. Client Implementation

- Extends `JobClientBase` or `SqlJobClientBase`
- For SQL destinations: derives from existing SQL client when possible (e.g., `DuckdbSqlClient`)
- Load jobs extend `RunnableLoadJob` or `LoadJob`
- Proper use of `TerminalException` vs `TransientException` for error handling

### 4. Module Hierarchy

- Avoid circular imports — use protocols from `dlt/common/` instead of importing concrete classes
- Keep destination-specific types out of `dlt/common/` — use protocols or put them in `dlt/destinations/`
- Inner imports (imports inside functions) should be minimized

### 5. Schema Integration

- Only tables found in the associated schema should be mapped
- Column hints (partition, sort, cluster) should follow the column-order convention used by other destinations
- "Unbound" columns for hints should be created so they are detected during normalize

### 6. Registration

- Factory is importable from `dlt/destinations/__init__.py`
- Added to `__all__` in destinations init
- Relevant extras added to `pyproject.toml` if new dependencies are needed
