---
title: Weaviate v4 Client Migration Guide
description: Changes and new features when upgrading from Weaviate Python client v3 to v4
keywords: [weaviate, vector database, destination, dlt, migration, v4]
---

# Weaviate v4 Client Migration Guide

This document describes the changes introduced when upgrading the dlt Weaviate destination from the `weaviate-client` v3.x to v4.x. The v4 client is a complete rewrite with significant API changes and new features.

## Breaking Changes

### 1. New gRPC Port Requirement (50051)

**What changed:** The Weaviate Python client v4 uses gRPC for improved performance on batch operations and queries. This requires port **50051** to be accessible.

**Why it matters:** If you're running Weaviate locally with Docker, you must expose the gRPC port in addition to the HTTP port:

```yaml
# docker-compose.yml
services:
  weaviate:
    image: semitechnologies/weaviate:1.27.0  # v4 requires 1.27.0+
    ports:
      - 8080:8080    # HTTP API
      - 50051:50051  # gRPC API (NEW - required for v4 client)
```

**User action required:** Update your Docker Compose or Kubernetes configuration to expose port 50051.

### 2. Minimum Weaviate Server Version

**What changed:** The v4 client requires Weaviate server version **1.27.0 or higher**.

**Why it matters:** Older Weaviate instances (e.g., 1.21.x) will fail to connect with an error like:
```
WeaviateStartUpError: Weaviate version 1.21.1 is not supported. Please use Weaviate version 1.27.0 or higher.
```

**User action required:** Upgrade your Weaviate server to 1.27.0+.

### 3. Terminology Change: Classes → Collections

**What changed:** Weaviate v4 renamed "classes" to "collections" throughout the API.

**Impact on dlt:** The internal implementation now uses collection-based APIs, but **backwards compatibility is maintained** - you can still use the same dlt pipeline code without changes.

| v3 Term | v4 Term |
|---------|---------|
| Class | Collection |
| Schema | Collection Config |
| Properties | Properties (unchanged) |

## New Imports and Their Purpose

The v4 client introduces a more structured configuration system:

```python
from weaviate.classes.config import (
    Configure,      # Factory for vectorizer/index configurations
    Property,       # Defines collection properties with type info
    DataType,       # Enum for Weaviate data types (TEXT, INT, etc.)
    Tokenization,   # Enum for text tokenization methods
)
from weaviate.classes.query import (
    Filter,         # Build query filters
    Sort,           # Build sort configurations
)
```

### Configure
Used to create vectorizer configurations. Example:
```python
Configure.Vectorizer.text2vec_openai()
Configure.Vectorizer.text2vec_contextionary(vectorize_collection_name=False)
Configure.Vectorizer.none()  # No vectorization
```

### Property
Defines property schemas with all options in one place:
```python
Property(
    name="title",
    data_type=DataType.TEXT,
    skip_vectorization=False,      # NEW: per-property vectorization control
    vectorize_property_name=False, # NEW: whether to vectorize the property name
    tokenization=Tokenization.WORD,
)
```

### DataType and Tokenization
Type-safe enums replacing string-based configuration:
```python
# v3 (string-based)
{"dataType": ["text"], "tokenization": "word"}

# v4 (enum-based)
Property(data_type=DataType.TEXT, tokenization=Tokenization.WORD)
```

## Tokenization Support

**What's new:** The v4 implementation properly exposes tokenization configuration through the Property class.

**Available tokenization methods:**
- `Tokenization.WORD` - Split on whitespace and punctuation (default)
- `Tokenization.LOWERCASE` - Like WORD, but lowercased
- `Tokenization.WHITESPACE` - Split only on whitespace
- `Tokenization.FIELD` - Index entire field as one token

**Usage with weaviate_adapter (unchanged):**
```python
weaviate_adapter(
    data,
    vectorize=["title"],
    tokenization={"title": "word", "description": "whitespace"},
)
```

## Batching Differences

### v3 Batch API
```python
with client.batch(
    batch_size=100,
    timeout_retries=5,
    callback=check_results,
) as batch:
    batch.add_data_object(data, class_name, uuid=uuid)
```

### v4 Batch API
```python
collection = client.collections.get(collection_name)
with collection.batch.dynamic() as batch:
    batch.add_object(properties=data, uuid=uuid)

# Check for failures after batch completes
failed_objects = collection.batch.failed_objects
```

**Key differences:**
1. **Collection-scoped:** Batches are now tied to a specific collection
2. **Dynamic batching:** `batch.dynamic()` automatically optimizes batch sizes
3. **Failure handling:** Failed objects are collected in `batch.failed_objects` instead of a callback
4. **Simpler API:** Properties are passed directly, no need to specify class name per object

## New Configuration Options

### 1. Per-Property Vectorization Control (`skip_vectorization`)

**New in v4:** You can now control vectorization at the property level more explicitly.

```python
Property(
    name="metadata",
    data_type=DataType.TEXT,
    skip_vectorization=True,  # Don't include this property in vectors
)
```

This is already exposed through the `weaviate_adapter`:
```python
weaviate_adapter(data, vectorize=["title"])  # Only "title" is vectorized
```

### 2. Property Name Vectorization (`vectorize_property_name`)

**New in v4:** Control whether the property name itself contributes to the vector.

```python
Property(
    name="description",
    data_type=DataType.TEXT,
    vectorize_property_name=False,  # Don't vectorize "description" as a word
)
```

**Note:** dlt sets this to `False` by default to avoid issues with property names containing underscores or non-English words (especially important for contextionary vectorizer).

### 3. Collection Name Vectorization

Configure whether the collection name is vectorized:
```toml
[destination.weaviate]
vectorizer = "text2vec-contextionary"
module_config = {text2vec-contextionary = {vectorizeClassName = false}}
```

## Benefits of Upgrading to v4

### Performance Improvements
- **gRPC support:** Faster batch imports and queries through binary protocol
- **Dynamic batching:** Automatic optimization of batch sizes based on object size
- **Connection pooling:** Better resource management for concurrent operations

### Better Type Safety
- Enum-based configuration instead of string magic values
- Pydantic models for configuration validation
- IDE autocompletion for all configuration options

### Improved Error Handling
- More descriptive error messages
- Structured error objects for batch failures
- Better retry handling built into the client

### New Features Available
- Multi-vector support (multiple vectors per object)
- Named vectors
- Improved multi-tenancy support
- Better async support (`WeaviateAsyncClient`)

## Configuration Reference

### Full Configuration Example

```toml
# secrets.toml
[destination.weaviate.credentials]
url = "http://localhost:8080"
api_key = "your-api-key"  # Optional for local instances

[destination.weaviate.credentials.additional_headers]
X-OpenAI-Api-Key = "your-openai-api-key"

# config.toml
[destination.weaviate]
vectorizer = "text2vec-openai"
module_config = {text2vec-openai = {model = "ada", modelVersion = "002", type = "text"}}

# Batch settings
batch_size = 100
batch_workers = 1
batch_consistency = "ONE"
batch_retries = 5

# Timeouts
conn_timeout = 10.0
read_timeout = 180.0
startup_period = 5
```

### Contextionary (Offline) Configuration

For fully offline operation without external APIs:

```toml
[destination.weaviate]
vectorizer = "text2vec-contextionary"
module_config = {text2vec-contextionary = {vectorizeClassName = false}}
```

**Important:** When using contextionary:
- Collection names must be parseable English words (avoid random hashes in dataset names)
- Property names are automatically configured with `vectorize_property_name=False` to avoid parsing issues

### Environment Variables

All configuration can also be set via environment variables:

```bash
# Credentials
export DESTINATION__WEAVIATE__CREDENTIALS__URL="http://localhost:8080"
export DESTINATION__WEAVIATE__CREDENTIALS__API_KEY="your-key"

# Vectorizer
export DESTINATION__WEAVIATE__VECTORIZER="text2vec-contextionary"
export DESTINATION__WEAVIATE__MODULE_CONFIG='{"text2vec-contextionary": {"vectorizeClassName": false}}'
```

## Migration Checklist

- [ ] Upgrade Weaviate server to 1.27.0+
- [ ] Expose gRPC port 50051 in Docker/Kubernetes
- [ ] Update `dlt[weaviate]` to get the new client
- [ ] Test existing pipelines (should work without code changes)
- [ ] Consider using contextionary for offline/local development
- [ ] Review new configuration options for optimization opportunities

## Troubleshooting

### "Weaviate version X is not supported"
Upgrade your Weaviate server to 1.27.0 or higher.

### Connection timeouts on batch operations
Ensure port 50051 (gRPC) is accessible. The v4 client requires both HTTP (8080) and gRPC (50051) ports.

### "Could not find word 'X' in contextionary"
When using text2vec-contextionary:
1. Set `vectorizeClassName = false` in module_config
2. Avoid dataset names with random hashes
3. dlt automatically sets `vectorize_property_name=False` for properties

### Batch insert failures with OpenAI
Ensure your OpenAI API key is set in `additional_headers`:
```toml
[destination.weaviate.credentials.additional_headers]
X-OpenAI-Api-Key = "sk-..."
```

