## Running Weaviate locally

Start Weaviate with Docker Compose:
```sh
docker compose -f tests/load/weaviate/docker-compose.yml up -d
```

Stop and clean up:
```sh
docker compose -f tests/load/weaviate/docker-compose.yml down -v --remove-orphans
```

This starts Weaviate with the contextionary vectorizer (no external APIs required). Add to `config.toml`:
```toml
[destination.weaviate]
connection_type = "local"
vectorizer = "text2vec-contextionary"
module_config = {text2vec-contextionary = {vectorizeClassName = false, vectorizePropertyName = true}}
```

For more details, see [Weaviate Local Quickstart](https://weaviate.io/developers/weaviate/quickstart/local).
