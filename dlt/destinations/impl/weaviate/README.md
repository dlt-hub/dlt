## Running Weaviate locally

Start Weaviate with Docker Compose:
```sh
docker compose -f tests/load/weaviate/docker-compose.yml up -d
```

Stop and clean up:
```sh
docker compose -f tests/load/weaviate/docker-compose.yml down -v --remove-orphans
```

This starts Weaviate with no vectorizer module. The tests bring their own vectors, so no
external API and no inference container are needed. Server-side batching needs Weaviate 1.36
or newer. Add to `config.toml`:
```toml
[destination.weaviate]
connection_type = "local"
vectorizer = "none"
```

For more details, see [Weaviate Local Quickstart](https://weaviate.io/developers/weaviate/quickstart/local).
