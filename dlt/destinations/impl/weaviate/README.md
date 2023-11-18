## Testing locally

Use the docker compose found in `.github` folder.
```
docker-compose -f .github/weaviate-compose.yml up -d
```

to stop
```
docker-compose -f .github/weaviate-compose.yml down -v --remove-orphans
```

It will start weaviate with contextionary vectorizer. It does not require any secrets. Provide the following section in `config.toml`
```toml
[destination.weaviate]
vectorizer="text2vec-contextionary"
module_config={text2vec-contextionary = { vectorizeClassName = false, vectorizePropertyName = true}}
```