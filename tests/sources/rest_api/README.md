# Pokemon API Test Environment

This directory contains a Docker Compose setup for running a PokeAPI instance for testing purposes.

## Local Usage

```bash
# From the project root
make start-pokemon-api

# Or manually:
cd tests/sources/rest_api
docker compose up -d --wait
docker compose exec -T app python manage.py migrate --settings=config.docker-compose
docker compose exec -T app sh -c 'echo "from data.v2.build import build_all; build_all()" | python manage.py shell --settings=config.docker-compose'
```

API will be available at: `http://localhost:8765`

