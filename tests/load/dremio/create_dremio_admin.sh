#!/usr/bin/env bash

set -euxo pipefail

DREMIO_URL=http://localhost:9047

# Bootstrap the first user
curl "${DREMIO_URL}/apiv2/bootstrap/firstuser" \
  -X 'PUT' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: null' \
  -d '{"userName":"dremio","firstName":"Dremio","lastName":"Admin","email":"dremio.admin@foo.com","createdAt":1694089769453,"password":"dremio123","extra":null}'

# Get token for admin user to use with api
output=$(curl -X POST "${DREMIO_URL}/apiv2/login" \
  -H 'Accept: */*' \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/json' \
  -d '{"userName":"dremio","password":"dremio123"}'
)
dremio_token=$(echo "$output" | python -c "import sys, json; print(json.load(sys.stdin)['token'])")
echo "$dremio_token"

# Create Dremio Tatooine Space and folders
curl "${DREMIO_URL}/api/v3/catalog" \
  -H 'Accept: */*' \
  -H "Authorization: _dremio${dremio_token}" \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/json' \
  -d '{"name":"Tatooine","entityType":"space"}'

curl "${DREMIO_URL}/apiv2/space/Tatooine/folder/?nocache=1694170905671" \
  -H 'Accept: */*' \
  -H "Authorization: _dremio${dremio_token}" \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/json' \
  -d '{"name":"Mos Eisley"}'

curl "${DREMIO_URL}/apiv2/source/nas/?nocache=1708370225409" \
  -X 'PUT' \
  -H 'Accept: */*' \
  -H "Authorization: _dremio${dremio_token}" \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/json' \
  -d @nas.json