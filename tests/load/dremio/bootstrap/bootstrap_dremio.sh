#!/usr/bin/env bash

set -euxo pipefail

DREMIO_URL=http://dremio:9047

# Poll the Dremio "live" endpoint until ready
until (curl "${DREMIO_URL}/live") do echo '...waiting...' && sleep 1; done;

# Bootstrap the first user
curl "${DREMIO_URL}/apiv2/bootstrap/firstuser" \
  -X 'PUT' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: null' \
  -d '{"userName":"dremio","firstName":"Dremio","lastName":"Admin","email":"dremio.admin@foo.com","createdAt":1694089769453,"password":"dremio123","extra":null}' \
  --fail-with-body

# Get token for admin user to use with api
output=$(curl -X POST "${DREMIO_URL}/apiv2/login" \
  -H 'Accept: */*' \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/json' \
  -d '{"userName":"dremio","password":"dremio123"}' \
  --fail-with-body
)
dremio_token=$(echo "$output" | python -c "import sys, json; print(json.load(sys.stdin)['token'])")
echo "$dremio_token"

# Need to increase the "single_field_size_bytes" limit otherwise some tests fail.
curl "${DREMIO_URL}/api/v3/sql" \
  -X 'POST' \
  -H 'Accept: */*' \
  -H "Authorization: _dremio${dremio_token}" \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/json' \
  -d '{"sql": "alter system set limits.single_field_size_bytes = 200000;"}' \
  --fail-with-body

# Create a NAS source. This will contain final ICEBERG tables.
curl "${DREMIO_URL}/apiv2/source/nas/?nocache=1708370225409" \
  -X 'PUT' \
  -H 'Accept: */*' \
  -H "Authorization: _dremio${dremio_token}" \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/json' \
  -d @nas.json \
  --fail-with-body

# Create an S3 source using minio. This will be used for staging data.
curl "${DREMIO_URL}/apiv2/source/minio/?nocache=1708370225409" \
  -X 'PUT' \
  -H 'Accept: */*' \
  -H "Authorization: _dremio${dremio_token}" \
  -H 'Connection: keep-alive' \
  -H 'Content-Type: application/json' \
  -d @minio.json \
  --fail-with-body
